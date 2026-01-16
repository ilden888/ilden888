from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
import json
import requests
from datetime import datetime
from airflow.models import Variable
import time
import logging


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 17),
}

dag = DAG(
    'chat2desk_tag_upload',
    default_args=default_args,
    description='Uploads daily tags and contacts',
    schedule_interval='40 9 * * *',
    tags=["chat2desk", "daily"],
)


def add_all_existing_clients_to_dwh():
    chat2desk_api_key = Variable.get("chat2desk_api_key")
    headers = {'Authorization': chat2desk_api_key}
    url = "https://api.chat2desk.com/v1/clients/"

    pg_hook = PostgresHook(postgres_conn_id='dwh')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT distinct phone FROM integrations.tag_queue WHERE chat2desk_id is null")
    rows = cursor.fetchall()

    for i, row in enumerate(rows):
        if i >= 200:
            logging.info("Processed 200 rows, stopping to limit runtime")
            break
        phone = row[0]
        clean_phone = phone.lstrip('+')
        response = requests.get(url, headers=headers, params={'phone': clean_phone})
        logging.info("Checked phone %s (%d/%d)", phone, i + 1, len(rows))
        time.sleep(0.5)

        if response.status_code == 200:
            json_data = response.json()
            if len(json_data['data']) > 0:
                chat2desk_id = json_data['data'][0]['id']
                cursor.execute(
                    """UPDATE integrations.tag_queue
                       SET chat2desk_id = %s WHERE phone = %s""",
                    (chat2desk_id, phone))
        else:
            raise Exception("API request failed with status code: {}".format(response.status_code))

    conn.commit()
    cursor.close()
    conn.close()


def send_contacts_to_crm():
    chat2desk_api_key = Variable.get("chat2desk_api_key")
    headers = {
        'Authorization': chat2desk_api_key,
        'Content-Type': 'application/json'
    }
    url = "https://api.chat2desk.com/v1/clients/"

    pg_hook = PostgresHook(postgres_conn_id='dwh')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT distinct phone FROM integrations.tag_queue WHERE chat2desk_id is null")
    rows = cursor.fetchall()
    for row in rows:
        phone = row[0]
        clean_phone = phone.lstrip('+')
        data = {
            'phone': clean_phone,
            'transport': 'wa_dialog'
        }
        response = requests.post(url, headers=headers, data=json.dumps(data))

        response_data = response.json()['data']
        update_query = """UPDATE integrations.tag_queue
                          SET chat2desk_id = %s 
                          WHERE phone = %s"""
        cursor.execute(update_query, (response_data['id'], row[0]))

    conn.commit()
    cursor.close()
    conn.close()


def get_all_tags_from_crm():
    offset = 0
    limit = 100
    chat2desk_api_key = Variable.get("chat2desk_api_key")
    headers = {'Authorization': chat2desk_api_key}

    pg_hook = PostgresHook(postgres_conn_id='dwh')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    truncate_query = "TRUNCATE TABLE integrations.chat2desk_tags"
    cursor.execute(truncate_query)

    while True:
        params = {
            'offset': offset,
            'limit': limit,
        }
        response = requests.get("https://api.chat2desk.com/v1/tags/", headers=headers, params=params)
        data = response.json()

        for item in data['data']:
            insert_query = """INSERT INTO integrations.chat2desk_tags 
                              (id, group_id, group_name, label, description) 
                              VALUES (%s, %s, %s, %s, %s)"""
            cursor.execute(insert_query, (item['id'], item['group_id'], item['group_name'], item['label'], item['description']))

        conn.commit()

        if len(data['data']) < limit:
            break
        offset += limit

    cursor.close()
    conn.close()


def create_new_tags_in_crm():
    chat2desk_api_key = Variable.get("chat2desk_api_key")
    pg_hook = PostgresHook(postgres_conn_id='dwh')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    url = "https://api.chat2desk.com/v1/tags"
    headers = {'Authorization': chat2desk_api_key}

    query = """ 
            SELECT DISTINCT tag, tag_group_id, tag_bg_color, tag_description
            FROM integrations.tag_queue 
            WHERE tag_id IS NULL
            """
    cursor.execute(query)
    rows = cursor.fetchall()

    for row in rows:
        body = {
            "tag_group_id": row[1],
            "tag_label": row[0],
            "tag_description": row[3],
            "tag_bg_color": row[2],
            "tag_text_color": "ffffff",
            "order_show": 1
        }
        response = requests.post(url, headers=headers, json=body)

        if response.status_code == 200:
            json_data = response.json()
            label_id = json_data['data']['id']
            label = json_data['data']['label']

            cursor.execute(
                "UPDATE integrations.tag_queue SET tag_id = %s WHERE tag = %s",
                (label_id, label))

    conn.commit()
    cursor.close()
    conn.close()


def assign_tags_to_clients():
    chat2desk_api_key = Variable.get("chat2desk_api_key")
    pg_hook = PostgresHook(postgres_conn_id='dwh')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    headers = {
        'Authorization': chat2desk_api_key,
        'Content-Type': 'application/json',
    }

    base_url = "https://api.chat2desk.com/v1/tags/assign_to"

    cursor.execute(
        """SELECT phone, chat2desk_id, tag_id
           FROM integrations.tag_queue
           WHERE chat2desk_id IS NOT NULL 
           AND uploaded_time IS NULL
        """)

    clients = cursor.fetchall()

    for row in clients:
        if row[1] and row[2]:
            phone = row[0]
            chat2desk_id = row[1]
            tag_id = row[2]

            response = requests.post(base_url, headers=headers, json={
                "tag_ids": [tag_id],
                "assignee_type": "client",
                "assignee_id": str(chat2desk_id)
            })

            if response.status_code == 200:
                timestamp = datetime.now()
                cursor.execute("UPDATE integrations.tag_queue SET uploaded_time = %s WHERE phone = %s AND tag_id = %s",
                               (timestamp, phone, tag_id))
                conn.commit()

    cursor.close()
    conn.close()

add_club_tags_to_queue = PostgresOperator(
    task_id='add_club_tags_to_queue',
    postgres_conn_id='dwh',
    sql="""
    INSERT INTO integrations.tag_queue (phone, tag, tag_group_id, tag_bg_color, tag_description)
    SELECT DISTINCT
        replace(g.phone, '+', ''),
        g.club_label, 
        73186 AS tag_group_id, 
        '5be6c1' AS tag_bg_color, 
        'Club' AS tag_description
    FROM dwh.yesterday_guests g
    LEFT JOIN integrations.tag_queue tq ON (replace(g.phone, '+', '')=tq.phone AND tag_group_id = 73186)
    WHERE tq.phone IS NULL
    """,
    dag=dag,
)

add_regtime_tags_to_queue = PostgresOperator(
    task_id='add_regtime_tags_to_queue',
    postgres_conn_id='dwh',
    sql="""
    INSERT INTO integrations.tag_queue (phone, tag, tag_group_id, tag_bg_color, tag_description)
    SELECT DISTINCT
        replace(g.phone, '+', ''),
        g.reg_time_label, 
        73223 AS tag_group_id, 
        'efb0f5' AS tag_bg_color, 
        'Registration date' AS tag_description
    FROM dwh.yesterday_guests g
    LEFT JOIN integrations.tag_queue tq ON (replace(g.phone, '+', '')=tq.phone AND tag_group_id = 73223)
    WHERE tq.phone IS NULL
    """,
    dag=dag,
)

add_newclient_tag_to_queue = PostgresOperator(
    task_id='add_newclient_tag_to_queue',
    postgres_conn_id='dwh',
    sql="""
    INSERT INTO integrations.tag_queue (phone, tag, tag_group_id, tag_bg_color, tag_description)
    SELECT DISTINCT
        replace(g.phone, '+', ''),
        'newclient' AS tag, 
        74304 AS tag_group_id, 
        'bfff8f' AS tag_bg_color, 
        'New client' AS tag_description
    FROM dwh.yesterday_guests g
    WHERE g.club_label IN ('DMBIG', 'DMSMALL', 'BW', 'VN', 'ICP', 'DH2')
    AND NOT EXISTS (
        SELECT 1 FROM integrations.tag_queue tq 
        WHERE tq.phone = replace(g.phone, '+', '') AND tq.tag_group_id = 74304
    );
    """,
    dag=dag,
)

add_newclientizi_tag_to_queue = PostgresOperator(
    task_id='add_newclientizi_tag_to_queue',
    postgres_conn_id='dwh',
    sql="""
    INSERT INTO integrations.tag_queue (phone, tag, tag_group_id, tag_bg_color, tag_description)
    SELECT DISTINCT
        replace(g.phone, '+', ''),
        'newclientizi' AS tag, 
        389837 AS tag_group_id, 
        'bddaa2' AS tag_bg_color, 
        'new users izi' AS tag_description
    FROM dwh.yesterday_guests g
    WHERE g.club_label NOT IN ('DMBIG', 'DMSMALL', 'BW', 'VN', 'ICP', 'DH2')
    AND NOT EXISTS (
        SELECT 1 FROM integrations.tag_queue tq 
        WHERE tq.phone = replace(g.phone, '+', '') AND tq.tag_group_id = 389837
    );
    """,
    dag=dag,
)

add_no1week_tag_to_queue = PostgresOperator(
    task_id='add_no1week_tag_to_queue',
    postgres_conn_id='dwh',
    sql="""
    INSERT INTO integrations.tag_queue (phone, tag, tag_group_id, tag_bg_color, tag_description)
    SELECT DISTINCT
        replace(g.phone, '+', ''),
        'no1week' AS tag, 
        75000 AS tag_group_id, 
        'd3d3d3' AS tag_bg_color, 
        'No visit in last 7 days' AS tag_description
    FROM dwh.no1week_guests g
    LEFT JOIN integrations.tag_queue tq ON (replace(g.phone, '+', '')=tq.phone AND tag_group_id = 75000)
    WHERE tq.phone IS NULL
    """,
    dag=dag,
)

add_no2week_tag_to_queue = PostgresOperator(
    task_id='add_no2week_tag_to_queue',
    postgres_conn_id='dwh',
    sql="""
    INSERT INTO integrations.tag_queue (phone, tag, tag_group_id, tag_bg_color, tag_description)
    SELECT DISTINCT
        replace(g.phone, '+', ''),
        'no2week' AS tag, 
        75001 AS tag_group_id, 
        'ffb6c1' AS tag_bg_color, 
        'No visit in last 14 days' AS tag_description
    FROM dwh.no2week_guests g
    LEFT JOIN integrations.tag_queue tq ON (replace(g.phone, '+', '')=tq.phone AND tag_group_id = 75001)
    WHERE tq.phone IS NULL
    """,
    dag=dag,
)

add_no1month_tag_to_queue = PostgresOperator(
    task_id='add_no1month_tag_to_queue',
    postgres_conn_id='dwh',
    sql="""
    INSERT INTO integrations.tag_queue (phone, tag, tag_group_id, tag_bg_color, tag_description)
    SELECT DISTINCT
        replace(g.phone, '+', ''),
        'no1month' AS tag, 
        75002 AS tag_group_id, 
        'ffa500' AS tag_bg_color, 
        'No visit in last 30 days' AS tag_description
    FROM dwh.no1month_guests g
    LEFT JOIN integrations.tag_queue tq ON (replace(g.phone, '+', '')=tq.phone AND tag_group_id = 75002)
    WHERE tq.phone IS NULL
    """,
    dag=dag,
)

add_meta_tag_to_queue = PostgresOperator(
    task_id='add_meta_tag_to_queue',
    postgres_conn_id='dwh',
    sql="""
    insert into integrations.tag_queue (phone, tag, tag_group_id, tag_bg_color, tag_description)
    select distinct
        replace(a.phone, '+', ''),
        'meta' as tag, 
        74304 as tag_group_id, 
        'bfff8f' as tag_bg_color, 
        'Meta lead' as tag_description
    FROM integrations.fb_ads a
    left join integrations.tag_queue tq on (replace(a.phone, '+', '')=tq.phone and tag_group_id = 74304)
    where tq.phone is null
    and (a.phone is not null and a.phone != '')
    """,
    dag=dag,
)

add_meta_adset_tag_to_queue = PostgresOperator(
    task_id='add_meta_adset_tag_to_queue',
    postgres_conn_id='dwh',
    sql="""
    insert into integrations.tag_queue (phone, tag, tag_group_id, tag_bg_color, tag_description)
    select distinct
        replace(a.phone, '+', ''),
        adset_name as tag, 
        74337 as tag_group_id, 
        'ffa3ac' as tag_bg_color, 
        'Meta adset name' as tag_description
    FROM integrations.fb_ads a
    left join integrations.tag_queue tq on (replace(a.phone, '+', '')=tq.phone and tq.tag= a.adset_name and tag_group_id = 74337)
    where tq.phone is null
    and (a.phone is not null and a.phone != '')
    and length (adset_name)<=20
    """,
    dag=dag,
)

add_1_day_to_subscription_end_tag_to_queue = PostgresOperator(
    task_id='add_1_day_to_subscription_end_tag_to_queue',
    postgres_conn_id='dwh',
    sql="""
    insert into integrations.tag_queue (phone, tag, tag_group_id, tag_bg_color, tag_description)
    select 
        replace(subs.phone, '+', ''),
        '1dr' || '_' || subs.club || coalesce ( '_' || short_tariff_name, '') as tag,
        74808 as tag_group_id,
        '88cff7' as tag_bg_color,
        'One day remains in the subscription' as tag_description
    from dwh.guests_with_single_active_sub subs
    left join integrations.tag_queue tq 
        on (replace(subs.phone, '+', '')=tq.phone
            and tag_group_id = 74808 
            and tq.tag = '1dr' || '_' || subs.club || '_' || coalesce (short_tariff_name, '')
            )  
    where true
        and current_date = day_before_expiration
        and tq.phone is null     
    """,
    dag=dag,
)

add_subscription_ended_tag_to_queue = PostgresOperator(
    task_id='add_subscription_ended_tag_to_queue',
    postgres_conn_id='dwh',
    sql="""
    insert into integrations.tag_queue (phone, tag, tag_group_id, tag_bg_color, tag_description)
    select 
        replace(subs.phone, '+', ''),
        'pke' || '_' || subs.club || coalesce ( '_' || short_tariff_name, '') as tag,
        74808 as tag_group_id,
        '88cff7' as tag_bg_color,
        'Subscription ends today' as tag_description
    from dwh.guests_with_single_active_sub subs
    left join integrations.tag_queue tq 
        on (replace(subs.phone, '+', '')=tq.phone 
            and tag_group_id = 74808 
            and tq.tag = 'pke' || '_' || subs.club || coalesce ( '_' || short_tariff_name, '')
            )
    where true
        and current_date = day_of_expiration
        and tq.phone is null    
    """,
    dag=dag,
)

add_tournament_users_to_queue = PostgresOperator(
    task_id='add_tournament_users_to_queue',
    postgres_conn_id='dwh',
    sql="""
    INSERT INTO integrations.tag_queue (phone, tag, tag_group_id, tag_bg_color, tag_description)
    SELECT DISTINCT
        replace(t.phone, '+', ''),
        'tourn' AS tag,
        385831 AS tag_group_id,
        '3e75a6' AS tag_bg_color,
        'Tournament participant' AS tag_description
    FROM dwh.new_guest_tourn t
    WHERE NOT EXISTS (
        SELECT 1 FROM integrations.tag_queue tq 
        WHERE tq.phone = replace(t.phone, '+', '') AND tq.tag = 'tourn'
    );
    """,
    dag=dag,
)

add_all_existing_clients_to_dwh_task = PythonOperator(
    task_id='add_all_existing_clients_to_dwh',
    python_callable=add_all_existing_clients_to_dwh,
    dag=dag,
)

send_contacts_to_crm_task = PythonOperator(
    task_id='send_contacts_to_crm',
    python_callable=send_contacts_to_crm,
    dag=dag,
)

get_all_tags_from_crm_task = PythonOperator(
    task_id='get_all_tags_from_crm',
    python_callable=get_all_tags_from_crm,
    dag=dag,
)

write_existing_tag_id_to_queue = PostgresOperator(
    task_id='write_existing_tag_id_to_queue',
    postgres_conn_id='dwh',
    sql="""
    WITH label_ids AS (
        SELECT label, MAX(id) AS consistent_id
        FROM integrations.chat2desk_tags
        WHERE group_id IN (73186, 73223, 74304, 74376, 74375, 74337, 74808, 75000)
        GROUP BY label
    )
    UPDATE integrations.tag_queue
    SET tag_id = label_ids.consistent_id
    FROM label_ids
    WHERE tag_queue.tag = label_ids.label
    AND tag_id IS NULL
    """,
    dag=dag,
)

create_new_tags_in_crm_task = PythonOperator(
    task_id='create_new_tags_in_crm',
    python_callable=create_new_tags_in_crm,
    dag=dag,
)

assign_tags_to_clients_task = PythonOperator(
    task_id='assign_tags_to_clients',
    python_callable=assign_tags_to_clients,
    dag=dag,
)

add_club_tags_to_queue >> add_no1week_tag_to_queue >> add_no2week_tag_to_queue >> add_no1month_tag_to_queue
add_no1week_tag_to_queue >> add_tournament_users_to_queue >> add_all_existing_clients_to_dwh_task >> send_contacts_to_crm_task >> get_all_tags_from_crm_task >> write_existing_tag_id_to_queue >> create_new_tags_in_crm_task >> assign_tags_to_clients_task

add_newclient_tag_to_queue >> add_all_existing_clients_to_dwh_task
add_newclientizi_tag_to_queue >> add_all_existing_clients_to_dwh_task
