import datetime
import time
import psycopg2

import requests
import json
import pandas as pd
import numpy as np

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models.xcom import XCom

###POSTGRESQL settings###
#set postgresql connectionfrom basehook
psql_conn = BaseHook.get_connection('postgresql_de')

##init test connection
conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
cur = conn.cursor()
cur.close()
conn.close()

#4. апдейт витринок (таблички f)
def customer_retention_view(ti):
    #connection to database
    psql_conn = BaseHook.get_connection('postgresql_de')
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    #f_activity
    cur.execute("""INSERT INTO mart.f_customer_retention (new_customers_count)
with new_customers_count AS (
    select count(customer_id) new_customers_count
    from mart.f_sales
    join mart.d_calendar on f_sales.date_id = d_calendar.date_id
    where week_of_year = DATE_PART('week', '{{ds}}'::DATE)
    GROUP BY customer_id
    HAVING count(customer_id) = 1
    )
SELECT count(new_customers_count)
FROM new_customers_count;


INSERT INTO mart.f_customer_retention (returning_customers_count) 
with returning_customers_count AS (
    select count(customer_id) returning_customers_count
    from mart.f_sales
    join mart.d_calendar on f_sales.date_id = d_calendar.date_id
    where week_of_year = DATE_PART('week', '{{ds}}'::DATE)
    GROUP BY customer_id
    HAVING count(customer_id) > 1
    )
SELECT count(returning_customers_count)
FROM returning_customers_count;


INSERT INTO mart.f_customer_retention (refunded_customer_count) 
with refunded_customer_count AS (
    select count(customer_id) refunded_customer_count
    from mart.f_sales
    join mart.d_calendar on f_sales.date_id = d_calendar.date_id
    where week_of_year = DATE_PART('week', '{{ds}}'::DATE)
      AND payment_amount < 0
    GROUP BY customer_id
    )
SELECT count(refunded_customer_count)
FROM refunded_customer_count;


INSERT INTO mart.f_customer_retention (period_name) VALUES ('weekly');


INSERT INTO mart.f_customer_retention (period_id) 
    select week_of_year AS period_id
    from mart.d_calendar
    where week_of_year = DATE_PART('week', '{{ds}}'::DATE)


INSERT INTO mart.f_customer_retention (new_customers_revenue) 
with new_customers_revenue AS (
    select sum(payment_amount) new_customers_revenue
    from mart.f_sales
    join mart.d_calendar on f_sales.date_id = d_calendar.date_id
    where week_of_year = DATE_PART('week', '{{ds}}'::DATE)
    GROUP BY customer_id
    HAVING count(customer_id) = 1
    )
SELECT sum(new_customers_revenue)
FROM new_customers_revenue;


INSERT INTO mart.f_customer_retention (returning_customers_revenue) 
with returning_customers_revenue AS (
    select sum(payment_amount) returning_customers_revenue
    from mart.f_sales
    join mart.d_calendar on f_sales.date_id = d_calendar.date_id
    where week_of_year = DATE_PART('week', '{{ds}}'::DATE)
    GROUP BY customer_id
    HAVING count(customer_id) > 1
    )
SELECT sum(returning_customers_revenue)
FROM returning_customers_revenue;


INSERT INTO mart.f_customer_retention (customers_refunded) 
    select count(payment_amount) customers_refunded
    from mart.f_sales
    join mart.d_calendar on f_sales.date_id = d_calendar.date_id
    where week_of_year = DATE_PART('week', '{{ds}}'::DATE)
    AND payment_amount < 0;
""")
    conn.commit()

    cur.close()
    conn.close()

    return 200

#Объявляем даг
dag = DAG(
    dag_id='create_customer_retention_view',
    schedule_interval = "0 11 * * MON",
    start_date=datetime.datetime.today() - datetime.timedelta(days=1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60)
)

t_customer_retention_view = PythonOperator(task_id='customer_retention_view',
                                        python_callable=customer_retention_view,
                                        dag=dag)

t_customer_retention_view


        