# -*- coding: utf-8 -*-
"""
Created on Sat Jun 14 19:49:16 2025

@author: atharv
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # 'email': ['atharvyande33@gmail.com'],
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='Stock-Market',
    default_args=default_args,
    description='Ingests stock data via Kafka and transforms it using dbt',
    schedule_interval='@once',#'0 18 * * *',  # Run every day at 6 PM
    start_date=datetime(2025, 6, 14),
    catchup=False,
    tags=['stock', 'kafka', 'dbt'],
) as dag:

    # Step 2: Run dbt transformations
    check_dir = BashOperator(
        task_id='check_dir',
        bash_command='ls;pwd;cd /opt/airflow/;ls;pwd'
    )

    # Step 1: Run stock_price_consumer
    consume_stock_data = BashOperator(
        task_id='consume_stock_data',
        bash_command='python /opt/airflow/scripts/stock_price_consumer.py'
    )

    # Step 2: Run dbt transformations
    run_dbt = BashOperator(
        task_id='run_dbt_transformations',
        bash_command='cd /opt/airflow/dbt/stock_dbt_project && dbt run'
    )

    # Optional: Notify on success (disabled by default)
    # notify_success = BashOperator(
    #     task_id='notify_success',
    #     bash_command='echo "Stock pipeline DAG completed!"'
    # )

    # DAG flow
    check_dir >> consume_stock_data >> run_dbt  # >> notify_success
