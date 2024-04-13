from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator


def insert_data_to_table():
    import os
    import pandas as pd
    pg_hook = PostgresHook.get_hook(conn_id='postgres_default')
    for file in os.listdir('/home/bao/airflow/dags/nyc_data'):
        if file.endswith(".parquet"):
            df = pd.read_parquet(os.path.join('/home/bao/airflow/dags/nyc_data', file))
            print("Inserting data from file: " + file)
            pg_hook.insert_rows(table="nyc_dwh", rows=df.values.tolist())

with DAG(
    dag_id='Data_Warehouse_with_NYC_TLC_Trip_Record_Data',
    description='A complex DAG, with ',
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['NYC_data_warehouse']) as dag:

    create_NYC_dwh = PostgresOperator(
        task_id="create_NYC_dwh",
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS nyc_dwh (
            id BIGSERIAL NOT NULL PRIMARY KEY,
            vendorid INT NOT NULL,
            pickup_datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            dropoff_datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            passenger_count INT NOT NULL,
            trip_distance FLOAT NOT NULL,
            ratecodeid INT NOT NULL,
            pulocationid INT NOT NULL,
            dolocationid INT NOT NULL,
            payment_type INT NOT NULL,
            fare_amount FLOAT NOT NULL,
            extra FLOAT NOT NULL,
            mta_tax FLOAT NOT NULL,
            tip_amount FLOAT NOT NULL,
            tolls_amount FLOAT NOT NULL,
            improvement_surcharge FLOAT NOT NULL,
            total_amount FLOAT NOT NULL,
            congestion_surcharge FLOAT NOT NULL,
            airport_fee FLOAT NOT NULL
        );
        """,
    )
    insert_data = PythonOperator(
        task_id="insert_data",
        python_callable=insert_data_to_table,
    )

    create_NYC_dwh >> insert_data
