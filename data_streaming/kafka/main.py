import os
import random
from datetime import datetime
from time import sleep

import pandas as pd

import pandas as pd
import psycopg2
from sqlalchemy import create_engine

class PostgresSQLClient:
    def __init__(self, database='nyc_db_cdc', user='postgres', password='191122', host="127.0.0.1", port="5432"):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def create_conn(self):
        # Establishing the connection
        conn = psycopg2.connect(
            database=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )
        # Creating a cursor object using the cursor() method
        return conn

    def execute_query(self, query):
        conn = self.create_conn()
        cursor = conn.cursor()
        cursor.execute(query)
        print(f"Query has been executed successfully!")
        conn.commit()
        # Closing the connection
        conn.close()

    def get_columns(self, table_name):
        conn = psycopg2.connect(f"dbname={self.database} user={self.user} password={self.password} port={self.port} host={self.host}")
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table_name} LIMIT 0")  # No need to fetch rows
        colnames = [desc[0] for desc in cur.description]
        conn.close()  # Close the connection
        return colnames

    def get__total_rows(self, table_name):
        conn = psycopg2.connect(f"dbname={self.database} user={self.user} password={self.password} port={self.port} host={self.host}")
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_rows = cur.fetchone()[0]  # Fetch the result of the query
        conn.close()  # Close the connection
        return total_rows
    
TABLE_NAME = "nyc_dwh_cdc"

def create_table(pc):
    create_table_query = """
        CREATE TABLE IF NOT EXISTS nyc_dwh_cdc (
            save_time TIMESTAMP WITHOUT TIME ZONE,
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
            airport_fee FLOAT NOT NULL,
            content VARCHAR(50) NOT NULL
        );
    """
    try:
        pc.execute_query(create_table_query)
        print(f"Create table successful!")
    except Exception as e:
        print(f"Failed to create table with error: {e}")



def insert_table(pc):
    kafka_df = pd.read_parquet("/home/bao/Documents/Project_DE/Features_storage/airflow/nyc_data/nyc_data_trip_merged/nyc_data_trip.parquet")
 
    # Get all columns from the devices table
    try:
        columns = pc.get_columns(table_name=TABLE_NAME)
        print(columns)
        print(len(columns))
    except Exception as e:
        print(f"Failed to get schema for table with error: {e}")

    num_rows = 5
    for _ in range(num_rows):
        # Randomize values for feature columns
        row = kafka_df.sample()
        ##convert to list
        feature_values = row.values.tolist()[0]

        feature_values[1] = str(feature_values[1])
        feature_values[2] = str(feature_values[2])
        save_time = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        data = [save_time] + feature_values

        query = f"""
            insert into {TABLE_NAME} ({",".join(columns)})
            values {tuple(data)}
        """

        pc.execute_query(query)
        sleep(1)


if __name__ == "__main__":
    pc = PostgresSQLClient()
    create_table(pc)

    insert_table(pc)