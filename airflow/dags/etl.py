from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task

with DAG(
    dag_id='NYC_TLC_Trip_Record_Data',
    description='A complex DAG, with ',
    schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['NYC_data']) as dag:

    @task
    def download_data_from_website():
        import requests
        import os
        import time
        data_type = 'yellow_tripdata'
        years = ['2021', '2022', '2023']
        months = ["01","02","03","04","05","06","07","08","09","10","11","12"]

        dir_save = '/home/bao/Documents/Project_DE/Features_storage/airflow/nyc_data'
        os.makedirs(dir_save, exist_ok=True)

        delay = 5  # delay in seconds
        for year in years:
            for month in months:
                file_name = f'{data_type}_{year}-{month}.parquet'

                url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}'

                for tries in range(10):
                    try:
                        response = requests.get(url)
                        with open(f'{dir_save}/{file_name}', "wb") as file:
                            file.write(response.content)
                        break
                    except :
                        print(f"Connection failed. Can't download file {file_name}")
                        print(f"Waiting for {delay} seconds before retrying. Attempt: {tries+1}")
                        time.sleep(delay)  # Wait for some time before retrying

    @task
    def transfrom_data():
        import pandas as pd
        import os

        dir_save = '/home/bao/Documents/Project_DE/Features_storage/airflow/nyc_data'
        for file in os.listdir(dir_save):
            if file.endswith('.parquet'):
                df = pd.read_parquet(os.path.join(dir_save, file))
                df = pd.DataFrame(df)
                if 'store_and_fwd_flag' in df.columns:
                    df.drop(columns=['store_and_fwd_flag'], inplace=True)
                df.rename(columns={
                        'tpep_pickup_datetime': 'pickup_datetime',
                        'tpep_dropoff_datetime': 'dropoff_datetime'},
                    inplace=True)

                df.columns = map(str.lower, df.columns)
                df = df.dropna()
                df = df.reindex(sorted(df.columns), axis=1)

                ints = ['dolocationid', 'vendorid', 'payment_type', 'passenger_count', 'pulocationid', 'ratecodeid']
                for int_dtype in ints:
                    df[int_dtype] = df[int_dtype].astype('int32')

                df.to_parquet(os.path.join(dir_save, file))
    @task
    def merge_data():
        import pandas as pd
        import os
        dir_save = '/home/bao/Documents/Project_DE/Features_storage/airflow/nyc_data'
        streamming_path = os.path.join(dir_save, "nyc_data_trip_merged")
        os.makedirs(streamming_path, exist_ok=True)
        df_list = []
        for file in os.listdir(dir_save):
            df = pd.read_parquet(os.path.join(dir_save, file))
            df = df.dropna()
            if df.shape[0] < 1000:
                continue
            df = df.sample(n=1000)
            df["content"] = [file.split("_")[0]] * 1000
            df_list.append(df)
        df = pd.concat(df_list)
        df.to_parquet(os.path.join(streamming_path, "nyc_data_trip.parquet"))


    (
        download_data_from_website()
        >> transfrom_data()
        >> merge_data()
    )