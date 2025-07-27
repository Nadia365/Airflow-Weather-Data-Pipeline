from airflow import DAG 
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.operators.http import HttpOperator
import json
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import boto3
from botocore.client import Config
import os




default_args = { 'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
} 


def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit


def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_farenheit= kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (F)": temp_farenheit,
                        "Feels Like (F)": feels_like_farenheit,
                        "Minimun Temp (F)":min_temp_farenheit,
                        "Maximum Temp (F)": max_temp_farenheit,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    file_name = f'current_weather_data_tunis_{dt_string}.csv'
    # Save DataFrame to a local temporary file
    local_file = f"/tmp/{file_name}"
    df_data.to_csv(local_file, index=False)
 # Use temporary directory inside Airflow folder
    airflow_tmp_dir = os.path.join(os.path.expanduser("~/airflow"), "tmp")
    local_file = os.path.join(airflow_tmp_dir, file_name)

    
    # Ensure the temporary directory exists
    os.makedirs(airflow_tmp_dir, exist_ok=True)
        
    # Save DataFrame to temporary file
    df_data.to_csv(local_file, index=False)

    # Configure boto3 to use MinIO
    s3_client = boto3.client(
            's3',
            endpoint_url='http://192.168.1.78:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            config=Config(signature_version='s3v4')
        )

    # Upload file to MinIO bucket
    bucket_name = 'weather-data'
    s3_client.upload_file(local_file, bucket_name, file_name)
    print(f"Uploaded {file_name} to s3://{bucket_name}/{file_name}")


    

with DAG('weather_dag',
    default_args=default_args,
    description='A simple weather DAG',
    schedule='@daily',
    catchup=False
) as dag :
    
    is_weahter_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Tunis&appid=d8c9b8d4c8c7a6a4687566dd86eacae1',

    )

    extract_weather_data = HttpOperator(
        task_id ='extract_weather_data',
        http_conn_id ='weathermap_api',
        endpoint='/data/2.5/weather?q=Tunis&appid=d8c9b8d4c8c7a6a4687566dd86eacae1',
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),
        log_response=True
        )
    transform_load_weather_data = PythonOperator(
        task_id= 'transform_load_weather_data',
        python_callable=transform_load_data
        )

    is_weahter_api_ready >> extract_weather_data >> transform_load_weather_data
