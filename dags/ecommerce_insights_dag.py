from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from google.cloud import storage
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ecommerce_insights',
    default_args=default_args,
    description='Generate e-commerce insights daily at 10 am',
    schedule_interval='0 10 * * *',
    catchup=False,
)

def read_data_from_gcs(bucket_name, source_blob_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    data = blob.download_as_string()
    df = pd.read_csv(pd.compat.StringIO(data.decode('utf-8')))
    return df

def process_insights():
    df = read_data_from_gcs('gs://ecommerce-customer-bucket', 'e-commerce-customer-behavior.csv')
    
    # Perform customer segmentation
    df['SpendingCategory'] = df['Total Spend'].apply(lambda x: 'High' if x > 1000 else 'Medium' if x > 500 else 'Low')
    
    # Identify inactive and recent customers
    df['Days Since Last Purchase'] = df['Days Since Last Purchase'].astype(int)
    inactive_customers = df[df['Days Since Last Purchase'] > 30]
    recent_customers = df[df['Days Since Last Purchase'] <= 30]
    
    write_data_to_gcs(inactive_customers, 'gs://project_imad', 'insights/inactive_customers.csv')
    write_data_to_gcs(recent_customers, 'gs://project_imad', 'insights/recent_customers.csv')

def write_data_to_gcs(df, bucket_name, destination_blob_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')

process_task = PythonOperator(
    task_id='process_insights',
    python_callable=process_insights,
    dag=dag,
)

process_task
