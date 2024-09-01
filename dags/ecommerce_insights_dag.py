from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from pyspark.sql import SparkSession, DataFrame, functions as sf

GCS_BUCKET = "ecommerce-customer"

def read_data(spark: SparkSession, file_path: str) -> DataFrame:
    """reads csv file from specified file path."""
    return spark.read.csv(file_path, header=True, inferSchema=True)

def calculate_highest_spend(df: DataFrame) -> DataFrame:
    membership_by_gender = (df.groupBy("Gender", "Membership Type")
                          .agg(sf.round(sf.mean("age"), 1).alias("Average_age"),
                                sf.round(sf.mean("Items Purchased"), 0).alias("Average_items_purchased"),
                               sf.round(sf.mean("Total Spend"), 2).alias("Average_spend"))
                        .sort("Average_spend", ascending=False))
    return membership_by_gender

def write_to_gcs(df: DataFrame, filepath: str):
    return (df
            .toPandas()
            .to_csv(filepath, index = False))

def etl_with_spark():
    spark = SparkSession.builder \
    .appName('data-engineering-capstone') \
    .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
    .config("spark.sql.repl.eagerEval.enabled", True) \
    .getOrCreate()

    file_path = f"gs://{GCS_BUCKET}/e-commerce-customer-behavior.csv"
    # read data from gcs
    customer_df = read_data(spark=spark, file_path=file_path)

    # perform some transformations
    insights_df = calculate_highest_spend(df=customer_df)

    # Write insights to GCS
    datetime_now = datetime.now().strftime("%m%d%Y%H%M%S")

    write_path = f"gs://{GCS_BUCKET}/insights/{datetime_now}.csv"

    write_to_gcs(df=insights_df, filepath=write_path)


default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="insights_dag1",
    start_date=datetime(2024, 7, 14),
    schedule_interval="0 10 * * *",  # Daily interval at 10am
    catchup=False,
    tags=[
        "Orchestration",
        "Customer Insights",
        "Ecommerce"
    ],

):
    etl_with_spark = PythonOperator(
        task_id="etl_with_spark", python_callable=etl_with_spark
    )

    does_nothing = EmptyOperator(task_id="does_nothing")

    etl_with_spark >> does_nothing



