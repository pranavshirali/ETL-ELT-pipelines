from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# ---------------- Default Args ----------------
default_args = {
    'start_date': datetime(2024, 5, 11),
}

# ---------------- DAG Definition ----------------
with DAG(
    dag_id='nyc_taxi_etl',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['pyspark', 'nyc_taxi', 'etl'],
) as dag:

    # Option A: Use single PySpark script for Transform + Load
    transform_and_load = BashOperator(
        task_id='transform_and_load',
        bash_command='/opt/spark/bin/spark-submit /home/pranav_shirali/airflow-project/scripts/ETL/transform_and_load.py'
    )

    transform_and_load

    # ---------------- Optional: Split ETL Tasks ----------------
    # Uncomment below if you want to break into separate stages

    # extract = BashOperator(
    #     task_id='extract_from_azure',
    #     bash_command='/opt/spark/bin/spark-submit /home/pranav_shirali/airflow-project/scripts/ETL/extract.py'
    # )
    
    # transform = BashOperator(
    #     task_id='transform_data',
    #     bash_command='/opt/spark/bin/spark-submit /home/pranav_shirali/airflow-project/scripts/ETL/transform.py'
    # )

    # load = BashOperator(
    #     task_id='load_to_snowflake',
    #     bash_command='/opt/spark/bin/spark-submit /home/pranav_shirali/airflow-project/scripts/ETL/load.py'
    # )

    # extract >> transform >> load
