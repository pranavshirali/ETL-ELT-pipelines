from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
import pendulum

# ------------------ Configuration ------------------
local_tz = pendulum.timezone("Asia/Kolkata")
start_dt = pendulum.datetime(2025, 5, 27, 17, 38, 0, tz=local_tz)

AIRBYTE_CONN_ID = 'airbyte_cloud'  # Airbyte Cloud connection (set in Airflow Connections)
AIRBYTE_CONNECTION_ID = '28c81c5c-fa42-428e-b902-afe4faf27bfe'  # Airbyte connection UUID

# ------------------ DAG Definition ------------------
with DAG(
    dag_id='airbyte_sync_dag',
    start_date=start_dt,
    schedule_interval='40 17 * * *',  # Runs every day at 5:40 PM IST
    catchup=True,
    tags=['airbyte'],
) as dag:

    # Trigger Airbyte sync job
    trigger_sync = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync',
        airbyte_conn_id=AIRBYTE_CONN_ID,
        connection_id=AIRBYTE_CONNECTION_ID,
    )

    # Wait for the sync job to complete
    wait_for_sync = AirbyteJobSensor(
        task_id='wait_for_airbyte_sync',
        airbyte_conn_id=AIRBYTE_CONN_ID,
        airbyte_job_id=trigger_sync.output,
    )

    # Task dependency
    trigger_sync >> wait_for_sync
