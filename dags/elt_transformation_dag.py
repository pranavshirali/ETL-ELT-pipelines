from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
import pendulum

# ------------------ DAG Configuration ------------------
local_tz = pendulum.timezone("Asia/Kolkata")
start_dt = pendulum.datetime(2025, 5, 27, 17, 38, 0, tz=local_tz)

with DAG(
    dag_id='elt_sales_stream_transformation',
    start_date=start_dt,                          # timezone-aware start date
    schedule_interval='10 18 * * *',              # Runs every day at 6:10 PM IST
    catchup=True,
    tags=['elt', 'snowflake'],
) as dag:

    # ------------------ ELT Transformation in Snowflake ------------------
    transform_sales_stream = SQLExecuteQueryOperator(
        task_id='transform_sales_stream',
        conn_id='snowflake_con',
        sql="""
        -- ------------------ Region Dimension ------------------
        CREATE OR REPLACE TABLE AIRFLOW_DB.ELT_TRANSFORMED.Region AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY REGION) AS Region_id,
            REGION
        FROM (
            SELECT DISTINCT REGION
            FROM AIRFLOW_DB.ELT_TRANSFORMED."SALES DATA RAW"
            WHERE REGION IS NOT NULL
        );

        -- ------------------ Country Dimension ------------------
        CREATE OR REPLACE TABLE AIRFLOW_DB.ELT_TRANSFORMED.Country AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY COUNTRY) AS Country_id,
            COUNTRY
        FROM (
            SELECT DISTINCT COUNTRY
            FROM AIRFLOW_DB.ELT_TRANSFORMED."SALES DATA RAW"
            WHERE COUNTRY IS NOT NULL
        );

        -- ------------------ Item Dimension ------------------
        CREATE OR REPLACE TABLE AIRFLOW_DB.ELT_TRANSFORMED.Item AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY "ITEM TYPE") AS Item_id,
            "ITEM TYPE" AS Item_type
        FROM (
            SELECT DISTINCT "ITEM TYPE"
            FROM AIRFLOW_DB.ELT_TRANSFORMED."SALES DATA RAW"
            WHERE "ITEM TYPE" IS NOT NULL
        );

        -- ------------------ Order Priority Dimension ------------------
        CREATE OR REPLACE TABLE AIRFLOW_DB.ELT_TRANSFORMED.Order_Priority AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY ORDER_PRIORITY_FULL) AS OrderPriority_id,
            ORDER_PRIORITY_FULL AS ORDER_PRIORITY
        FROM (
            SELECT DISTINCT
                CASE 
                    WHEN "ORDER PRIORITY" = 'H' THEN 'High'
                    WHEN "ORDER PRIORITY" = 'L' THEN 'Low'
                    WHEN "ORDER PRIORITY" = 'M' THEN 'Medium'
                    WHEN "ORDER PRIORITY" = 'C' THEN 'Cold'
                    ELSE "ORDER PRIORITY"
                END AS ORDER_PRIORITY_FULL
            FROM AIRFLOW_DB.ELT_TRANSFORMED."SALES DATA RAW"
            WHERE "ORDER PRIORITY" IS NOT NULL
        );

        -- ------------------ Orders Fact Table ------------------
        CREATE OR REPLACE TABLE AIRFLOW_DB.ELT_TRANSFORMED.Orders AS
        SELECT
            s."ORDER ID",
            s."ITEM TYPE",
            s."SHIP DATE",
            s."UNIT COST",
            s."ORDER DATE",
            s."TOTAL COST",
            s."UNIT PRICE",
            s."UNITS SOLD",
            s."TOTAL PROFIT",
            s."SALES CHANNEL",
            s."TOTAL REVENUE",
            r.Region_id,
            c.Country_id,
            i.Item_id,
            op.OrderPriority_id,
            DATEDIFF(
                'day',
                TRY_TO_DATE(s."ORDER DATE", 'MM/DD/YYYY'),
                TRY_TO_DATE(s."SHIP DATE", 'MM/DD/YYYY')
            ) AS SHIPMENT_LEAD_TIME
        FROM (
            SELECT *,
                CASE 
                    WHEN "ORDER PRIORITY" = 'H' THEN 'High'
                    WHEN "ORDER PRIORITY" = 'L' THEN 'Low'
                    WHEN "ORDER PRIORITY" = 'M' THEN 'Medium'
                    WHEN "ORDER PRIORITY" = 'C' THEN 'Cold'
                    ELSE "ORDER PRIORITY"
                END AS ORDER_PRIORITY_FULL
            FROM AIRFLOW_DB.ELT_TRANSFORMED."SALES DATA RAW"
            WHERE 
                "ORDER ID" IS NOT NULL AND
                "ITEM TYPE" IS NOT NULL AND
                REGION IS NOT NULL AND
                COUNTRY IS NOT NULL AND
                "ORDER PRIORITY" IS NOT NULL
        ) s
        JOIN AIRFLOW_DB.ELT_TRANSFORMED.Region r 
            ON r.Region = s.REGION
        JOIN AIRFLOW_DB.ELT_TRANSFORMED.Country c 
            ON c.Country = s.COUNTRY
        JOIN AIRFLOW_DB.ELT_TRANSFORMED.Item i 
            ON i.Item_type = s."ITEM TYPE"
        JOIN AIRFLOW_DB.ELT_TRANSFORMED.Order_Priority op 
            ON op.ORDER_PRIORITY = s.ORDER_PRIORITY_FULL;
        """
    )
