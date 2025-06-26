from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("NYC Taxi Load to Snowflake").getOrCreate()

# Load Trip Table
trip_df = spark.read.parquet("/home/pranav_shirali/airflow-project/scripts/ETL/nyc_taxi_trip.parquet")
sfOptions_trip = {
    "sfURL": "VSIVUBN-UP35521.snowflakecomputing.com",
    "sfUser": "pranavshirali",
    "sfPassword": "@MJd723*tiw&LN",  # 🔐 Use env var
    "sfDatabase": "AIRFLOW_DB",
    "sfSchema": "ETL_TRANSFORMED",
    "sfWarehouse": "AIRFLOW_WH",
    "sfRole": "ACCOUNTADMIN"
}
trip_df.write.format("snowflake").options(**sfOptions_trip).mode("overwrite").save()

# Load Payment Table
payment_df = spark.read.parquet("/home/pranav_shirali/airflow-project/scripts/ETL/nyc_taxi_payment.parquet")
sfOptions_payment = sfOptions_trip.copy()
sfOptions_payment["dbtable"] = "nyc_taxi_payment"
payment_df.write.format("snowflake").options(**sfOptions_payment).mode("overwrite").save()