from pyspark.sql import SparkSession
from pyspark.sql.functions import month, when, lit, col, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
import requests

# ------------------------- Spark Session Initialization -------------------------
spark = (
    SparkSession.builder
    .appName("NYC Taxi Extract")
    .config("spark.jars", "/opt/spark/jars/hadoop-azure-3.3.1.jar,"
                          "/opt/spark/jars/azure-storage-8.6.6.jar,"
                          "/opt/spark/jars/hadoop-azure-datalake-3.3.1.jar")
    .config("spark.hadoop.fs.azure.account.auth.type.tietoevryproject1.dfs.core.windows.net", "SharedKey")
    .config("spark.hadoop.fs.azure.account.key.tietoevryproject1.dfs.core.windows.net", 
            "j1X0f596sQmm2mcRQydj1JQXyhinvkYj+TqtA1uYaPaymIqqz/pAYM0sFBAd/uuRqR1qgunf83wM+ASt3dQnCQ==")
    .getOrCreate()
)

# ------------------------- Load Parquet File -------------------------
file_path = "abfss://nyc-taxi-data@tietoevryproject1.dfs.core.windows.net/part.0.parquet"
df = spark.read.parquet(file_path)

# ------------------------- Transformations -------------------------
# 1. Add Month Column
df = df.withColumn("Month", month("tpep_pickup_datetime"))

# 2. Add Car Utilization Column
df = df.withColumn(
    "Car_utilization",
    when((col("passenger_count") <= 2), "Low")
    .when((col("passenger_count") <= 4), "Medium")
    .when(col("passenger_count") > 4, "Full")
    .otherwise("Unknown")
)

# 3. Add Distance Length Column
df = df.withColumn(
    "Distance_Length",
    when((col("trip_distance") >= 1) & (col("trip_distance") < 5), "Short")
    .when((col("trip_distance") >= 5) & (col("trip_distance") < 10), "Medium")
    .when(col("trip_distance") >= 10, "Long")
    .otherwise("Unknown")
)

# ------------------------- Currency Conversion -------------------------
def get_usd_inr():
    try:
        url = "http://api.exchangerate.host/live"
        params = {
            "access_key": "251136c87e84b6cfe422e03bae16429b",
            "format": "1"
        }
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data.get("success") and "quotes" in data and "USDINR" in data["quotes"]:
            return data["quotes"]["USDINR"]
        else:
            raise ValueError("Unexpected API response format or missing USDINR rate.")
            
    except Exception as e:
        print("Error fetching exchange rate from exchangerate.host, defaulting to 83.0:", e)
        return 83.0

# 4. Currency Conversion
usd_inr = get_usd_inr()
df = df.withColumn("trip_amt_INR", col("total_amount") * lit(usd_inr))

# ------------------------- Type Casting -------------------------
df = df.withColumn("VendorID", col("VendorID").cast("int"))
df = df.withColumn("passenger_count", col("passenger_count").cast("int"))
df = df.withColumn("RatecodeID", col("RatecodeID").cast("int"))
df = df.withColumn("payment_type", col("payment_type").cast("int"))
df = df.withColumn("PULocationID", col("PULocationID").cast("bigint"))
df = df.withColumn("DOLocationID", col("DOLocationID").cast("bigint"))

# ------------------------- Unique ID Generation -------------------------
trip_window = Window.orderBy("tpep_pickup_datetime", "VendorID")
payment_window = Window.orderBy("tpep_pickup_datetime", "payment_type")

df = df.withColumn("Trip_id", row_number().over(trip_window))
df = df.withColumn("Payment_id", row_number().over(payment_window))

# ------------------------- Date Formatting -------------------------
df = df.withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast(TimestampType()))
df = df.withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast(TimestampType()))

# ------------------------- Split DataFrames -------------------------
trip_columns = [
    "Trip_id", "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag",
    "PULocationID", "DOLocationID", "Payment_id", "Month",
    "Car_utilization", "Distance_Length", "trip_amt_INR"
]
trip_df = df.select(*trip_columns)

payment_columns = [
    "Payment_id", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge"
]
payment_df = df.select(*payment_columns)

# ------------------------- Snowflake Write -------------------------
sfOptions = {
    "sfURL": "VSIVUBN-UP35521.snowflakecomputing.com",
    "sfUser": "pranavshirali",
    "sfPassword": "@MJd723*tiw&LN", 
    "sfDatabase": "AIRFLOW_DB",
    "sfSchema": "ETL_TRANSFORMED",
    "sfWarehouse": "AIRFLOW_WH",
    "sfRole": "ACCOUNTADMIN"
}

# Write to Snowflake
trip_df.write.format("snowflake").options(**sfOptions, dbtable="nyc_taxi_trip").mode("overwrite").save()
payment_df.write.format("snowflake").options(**sfOptions, dbtable="nyc_taxi_payment").mode("overwrite").save()
