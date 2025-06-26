from pyspark.sql import SparkSession
from pyspark.sql.functions import month, when, lit
import requests
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("NYC Taxi Transform").getOrCreate()

df = spark.read.parquet("/home/pranav_shirali/airflow-project/scripts/ETL/nyc_taxi_extracted.parquet")

# 1. Add Month column
df = df.withColumn("Month", month(df["tpep_pickup_datetime"]))

# 2. Add Car_Utilization column
df = df.withColumn(
    "Car_utilization",
    when((df.passenger_count == 1) | (df.passenger_count == 2), "Low")
    .when((df.passenger_count == 3) | (df.passenger_count == 4), "Medium")
    .when((df.passenger_count > 4), "Full")
    .otherwise("Unknown")
)

# 3. Add Distance_Length column
df = df.withColumn(
    "Distance_Length",
    when((df.trip_distance >= 1) & (df.trip_distance < 5), "Short")
    .when((df.trip_distance >= 5) & (df.trip_distance < 10), "Medium")
    .when(df.trip_distance >= 10, "Long")
    .otherwise("Unknown")
)

# 4. Add trip_amt_INR column
def get_usd_inr():
    try:
        response = requests.get("https://api.exchangerate-api.com/v4/latest/USD", timeout=10)
        response.raise_for_status()
        return response.json()["rates"]["INR"]
    except Exception as e:
        print("Error fetching exchange rate, defaulting to 83.0:", e)
        return 83.0

usd_inr = get_usd_inr()
df = df.withColumn("trip_amt_INR", df["total_amount"] * lit(usd_inr))

# Drop the unnecessary column
#df = df.drop("__null_dask_index__")


df = df.withColumn("VendorID", col("VendorID").cast("int"))
df = df.withColumn("passenger_count", col("passenger_count").cast("int"))
df = df.withColumn("RatecodeID", col("RatecodeID").cast("int"))
df = df.withColumn("payment_type", col("payment_type").cast("int"))
df = df.withColumn("PULocationID", col("PULocationID").cast("bigint"))
df = df.withColumn("DOLocationID", col("DOLocationID").cast("bigint"))

# Define window for sequential IDs
window = Window.orderBy("tpep_pickup_datetime", "VendorID")

# Add Trip_id (sequential)
df = df.withColumn("Trip_id", row_number().over(window))
df = df.withColumn("Payment_id", row_number().over(window))

# Cast datetime columns to string for Snowflake compatibility
df = df.withColumn("tpep_pickup_datetime", df["tpep_pickup_datetime"].cast("string"))
df = df.withColumn("tpep_dropoff_datetime", df["tpep_dropoff_datetime"].cast("string"))

# Trip Table columns
trip_columns = [
    "Trip_id", "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag",
    "PULocationID", "DOLocationID", "Month", "Distance_Length", "Car_utilization", "__null_dask_index__"
]

# Payment Table columns
payment_columns = [
    "Trip_id",  # Foreign Key
    "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", "total_amount", "trip_amt_INR", "congestion_surcharge", "__null_dask_index__"
]


# Final DataFrames
trip_df = df.select(*trip_columns)
payment_columns_final = ["Payment_id"] + payment_columns
payment_df = df.select(*payment_columns_final)

# Save as Parquet for loading step
trip_df.write.mode("overwrite").parquet("/home/pranav_shirali/airflow-project/scripts/ETL/nyc_taxi_trip.parquet")
payment_df.write.mode("overwrite").parquet("/home/pranav_shirali/airflow-project/scripts/ETL/nyc_taxi_payment.parquet")