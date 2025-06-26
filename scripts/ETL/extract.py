from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("NYC Taxi Extract")
    .config("spark.jars", "/opt/spark/jars/hadoop-azure-3.3.1.jar,"
                         "/opt/spark/jars/azure-storage-8.6.6.jar,"
                         "/opt/spark/jars/hadoop-azure-datalake-3.3.1.jar")
    .config("spark.hadoop.fs.azure.account.auth.type.tietoevryproject1.dfs.core.windows.net", "SharedKey")
    .config("spark.hadoop.fs.azure.account.key.tietoevryproject1.dfs.core.windows.net", 
            "j1X0f596sQmm2mcRQydj1JQXyhinvkYj+TqtA1uYaPaymIqqz/pAYM0sFBAd/uuRqR1qgunf83wM+ASt3dQnCQ==")
    .getOrCreate()
)

file_path = "abfss://nyc-taxi-data@tietoevryproject1.dfs.core.windows.net/part.0.parquet"
df = spark.read.parquet(file_path)
df.write.mode("overwrite").parquet("/home/pranav_shirali/airflow-project/scripts/ETL/nyc_taxi_extracted.parquet")