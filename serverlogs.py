from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Create Spark session
spark = SparkSession.builder \
    .appName("ServerLogsConsumer") \
    .getOrCreate()

# Define schema for the log data
schema = StructType([
    StructField("ip_address", StringType(), True),
    StructField("user_name", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("http_method", StringType(), True),
    StructField("path", StringType(), True),
    StructField("status_code", IntegerType(), True)
])



def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("ClickAnalytics") \
        .getOrCreate()
    
    kafka_stream_df = spark.readStream \
        .format('kafka')\
        .option('kafka.bootstrap.servers', BOOTSTRAP_SERVERS)\
        .option('subscribe',TOPIC)\
        .load()

    query = kafka_stream_df \
    .writeStream \
    .format('console') \
    .outputMode('append') \
    .start() \
    .awaitTermination ()

# Read streaming data from Kafka
server_logs_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "server_logs") \
    .load()

# Extract relevant fields from the data
logs_df = server_logs_df.selectExpr("CAST(value AS STRING) as log_json") \
    .select(from_json(col("log_json"), schema).alias("data")) \
    .select("data.*")



# SQL
def write_to_postgres(df, epoch_id):
    mode="overwrite"
    # the following variables are unique to each person:
    postgres_port="5432"
    database_name="analytics"
    user_name="postgres"
    password="hello"
    table_name="analytics"
    # then use these values:
    url = f"jdbc:postgresql://host.docker.internal:{postgres_port}/{database_name}"
    properties = {"user": user_name, "password": password, "driver": "org.postgresql.Driver"}
    df.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)

    .start() \
    .awaitTermination()
