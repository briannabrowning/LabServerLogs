from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

BOOTSTRAP_SERVERS = "confluent-local-broker-1:62444"
TOPIC = "server_logs"

# DATABASE SETTINGS
# the following variables are unique to each person:
DB_PORT="5432"
DB_NAME="server_logs"
DB_USERNAME="postgres"
DB_PASSWORD="hellosql"
# ^^^ confirm your own values
DB_URL = f"jdbc:postgresql://host.docker.internal:{DB_PORT}/{DB_NAME}"
DB_PROPERTIES = {"user": DB_USERNAME, "password": DB_PASSWORD, "driver": "org.postgresql.Driver"}

def write_logs_to_postgres(df, epoch_id):
    mode="append"
    table_name="server_logs"
    df.write.jdbc(url=DB_URL, table=table_name, mode=mode, properties=DB_PROPERTIES)


def write_errors_to_postgres2(df, epoch_id):
    mode="overwrite"
    table_name="errors_by_path"
    df.write.jdbc(url=DB_URL, table=table_name, mode=mode, properties=DB_PROPERTIES)
    
def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("ClickAnalytics") \
        .getOrCreate()
    
    
    # Read from Kafka
    kafka_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC) \
        .load()

    # write data to sql---------------------------------------------------------------------
    df = kafka_stream_df \
        .selectExpr("CAST(value AS STRING) as value") \
        .select(F.regexp_replace(F.col("value"), r'[\[\]"]', "").alias("cleaned_value")) \
        .select(F.split(F.col("cleaned_value"), " ").alias("data")) \
        .select(
            F.col("data").getItem(0).cast(StringType()).alias("ip_address"),
            F.col("data").getItem(1).cast(StringType()).alias("user_name"),
            F.col("data").getItem(2).cast(IntegerType()).alias("user_id"),
            F.col("data").getItem(3).cast(TimestampType()).alias("timestamp"),
            F.col("data").getItem(4).cast(StringType()).alias("http_method"),
            F.col("data").getItem(5).cast(StringType()).alias("path"),
            F.col("data").getItem(6).cast(IntegerType()).alias("status_code"),
        ) 
    
    df.writeStream \
        .outputMode('append') \
        .foreachBatch(write_logs_to_postgres) \
        .start()


    # find top errors---------------------------------------------------------------------
    filtered_df = df.filter((F.col("status_code") == 404) | (F.col("status_code") == 500))

    # Group by action_detail and count occurrences
    # df_agg = filtered_df.groupBy("path").agg(F.count_if(F.col("status_code")==404).alias("404_errors"),F.count_if(F.col("status_code")==500).alias("500_errors"), F.count(F.col("*")).alias("total")).orderBy(F.col("total").desc())
    
    # df_agg.writeStream \
    #     .outputMode("complete") \
    #     .foreachBatch(write_errors_to_postgres2) \
    #     .start()

    # avg click rate per user
    
    # window = Window.orderBy("ip_address")
    window_duration = "1 minute"
    # Create a 1-minute tumbling window on the 'timestamp' column
    window_spec = F.window("timestamp", "1 minute")

    # Group by ip_address and window of 1 minute, and count the logs
    window_agg = df.withWatermark('timestamp', window_duration).groupBy(window_spec, "ip_address") \
        .count()


    # Include window time and window end 
    windowed_df = window_agg.select(
        window_agg.window.start.alias("window_start"),
        window_agg.window.end.alias("window_end"),
        "ip_address",
        "count"
    )

    # # sort and add dos_attack column (boolean)
    windowed_df = windowed_df.withColumn("dos_attack", F.expr("count > 100")) \
    .filter(F.col("dos_attack") == True) \
    .orderBy(F.desc("count"))

    # # Write the result to the console
    windowed_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()
    
    # filtered_df = windowed_df.filter(col("dos_attack") == True)
    
    # filtered_df.writeStream \
    #     .foreachBatch(write_to_postgres) \
    #     .outputMode('append') \
    #     .start()

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()