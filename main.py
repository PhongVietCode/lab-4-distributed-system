from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = "192.168.0.68:9092"  # Replace with your Kafka broker addresses
AIR_TOPIC = "AIR"
EARTH_TOPIC = "EARTH"
WATER_TOPIC = "WATER"

# Define Schemas for Each Topic
air_schema = StructType([
    StructField("data_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("station", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("moisture", DoubleType(), True),
    StructField("light", DoubleType(), True),
    StructField("total_rainfall", DoubleType(), True)
])

earth_schema = StructType([
    StructField("data_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("station", StringType(), True),
    StructField("moisture", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("salinity", DoubleType(), True),
    StructField("ph", DoubleType(), True)
])

water_schema = StructType([
    StructField("data_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("station", StringType(), True),
    StructField("ph", DoubleType(), True),
    StructField("do", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("salinity", DoubleType(), True)
])

# Step 1: Create Spark Session
spark = SparkSession.builder \
    .appName("KafkaJoinToHDFS") \
    .getOrCreate()

# Step 2: Define a Function to Read and Parse Each Kafka Stream
def read_kafka_stream(topic, schema):
    stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()
    
    parsed_stream = stream.selectExpr("CAST(value AS STRING) as raw_value") \
        .withColumn("fields", split(col("raw_value"), ",")) \
        .select(
            col("fields").getItem(0).alias("data_type"),
            col("fields").getItem(1).alias("timestamp"),
            col("fields").getItem(2).alias("station"),
            *[
                col("fields").getItem(i).cast(schema.fields[i].dataType).alias(schema.fields[i].name)
                for i in range(3, len(schema.fields))
            ]
        ) \
        .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
    
    return parsed_stream

# Step 3: Read Streams for Air, Earth, and Water Topics
air_stream = read_kafka_stream(AIR_TOPIC, air_schema)
earth_stream = read_kafka_stream(EARTH_TOPIC, earth_schema)
water_stream = read_kafka_stream(WATER_TOPIC, water_schema)

# Step 4: Join the Streams on Timestamp and Station
joined_stream = air_stream \
    .join(earth_stream, ["timestamp"], "inner") \
    .join(water_stream, ["timestamp"], "inner")

# Step 5: Write the Joined Stream to HDFS
query = joined_stream.writeStream \
    .format("csv") \
    .option("path", "hdfs://node1:9000/env_data_2/") \
    .option("checkpointLocation", "hdfs://node1:9000/checkpoint") \
    .option("header", "true") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
