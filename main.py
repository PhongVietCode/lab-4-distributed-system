from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, to_timestamp, udf
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
    StructField("temperature", StringType(), True),
    StructField("moisture", StringType(), True),
    StructField("light", StringType(), True),
    StructField("total_rainfall", StringType(), True),
    StructField("rainfall", StringType(), True),
    StructField("wind_direction", StringType(), True),
    StructField("pm25", StringType(), True),
    StructField("pm10", StringType(), True),
    StructField("co", StringType(), True),
    StructField("nox", StringType(), True),
    StructField("so2", StringType(), True)
])

earth_schema = StructType([
    StructField("data_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("station", StringType(), True),
    StructField("moisture", StringType(), True),
    StructField("temperature", StringType(), True),
    StructField("salinity", StringType(), True),
    StructField("ph", StringType(), True),
    StructField("water_root", StringType(), True),
    StructField("water_leaf", StringType(), True),
    StructField("water_level", StringType(), True),
    StructField("voltage", StringType(), True)
])

water_schema = StructType([
    StructField("data_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("station", StringType(), True),
    StructField("ph", StringType(), True),
    StructField("do", StringType(), True),
    StructField("temperature", StringType(), True),
    StructField("salinity", StringType(), True)
])



def deserialize_air(value):
    try:
        buffer = memoryview(value)
        offset = 0

        # Helper function to read a length-prefixed UTF-8 string
        def read_string():
            nonlocal offset
            length = struct.unpack_from(">I", buffer, offset)[0]  # Read string length
            offset += 4
            string_value = buffer[offset:offset + length].tobytes().decode("utf-8")  # Read the actual string
            offset += length
            return string_value

        # Deserialize each field
        data_type = read_string()
        timestamp = read_string()
        station = read_string()
        temperature = read_string()
        moisture = read_string()
        light = read_string()
        total_rainfall = read_string()
        rainfall = read_string()
        wind_direction = read_string()
        pm25 = read_string()
        pm10 = read_string()
        co = read_string()
        nox = read_string()
        so2 = read_string()

        return (data_type, timestamp, station, temperature, moisture, light, total_rainfall,
                rainfall, wind_direction, pm25, pm10, co, nox, so2)
    except Exception as e:
        # Handle parsing error
        return None

def deserialize_earth(value):
    try:
        buffer = memoryview(value)
        offset = 0

        def read_string():
            nonlocal offset
            length = struct.unpack_from(">I", buffer, offset)[0]
            offset += 4
            string_value = buffer[offset:offset + length].tobytes().decode("utf-8")
            offset += length
            return string_value

        # Deserialize fields
        data_type = read_string()
        timestamp = read_string()
        station = read_string()
        moisture = read_string()
        temperature = read_string()
        salinity = read_string()
        pH = read_string()
        water_Root = read_string()
        water_Leaf = read_string()
        water_Level = read_string()
        voltage = read_string()

        return (data_type, timestamp, station, moisture, temperature, salinity, pH,
                water_Root, water_Leaf, water_Level, voltage)
    except Exception as e:
        # Handle parsing errors
        print(f"Error deserializing Earth data: {e}")
        return None

def deserialize_water(value):
    try:
        buffer = memoryview(value)
        offset = 0

        def read_string():
            nonlocal offset
            length = struct.unpack_from(">I", buffer, offset)[0]
            offset += 4
            string_value = buffer[offset:offset + length].tobytes().decode("utf-8")
            offset += length
            return string_value

        # Deserialize fields
        data_type = read_string()
        timestamp = read_string()
        station = read_string()
        pH = read_string()
        do = read_string()
        temperature = read_string()
        salinity = read_string()

        return (data_type, timestamp, station, pH, do, temperature, salinity)
    except Exception as e:
        # Handle parsing errors
        print(f"Error deserializing Water data: {e}")
        return None

# Register the Deserialization Function as a UDF
deserialize_air_udf = udf(deserialize_air, air_schema)
deserialize_earth_udf = udf(deserialize_earth, earth_schema)
deserialize_water_udf = udf(deserialize_water, water_schema)


def read_air_stream(spark, kafka_servers, topic):
    spark = SparkSession.builder.getOrCreate()

    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Deserialize Kafka messages
    air_stream = kafka_stream \
        .selectExpr("CAST(value AS BINARY) as raw_value") \
        .withColumn("data", deserialize_air_udf("raw_value")) \
        .select(
            col("data.data_type").alias("data_type"),
            to_timestamp(col("data.timestamp"), "yyyy-MM-dd'T'HH:mm:ss").alias("timestamp"),
            col("data.station").alias("station"),
            col("data.temperature").alias("temperature"),
            col("data.moisture").alias("moisture"),
            col("data.light").alias("light"),
            col("data.total_rainfall").alias("total_rainfall"),
            col("data.rainfall").alias("rainfall"),
            col("data.wind_direction").alias("wind_direction"),
            col("data.pm25").alias("pm25"),
            col("data.pm10").alias("pm10"),
            col("data.co").alias("co"),
            col("data.nox").alias("nox"),
            col("data.so2").alias("so2")
        )

    return air_stream

def read_earth_stream(spark, kafka_servers, topic):
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    earth_stream = kafka_stream.selectExpr("CAST(value AS BINARY) as raw_value") \
        .withColumn("data", deserialize_earth_udf("raw_value")) \
        .select(
            col("data.data_type").alias("data_type"),
            to_timestamp(col("data.timestamp"), "dd/MM/yyyy HH:mm:ss").alias("timestamp"),
            col("data.station").alias("station"),
            col("data.moisture").alias("moisture"),
            col("data.temperature").alias("temperature"),
            col("data.salinity").alias("salinity"),
            col("data.pH").alias("pH"),
            col("data.water_Root").alias("water_Root"),
            col("data.water_Leaf").alias("water_Leaf"),
            col("data.water_Level").alias("water_Level"),
            col("data.voltage").alias("voltage")
        )

    return earth_stream

def read_water_stream(spark, kafka_servers, topic):
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    water_stream = kafka_stream.selectExpr("CAST(value AS BINARY) as raw_value") \
        .withColumn("data", deserialize_water_udf("raw_value")) \
        .select(
            col("data.data_type").alias("data_type"),
            to_timestamp(col("data.timestamp"), "dd/MM/yyyy HH:mm:ss").alias("timestamp"),
            col("data.station").alias("station"),
            col("data.pH").alias("pH"),
            col("data.DO").alias("DO"),
            col("data.temperature").alias("temperature"),
            col("data.salinity").alias("salinity")
        )

    return water_stream

# Step 1: Create Spark Session
spark = SparkSession.builder \
    .appName("KafkaJoinToHDFS") \
    .getOrCreate()

# Step 3: Read Streams for Air, Earth, and Water Topics
air_stream = read_air_stream(spark, KAFKA_BOOTSTRAP_SERVERS, AIR_TOPIC)
earth_stream = read_earth_stream(spark, KAFKA_BOOTSTRAP_SERVERS, EARTH_TOPIC)
water_stream = read_water_stream(spark, KAFKA_BOOTSTRAP_SERVERS, WATER_TOPIC)

# # Step 4: Join the Streams on Timestamp and Station
# joined_stream = air_stream \
#     .join(earth_stream, ["timestamp"], "inner") \
#     .join(water_stream, ["timestamp"], "inner")

# Write input streams to console
air_stream.writeStream.format("console").outputMode("append").start()
earth_stream.writeStream.format("console").outputMode("append").start()
water_stream.writeStream.format("console").outputMode("append").start()

# Debug joined stream
joined_stream.writeStream.format("console").outputMode("append").start()

# Write joined stream to HDFS
query = joined_stream.writeStream \
    .format("csv") \
    .option("path", "hdfs://node1:9000/env_data_2/") \
    .option("checkpointLocation", "hdfs://node1:9000/checkpoint/") \
    .option("header", "true") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()

