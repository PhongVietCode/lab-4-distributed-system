from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from io import BytesIO
from datetime import datetime

import sys

# Redirect stdout and stderr to a log file
log_file = open("terminal_logs.txt", "w")
sys.stdout = log_file
sys.stderr = log_file

kafka_params = {
    "kafka.bootstrap.servers": "192.168.0.68:9092",
    "subscribe": "air,earth,water",
    "startingOffsets": "earliest"
}

def create_spark_session():
    return SparkSession.builder \
        .appName("EnvironmentalSensorProcessing") \
        .config("spark.sql.streaming.checkpointLocation", "hdfs://node1:9000/checkpoint") \
        .getOrCreate()

def create_schemas():
    air_schema = StructType([
        StructField("type", StringType(), True),
        StructField("date", TimestampType(), True),
        StructField("air_station", StringType(), True),
        StructField("air_temperature", DoubleType(), True),
        StructField("air_moisture", DoubleType(), True),
        StructField("air_light", DoubleType(), True),
        StructField("air_total_rainfall", DoubleType(), True),
        StructField("air_rainfall", DoubleType(), True),
        StructField("air_wind_direction", DoubleType(), True),
        StructField("air_pm25", DoubleType(), True),
        StructField("air_pm10", DoubleType(), True),
        StructField("air_co", DoubleType(), True),
        StructField("air_nox", DoubleType(), True),
        StructField("air_so2", DoubleType(), True)
    ])

    earth_schema = StructType([
        StructField("type", StringType(), True),
        StructField("date", TimestampType(), True),
        StructField("earth_station", StringType(), True),
        StructField("earth_moisture", DoubleType(), True),
        StructField("earth_temperature", DoubleType(), True),
        StructField("earth_salinity", DoubleType(), True),
        StructField("earth_ph", DoubleType(), True),
        StructField("earth_water_root", DoubleType(), True),
        StructField("earth_water_leaf", DoubleType(), True),
        StructField("earth_water_level", DoubleType(), True),
        StructField("earth_voltage", DoubleType(), True)
    ])

    water_schema = StructType([
        StructField("type", StringType(), True),
        StructField("date", TimestampType(), True),
        StructField("water_station", StringType(), True),
        StructField("water_ph", DoubleType(), True),
        StructField("water_do", DoubleType(), True),
        StructField("water_temperature", DoubleType(), True),
        StructField("water_salinity", DoubleType(), True)
    ])

    return air_schema, earth_schema, water_schema

def read_stream(spark, topic, schema):
    stream = spark.readStream \
                    .format("kafka") \
                    .options(**kafka_params) \
                    .option("subscribe", topic) \
                    .load()
    
    def deserialize(binary_data):
        if binary_data is None:
            result = []
            for field in schema.fields:
                result.append(None)
            return result
        try:
            buf = BytesIO(binary_data)
            result = []
            
            for field in schema.fields:
                length_bytes = buf.read(4)
                if not length_bytes:
                    result = []
                    for field in schema.fields:
                        result.append(None)
                    return result
                length = int.from_bytes(length_bytes, byteorder='big')

                value_bytes = buf.read(length)
                if not value_bytes:
                    result = []
                    for field in schema.fields:
                        result.append(None)
                    return result
                value_str = value_bytes.decode('utf-8')
                
                if value_str == "null":
                    result.append(None)
                    continue
                
                try:
                    if isinstance(field.dataType, TimestampType):
                        dt = datetime.strptime(value_str, '%d/%m/%Y %H:%M:%S')
                        result.append(dt)
                    elif isinstance(field.dataType, DoubleType):
                        result.append(float(value_str))
                    else:
                        result.append(value_str)
                except:
                    result.append(None)
            
            return tuple(result)
        except Exception as e:
            result = []
            for field in schema.fields:
                result.append(None)
            return result

    deserializer = udf(deserialize, schema)

    returned_stream = stream.select(deserializer(col("value")).alias("deserialized")) \
                            .select("deserialized.*") \
                            .filter(col("date").isNotNull())

    return returned_stream

def main():
    spark = create_spark_session()
    air_schema, earth_schema, water_schema = create_schemas()
    
    # air_df = imputer.process_dataframe(read_stream("air", air_schema), "air")
    # earth_df = imputer.process_dataframe(read_stream("earth", earth_schema), "earth")
    # water_df = imputer.process_dataframe(read_stream("water", water_schema), "water")
    air_df = read_stream(spark, "AIR", air_schema)
    earth_df = read_stream(spark, "EARTH", earth_schema)
    water_df = read_stream(spark, "WATER", water_schema)
    air_df = air_df.withColumn("date", to_timestamp("date", "yyyy-MM-dd HH:mm:ss")) \
                .dropDuplicates(["date", "station"])

    earth_df = earth_df.withColumn("date", to_timestamp("date", "yyyy-MM-dd HH:mm:ss")) \
                    .dropDuplicates(["date", "station"])

    water_df = water_df.withColumn("date", to_timestamp("date", "yyyy-MM-dd HH:mm:ss")) \
                    .dropDuplicates(["date", "station"])
    joined_df = air_df \
        .withWatermark("date", "1 second") \
        .join(
            earth_df.withWatermark("date", "1 second"),
            "date",
            "outer"
        ) \
        .join(
            water_df.withWatermark("date", "1 second"),
            "date",
            "outer"
        )

    # query = joined_df.writeStream.format("console") \
    #                              .option("truncate", False) \
    #                              .option("numRows", 10) \
    #                              .option("maxColumnWidth", 100) \
    #                              .start()

    query = joined_df.writeStream \
                     .format("csv") \
                     .option("path", "hdfs://node1:9000/environment_datas") \
                     .option("checkpointLocation", "hdfs://node1:9000/checkpoint") \
                     .option("header", "true") \
                     .trigger(processingTime="1 second") \
                     .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
    # Example code to generate terminal logs
    print("This is standard output.")
    raise Exception("This is an error message.")

    # Close the log file at the end
    log_file.close()