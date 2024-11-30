from pyspark.sql import SparkSession
logfile=open("read-log.txt","w")
sys.stdout=logfile
sys.stderr=logfile
spark = SparkSession.builder \ 
        .appName("readCSV") \ 
        .getOrCreate()

df = spark.read \
        .option("header", True) \
        .option("truncate", False) \
        .option("numRows", 10) \
        .option("maxColumnWidth", 100) \
        .csv("hdfs://node1:9000/environment_data_2/")
df.show()
df.printSchema()

spark.stop()
