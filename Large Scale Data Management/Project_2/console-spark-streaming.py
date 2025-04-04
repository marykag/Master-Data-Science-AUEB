from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json, col

spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# schema for movie ratings
rating_schema = StructType([
    StructField("user", StringType(), True),
    StructField("movie", StringType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:29092") \
  .option("subscribe", "test") \
  .option("startingOffsets","earliest") \
  .load() 

sdf = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), rating_schema).alias("data")).select("data.*")

sdf.writeStream \
  .outputMode("append") \
  .format("console") \
  .option("truncate", False) \
  .start() \
  .awaitTermination()
