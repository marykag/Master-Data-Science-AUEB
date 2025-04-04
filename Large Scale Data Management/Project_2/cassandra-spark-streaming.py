from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, trim, lower, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import traceback
import time

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaToCassandra") \
    .config("spark.cassandra.connection.host", "172.18.0.3") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define Kafka Schema
rating_schema = StructType([
    StructField("user", StringType(), True),
    StructField("movie", StringType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

# Read the Kafka stream
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "test") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Deserialize the Kafka stream and parse the JSON data
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), rating_schema).alias("data")) \
    .select("data.*")

# Convert timestamp
parsed_df = parsed_df.withColumn("rating_timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))

# Rename columns
parsed_df = parsed_df.withColumnRenamed("user", "user_name") \
                     .withColumnRenamed("movie", "movie_name")

# Normalize `movie_name` to avoid mismatches due to spacing or case differences
parsed_df = parsed_df.withColumn("movie_name", trim(lower(col("movie_name"))))

# Define Schema for Netflix Movie Metadata
netflix_schema = StructType([
    StructField("show_id", StringType(), True),
    StructField("title", StringType(), False),
    StructField("director", StringType(), True),
    StructField("country", StringType(), True),
    StructField("release_year", IntegerType(), True),  # Make sure it's Integer
    StructField("rating", StringType(), True),  # Categorical rating
    StructField("duration", StringType(), True)  # Will be converted to Integer
])

# Load Netflix Movie Metadata (Static Data)
movies_df = spark.read.schema(netflix_schema) \
    .option("header", True) \
    .csv("/vagrant/data/netflix.csv")

# Normalize movie title for better join accuracy
movies_df = movies_df.withColumn("movie_name", trim(lower(col("title")))) \
                     .withColumnRenamed("rating", "rating_category")  # Rename to avoid conflict

# Convert duration to Integer (Extract first part of "90 min")
movies_df = movies_df.withColumn("duration", col("duration").substr(1, 3).cast(IntegerType()))

# Perform the join with movie metadata
enriched_df = parsed_df.join(movies_df, "movie_name", "left") \
    .select(
        parsed_df.user_name, parsed_df.movie_name, parsed_df.rating, parsed_df.rating_timestamp,
        movies_df.show_id, movies_df.director, movies_df.country,
        movies_df.release_year, movies_df.rating_category, movies_df.duration
    )

# Filter out rows where user_name is null before writing to Cassandra
enriched_df = enriched_df.filter(col("user_name").isNotNull())

# Function to Write to Cassandra
def writeToCassandra(writeDF, _):
    writeDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="ratings", keyspace="netflix") \
        .save()

# ✅ Stream Processing with Configurable Interval (30 seconds by default)
result = None
while result is None:
    try:
        # ✅ Configurable interval for processing data every 30 seconds
        result = enriched_df.writeStream \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .foreachBatch(writeToCassandra) \
            .outputMode("append") \
            .trigger(processingTime='30 seconds') \
            .start() 
        result.awaitTermination()


    except Exception as e:
        print("Error:", e)
        traceback.print_exc()
        time.sleep(5)  # Wait before retrying
