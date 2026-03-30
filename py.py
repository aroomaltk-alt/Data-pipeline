# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json, current_timestamp
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# # -----------------------------
# # SparkSession with Kafka package
# # -----------------------------
# spark = SparkSession.builder \
#     .appName("WeatherKafkaProcessing") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.7") \
#     .getOrCreate()

# # -----------------------------
# # Read streaming data from Kafka
# # -----------------------------
# kafka_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "weather_topic") \
#     .option("startingOffsets", "latest") \
#     .load()

# # Convert binary 'value' to string
# json_df = kafka_df.selectExpr("CAST(value AS STRING)")

# # -----------------------------
# # Define JSON schema
# # -----------------------------
# schema = StructType([
#     StructField("name", StringType(), True),
#     StructField("main", StructType([
#         StructField("temp", DoubleType(), True),
#         StructField("humidity", DoubleType(), True)
#     ]))
# ])

# # -----------------------------
# # Parse JSON and extract fields
# # -----------------------------
# weather_df = json_df.select(from_json(col("value"), schema).alias("data"))

# result = weather_df.select(
#     col("data.name").alias("city"),
#     col("data.main.temp").alias("temperature"),
#     col("data.main.humidity").alias("humidity"),
#     current_timestamp().alias("processed_at")
# )

# # -----------------------------
# # Write stream to console with checkpoint
# # -----------------------------
# query = result.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", False) \
#     .option("checkpointLocation", "/tmp/spark_checkpoints/weather") \
#     .start()

# query.awaitTermination()

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json
# from pyspark.sql.types import StructType, StringType, IntegerType

# # 1. Initialize Spark Session with Kafka dependency
# spark = SparkSession.builder \
#     .appName("KafkaLocalConsumer") \
#     .master("local[*]") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
#     .getOrCreate()

# # Reduce log noise for clarity
# spark.sparkContext.setLogLevel("WARN")

# # 2. Define the schema of the JSON data coming from Kafka
# #    This should match the structure of the messages in your topic.
# #    Example: {"name": "Alice", "age": 30, "city": "New York"}
# json_schema = StructType() \
#     .add("name", StringType()) \
#     .add("age", IntegerType()) \
#     .add("city", StringType())

# # 3. Read the streaming data from Kafka
# df_raw = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "weather_topic") \
#     .option("startingOffsets", "latest") \
#     .load()

# # 4. Cast the binary "value" column to string and parse the JSON
# df_parsed = df_raw \
#     .selectExpr("CAST(value AS STRING) as json_string") \
#     .select(from_json(col("json_string"), json_schema).alias("data")) \
#     .select("data.*")

# # 5. Write the final stream to the console (for debugging)
# query = df_parsed.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .trigger(processingTime="5 seconds") \
#     .start()

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json
# from pyspark.sql.types import (
#     StructType, StructField, StringType, IntegerType, 
#     DoubleType, LongType, ArrayType
# )

# spark = SparkSession.builder \
#     .appName("KafkaNewsStreaming") \
#     .master("local[*]") \
#     .config("spark.jars", "postgresql-42.7.3.jar") \
#     .config(
#         "spark.jars.packages",
#         "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
#     ) \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # --- Schema matching your actual OpenWeatherMap JSON ---

# coord_schema = StructType([
#     StructField("lon", DoubleType()),
#     StructField("lat", DoubleType())
# ])

# weather_item_schema = StructType([
#     StructField("id", IntegerType()),
#     StructField("main", StringType()),
#     StructField("description", StringType()),
#     StructField("icon", StringType())
# ])

# main_schema = StructType([
#     StructField("temp", DoubleType()),
#     StructField("feels_like", DoubleType()),
#     StructField("temp_min", DoubleType()),
#     StructField("temp_max", DoubleType()),
#     StructField("pressure", IntegerType()),
#     StructField("humidity", IntegerType()),
#     StructField("sea_level", IntegerType()),
#     StructField("grnd_level", IntegerType())
# ])

# wind_schema = StructType([
#     StructField("speed", DoubleType()),
#     StructField("deg", IntegerType()),
#     StructField("gust", DoubleType())
# ])

# clouds_schema = StructType([
#     StructField("all", IntegerType())
# ])

# sys_schema = StructType([
#     StructField("country", StringType()),
#     StructField("sunrise", LongType()),
#     StructField("sunset", LongType())
# ])

# json_schema = StructType([
#     StructField("coord", coord_schema),
#     StructField("weather", ArrayType(weather_item_schema)),
#     StructField("base", StringType()),
#     StructField("main", main_schema),
#     StructField("visibility", IntegerType()),
#     StructField("wind", wind_schema),
#     StructField("clouds", clouds_schema),
#     StructField("dt", LongType()),
#     StructField("sys", sys_schema),
#     StructField("timezone", IntegerType()),
#     StructField("id", LongType()),
#     StructField("name", StringType()),
#     StructField("cod", IntegerType())
# ])

# # --- Read from Kafka ---
# df_raw = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "weather_topic") \
#     .option("startingOffsets", "latest") \
#     .load()

# # --- Parse JSON ---
# df_parsed = df_raw \
#     .selectExpr("CAST(value AS STRING) as json_string") \
#     .select(from_json(col("json_string"), json_schema).alias("data")) \
#     .select("data.*")

# # --- Flatten into clean columns for Spark processing ---
# df_flat = df_parsed.select(
#     col("id").alias("city_id"),
#     col("name").alias("city_name"),
#     col("dt").alias("timestamp"),
#     col("base"),
#     col("visibility"),
#     col("timezone"),
#     col("cod"),

#     col("coord.lat").alias("lat"),
#     col("coord.lon").alias("lon"),

#     # weather is an array — grab first element
#     col("weather")[0]["main"].alias("weather_main"),
#     col("weather")[0]["description"].alias("weather_description"),
#     col("weather")[0]["icon"].alias("weather_icon"),

#     col("main.temp").alias("temp"),
#     col("main.feels_like").alias("feels_like"),
#     col("main.temp_min").alias("temp_min"),
#     col("main.temp_max").alias("temp_max"),
#     col("main.pressure").alias("pressure"),
#     col("main.humidity").alias("humidity"),
#     col("main.sea_level").alias("sea_level"),
#     col("main.grnd_level").alias("grnd_level"),

#     col("wind.speed").alias("wind_speed"),
#     col("wind.deg").alias("wind_deg"),
#     col("wind.gust").alias("wind_gust"),

#     col("clouds.all").alias("cloud_coverage"),

#     col("sys.country").alias("country"),
#     col("sys.sunrise").alias("sunrise"),
#     col("sys.sunset").alias("sunset")
# )

# # --- Output to console ---
# # query = df_flat.writeStream \
# #     .outputMode("append") \
# #     .format("console") \
# #     .option("truncate", "false") \
# #     .trigger(processingTime="5 seconds") \
# #     .start()

# # query.awaitTermination()
# def write_to_postgres(batch_df, batch_id):
#     batch_df.write \
#         .format("jdbc") \
#         .option("url", "jdbc:postgresql://localhost:5432/weatherdb") \
#         .option("dbtable", "weather_topic") \
#         .option("user", "postgres") \
#         .option("password", "postgres") \
#         .option("driver", "org.postgresql.Driver") \
#         .mode("append") \
#         .save()

# query = df_flat.writeStream \
#     .foreachBatch(write_to_postgres) \
#     .outputMode("append") \
#     .start()

# 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType, LongType, ArrayType
)

# -----------------------------
# Spark Session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("KafkaWeatherToPostgres")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    )
    .config(
        "spark.jars",
        "/home/aromal/kafka/kafka_2.13-3.8.1/postgresql-42.7.3.jar"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Weather JSON Schema
# -----------------------------
json_schema = StructType([
    StructField("coord", StructType([
        StructField("lon", DoubleType()),
        StructField("lat", DoubleType())
    ])),
    StructField("weather", ArrayType(StructType([
        StructField("id", IntegerType()),
        StructField("main", StringType()),
        StructField("description", StringType()),
        StructField("icon", StringType())
    ]))),
    StructField("main", StructType([
        StructField("temp", DoubleType()),
        StructField("humidity", IntegerType())
    ])),
    StructField("dt", LongType()),
    StructField("name", StringType())
])

# -----------------------------
# Read Kafka Stream
# -----------------------------
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "weather_topic")
    .option("startingOffsets", "earliest")
    .load()
)

# -----------------------------
# Parse JSON
# -----------------------------
df_parsed = (
    df_raw
    .selectExpr("CAST(value AS STRING) as json_string")
    .select(from_json(col("json_string"), json_schema).alias("data"))
    .select("data.*")
)

# -----------------------------
# MATCH POSTGRES SCHEMA
# -----------------------------
df_final = df_parsed.select(
    col("name").alias("city"),
    col("main.temp").alias("temperature"),
    col("main.humidity").alias("humidity"),
    from_unixtime(col("dt")).cast("timestamp").alias("processed_at")
)

df_final.printSchema()

# -----------------------------
# Write to PostgreSQL
# -----------------------------
def write_to_postgres(batch_df, batch_id):
    df_clean = batch_df.filter(
        col("city").isNotNull() & col("temperature").isNotNull()
    )
    if df_clean.rdd.isEmpty():
        print(f"Batch {batch_id}: empty")
        return
    print(f"Batch {batch_id}: writing {df_clean.count()} rows to Postgres")
    df_clean.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/weather_db") \
        .option("dbtable", "weather_data") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    print(f"Batch {batch_id}: done.")
    

# -----------------------------
# Streaming Query
# -----------------------------
query = (
    df_final.writeStream
    .outputMode("append")
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "/tmp/weather_checkpoint2")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()