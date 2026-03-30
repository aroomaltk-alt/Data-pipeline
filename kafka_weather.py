from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaWeatherGlueJob") \
    .getOrCreate()

# Read from Kafka
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "43.205.126.78:9092") \
    .option("subscribe", "weather_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert Kafka value (binary → string)
json_df = df.selectExpr("CAST(value AS STRING) as json_data")

# Show data (for testing)
json_df.show(truncate=False)