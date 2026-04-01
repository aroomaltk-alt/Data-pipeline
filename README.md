# Weather Data Pipeline using Kafka, PySpark, and PostgreSQL

## Project Overview
This project builds a real-time data pipeline that collects live weather data from OpenWeather API, streams the data using Apache Kafka, processes the data using PySpark Streaming, and stores the processed data into PostgreSQL.

## Architecture
OpenWeather API → Python Producer → Apache Kafka → PySpark Streaming → PostgreSQL

## Technologies Used
- Python
- Apache Kafka
- PySpark
- PostgreSQL
- OpenWeather API

## Project Structure
producer → Kafka Producer (Weather API data)
consumer → PySpark Streaming Consumer
database → PostgreSQL Writer
config → Configuration file

## How to Run

### Start Kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

### Run Kafka Producer
python kafka_weather_producer.py

### Run Spark Streaming Consumer
spark-submit spark_streaming_consumer.py

### PostgreSQL
Data will be stored in PostgreSQL table.
