# Weather Data Pipeline using Kafka, PySpark, and PostgreSQL

## Project Overview
This project builds a real-time data pipeline that collects live weather data from OpenWeather API, streams the data using Apache Kafka, processes the data using PySpark Streaming, and stores the processed data into PostgreSQL.

## Architecture
OpenWeather API → Python Producer → Apache Kafka → PySpark Streaming → PostgreSQL

## Technologies Used
- OpenWeather API
- Python
- Apache Kafka
- PySpark
- PostgreSQL


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



## AWS Architecture

This project is deployed on AWS cloud using the following architecture:

OpenWeather API → AWS Lambda → Amazon EC2 (Apache Kafka) → AWS Glue (PySpark) → Amazon RDS (PostgreSQL)

### Service Explanation

- **AWS Lambda**: Collects weather data from OpenWeather API and sends data to Kafka.
- **Amazon EC2**: Hosts Apache Kafka broker for real-time data streaming.
- **AWS Glue (PySpark)**: Consumes data from Kafka, processes and transforms the data.
- **Amazon RDS (PostgreSQL)**: Stores the processed weather data.
- **CloudWatch**: Used for logging and monitoring.

### Data Flow

1. AWS Lambda fetches real-time weather data from OpenWeather API.
2. Lambda sends the data to Kafka running on EC2.
3. AWS Glue PySpark job reads data from Kafka topic.
4. PySpark processes and transforms the data.
5. Processed data is stored in Amazon RDS (PostgreSQL).
