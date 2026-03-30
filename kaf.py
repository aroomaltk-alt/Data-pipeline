from kafka import KafkaProducer
import json
import requests

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",  # ← lowercase, not a variable name
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

api_key = "2aa2518ff4c78ac0a4d6fb0309fc9f41"
city = "Tokyo"

url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
response = requests.get(url)
data = response.json()

producer.send("weather_topic", data)
producer.flush()
print("data done")