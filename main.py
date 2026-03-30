import requests
api_key = "2aa2518ff4c78ac0a4d6fb0309fc9f41"
city =  "Tokyo"
url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
response = requests.get(url)
data = response.json()
print(data)