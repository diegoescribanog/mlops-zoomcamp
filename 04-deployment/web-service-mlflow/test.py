import requests

ride = {
    "PULocationID": 60,
    "DOLocationID": 50,
    "trip_distance": 15
}

url = 'http://localhost:9696/predict'
response = requests.post(url, json=ride)
print(response.json())
