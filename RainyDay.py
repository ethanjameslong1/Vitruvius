import requests
import time

layout = "2006/01/02 15:04:05"
apiKey = "f6861b00eb45a377be4a7bb4bdcc9870"
lat = 41.85
lon = -87.65
timestamp = int(time.time()) - (5 * 24 * 60 * 60)
day = "2026-02-11"

url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=rain&start_date={day}&end_date={day}"

r = requests.get(url)
print(r.json())
