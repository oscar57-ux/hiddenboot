import os
import requests
import sqlite3
from datetime import date

API_KEY = os.environ.get("API_SPORTS_KEY", "")
headers = {"x-apisports-key": API_KEY}

response = requests.get(
    "https://v3.football.api-sports.io/fixtures",
    headers=headers,
    params={"date": date.today().strftime("%Y-%m-%d"), "timezone": "Europe/Paris"}
)

data = response.json()

for match in data["response"]:
    if "Santos" in match["teams"]["home"]["name"] or "Vasco" in match["teams"]["home"]["name"]:
        print(f"Home: {match['teams']['home']['name']} ID:{match['teams']['home']['id']}")
        print(f"Away: {match['teams']['away']['name']} ID:{match['teams']['away']['id']}")