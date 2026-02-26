import requests
import json

API_KEY = "f0841753cabc35b8ecca13ee835435d1"

headers = {
    "x-apisports-key": API_KEY
}

# Dernier match Real Madrid
response = requests.get(
    "https://v3.football.api-sports.io/fixtures",
    headers=headers,
    params={
        "team": 541,
        "last": 1
    }
)

data = response.json()

if data["results"] > 0:
    match = data["response"][0]
    fixture_id = match["fixture"]["id"]
    home = match["teams"]["home"]["name"]
    away = match["teams"]["away"]["name"]
    score = f"{match['goals']['home']} - {match['goals']['away']}"
    print(f"Match: {home} vs {away} → {score}")
    print(f"Fixture ID: {fixture_id}")

    # On récupère les événements du match (buts, cartons)
    events = requests.get(
        "https://v3.football.api-sports.io/fixtures/events",
        headers=headers,
        params={"fixture": fixture_id}
    ).json()

    print("\n⚽ BUTS:")
    for event in events["response"]:
        if event["type"] == "Goal":
            joueur = event["player"]["name"]
            minute = event["time"]["elapsed"]
            equipe = event["team"]["name"]
            print(f"  {minute}' - {joueur} ({equipe})")