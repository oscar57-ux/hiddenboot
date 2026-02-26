import requests
import sqlite3
import time
from datetime import datetime

API_KEY = "f0841753cabc35b8ecca13ee835435d1"
headers = {"x-apisports-key": API_KEY}

def api_get(endpoint, params={}):
    response = requests.get(
        f"https://v3.football.api-sports.io/{endpoint}",
        headers=headers,
        params=params
    )
    time.sleep(0.5)
    return response.json()

LIGUES_CIBLES = {
    "Ligue 1": 61, "Ligue 2": 62,
    "Premier League": 39, "Championship": 40,
    "La Liga": 140, "La Liga 2": 141,
    "Serie A": 135, "Serie B": 136,
    "Bundesliga": 78, "Bundesliga 2": 79,
    "Primeira Liga": 94, "Segunda Liga": 95,
    "Eredivisie": 88, "Eerste Divisie": 89,
    "Pro League BE": 144, "Süper Lig": 203,
    "Premier League RU": 235, "Ekstraklasa": 106,
    "Super Liga SR": 286, "Liga 1 RO": 283,
    "Super League GR": 197, "HNL": 210,
    "Bundesliga AT": 218, "Super League CH": 207,
    "Premiership SC": 179, "Brasileirao": 71,
    "Serie B BR": 72, "Liga Profesional": 128,
    "Primera Nacional": 131, "Liga BetPlay": 239,
    "Primera Division CL": 265, "Primera Division UY": 268,
}

SAISON = 2025

conn = sqlite3.connect("botfoot.db")
c = conn.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS classements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    equipe_id INTEGER,
    ligue_id INTEGER,
    rang INTEGER,
    points INTEGER,
    victoires INTEGER,
    nuls INTEGER,
    defaites INTEGER,
    buts_pour INTEGER,
    buts_contre INTEGER,
    diff_buts INTEGER,
    forme TEXT,
    date_maj TEXT
)''')

c.execute("DELETE FROM classements")
conn.commit()

total = 0
for nom_ligue, ligue_id in LIGUES_CIBLES.items():
    print(f"  Classement {nom_ligue}...")
    data = api_get("standings", {"league": ligue_id, "season": SAISON})

    try:
        standings = data["response"][0]["league"]["standings"][0]
        for team in standings:
            forme = team.get("form", "")
            c.execute('''INSERT INTO classements
                (equipe_id, ligue_id, rang, points, victoires, nuls, defaites,
                 buts_pour, buts_contre, diff_buts, forme, date_maj)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (team["team"]["id"], ligue_id, team["rank"], team["points"],
                 team["all"]["win"], team["all"]["draw"], team["all"]["lose"],
                 team["all"]["goals"]["for"], team["all"]["goals"]["against"],
                 team["goalsDiff"], forme,
                 datetime.now().strftime("%Y-%m-%d %H:%M")))
            total += 1
    except Exception as e:
        print(f"  Erreur {nom_ligue}: {e}")

    conn.commit()

conn.close()
print(f"\n✅ {total} classements insérés en BDD !")