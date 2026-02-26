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

def init_bdd():
    conn = sqlite3.connect("botfoot.db")
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS api_ligues (
        id INTEGER PRIMARY KEY,
        nom TEXT,
        pays TEXT,
        saison INTEGER
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS api_equipes (
        id INTEGER PRIMARY KEY,
        nom TEXT,
        ligue_id INTEGER,
        pays TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS api_joueurs (
        id INTEGER PRIMARY KEY,
        nom TEXT,
        age INTEGER,
        nationalite TEXT,
        poste TEXT,
        equipe_id INTEGER,
        ligue_id INTEGER,
        matchs INTEGER,
        buts INTEGER,
        passes INTEGER,
        note REAL,
        minutes INTEGER,
        ratio REAL,
        score REAL,
        saison INTEGER,
        date_maj TEXT
    )''')

    conn.commit()
    conn.close()
    print("✅ BDD initialisée")

LIGUES_CIBLES = {
    "Ligue 1": 61,
    "Ligue 2": 62,
    "Premier League": 39,
    "Championship": 40,
    "La Liga": 140,
    "La Liga 2": 141,
    "Serie A": 135,
    "Serie B": 136,
    "Bundesliga": 78,
    "Bundesliga 2": 79,
    "Primeira Liga": 94,
    "Segunda Liga": 95,
    "Eredivisie": 88,
    "Eerste Divisie": 89,
    "Pro League BE": 144,
    "Süper Lig": 203,
    "Premier League RU": 235,
    "Ekstraklasa": 106,
    "Super Liga SR": 286,
    "Liga 1 RO": 283,
    "Super League GR": 197,
    "HNL": 210,
    "Bundesliga AT": 218,
    "Super League CH": 207,
    "Premiership SC": 179,
    "Brasileirao": 71,
    "Serie B BR": 72,
    "Liga Profesional": 128,
    "Primera Nacional": 131,
    "Liga BetPlay": 239,
    "Primera Division CL": 265,
    "Primera Division UY": 268,
}

SAISON = 2025

def bootstrap_ligues():
    conn = sqlite3.connect("botfoot.db")
    c = conn.cursor()
    for nom, ligue_id in LIGUES_CIBLES.items():
        c.execute("INSERT OR REPLACE INTO api_ligues (id, nom, pays, saison) VALUES (?, ?, ?, ?)",
                  (ligue_id, nom, "", SAISON))
    conn.commit()
    conn.close()
    print(f"✅ {len(LIGUES_CIBLES)} ligues insérées")

def bootstrap_equipes():
    conn = sqlite3.connect("botfoot.db")
    c = conn.cursor()
    total = 0

    for nom_ligue, ligue_id in LIGUES_CIBLES.items():
        print(f"  Équipes {nom_ligue}...")
        data = api_get("teams", {"league": ligue_id, "season": SAISON})
        for team in data.get("response", []):
            c.execute("INSERT OR REPLACE INTO api_equipes (id, nom, ligue_id, pays) VALUES (?, ?, ?, ?)",
                      (team["team"]["id"], team["team"]["name"], ligue_id, team["team"]["country"]))
            total += 1
        conn.commit()

    conn.close()
    print(f"✅ {total} équipes insérées")

def bootstrap_joueurs():
    conn = sqlite3.connect("botfoot.db")
    c = conn.cursor()
    total = 0
    postes_cibles = ["Attacker", "Midfielder"]

    for nom_ligue, ligue_id in LIGUES_CIBLES.items():
        print(f"  Joueurs {nom_ligue}...")
        page = 1

        while True:
            data = api_get("players", {
                "league": ligue_id,
                "season": SAISON,
                "page": page
            })

            if not data.get("response"):
                break

            for item in data["response"]:
                joueur = item["player"]
                stats = item["statistics"][0] if item["statistics"] else None
                if not stats:
                    continue

                poste = stats.get("games", {}).get("position", "")
                if poste not in postes_cibles:
                    continue

                matchs = stats["games"].get("appearences") or 0
                buts = stats["goals"].get("total") or 0
                passes = stats["goals"].get("assists") or 0
                note = float(stats["games"].get("rating") or 0)
                minutes = stats["games"].get("minutes") or 0
                equipe_id = stats["team"]["id"]
                ratio = round(buts / matchs, 2) if matchs > 0 else 0
                score = round((buts * 3) + (ratio * 10) + note, 2)

                c.execute('''INSERT OR REPLACE INTO api_joueurs
                    (id, nom, age, nationalite, poste, equipe_id, ligue_id,
                     matchs, buts, passes, note, minutes, ratio, score, saison, date_maj)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (joueur["id"], joueur["name"], joueur.get("age"),
                     joueur.get("nationality"), poste, equipe_id, ligue_id,
                     matchs, buts, passes, note, minutes, ratio, score,
                     SAISON, datetime.now().strftime("%Y-%m-%d %H:%M")))
                total += 1

            total_pages = data.get("paging", {}).get("total", 1)
            if page >= total_pages:
                break
            page += 1
            conn.commit()

    conn.close()
    print(f"✅ {total} joueurs offensifs insérés")

# LANCEMENT
print("🚀 Démarrage du bootstrap...")
print("="*50)

init_bdd()

print("\n📋 Étape 1/3 - Ligues...")
bootstrap_ligues()

print("\n👥 Étape 2/3 - Équipes...")
bootstrap_equipes()

print("\n⚽ Étape 3/3 - Joueurs...")
bootstrap_joueurs()

print("\n🏆 Bootstrap terminé !")

conn = sqlite3.connect("botfoot.db")
c = conn.cursor()
c.execute("SELECT COUNT(*) FROM api_ligues")
nb_ligues = c.fetchone()[0]
c.execute("SELECT COUNT(*) FROM api_equipes")
nb_equipes = c.fetchone()[0]
c.execute("SELECT COUNT(*) FROM api_joueurs")
nb_joueurs = c.fetchone()[0]
conn.close()

print(f"\n📊 Résumé BDD saison 2025/2026 :")
print(f"   {nb_ligues} ligues")
print(f"   {nb_equipes} équipes")
print(f"   {nb_joueurs} joueurs offensifs")