import requests
import sqlite3
import time
from datetime import datetime

API_KEY = "f0841753cabc35b8ecca13ee835435d1"
headers = {"x-apisports-key": API_KEY}

SAISON = 2025

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

def api_get(endpoint, params={}):
    response = requests.get(
        f"https://v3.football.api-sports.io/{endpoint}",
        headers=headers,
        params=params
    )
    time.sleep(0.5)
    return response.json()

def init_table():
    conn = sqlite3.connect("botfoot.db")
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS joueurs_forme (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        joueur_id INTEGER,
        fixture_id INTEGER,
        equipe_id INTEGER,
        ligue_id INTEGER,
        date TEXT,
        buts INTEGER DEFAULT 0,
        passes INTEGER DEFAULT 0,
        note REAL DEFAULT 0,
        minutes INTEGER DEFAULT 0,
        titulaire INTEGER DEFAULT 0,
        UNIQUE(joueur_id, fixture_id)
    )''')
    conn.commit()
    conn.close()
    print("✅ Table joueurs_forme créée")

def get_derniers_fixtures(ligue_id, nb=5):
    """Récupère les IDs des 5 dernières journées d'une ligue"""
    data = api_get("fixtures", {
        "league": ligue_id,
        "season": SAISON,
        "last": nb * 12,  # ~12 matchs par journée
        "status": "FT"
    })
    
    fixtures = []
    journees_vues = set()
    
    for match in data.get("response", []):
        journee = match["league"]["round"]
        if journee not in journees_vues:
            journees_vues.add(journee)
        if len(journees_vues) > nb:
            break
        fixtures.append(match["fixture"]["id"])
    
    return fixtures[:nb * 12]

def get_stats_fixture(fixture_id, ligue_id):
    """Récupère les stats de tous les joueurs pour un match"""
    data = api_get("fixtures/players", {"fixture": fixture_id})
    
    conn = sqlite3.connect("botfoot.db")
    c = conn.cursor()
    total = 0
    
    for team_data in data.get("response", []):
        equipe_id = team_data["team"]["id"]
        
        for player_data in team_data.get("players", []):
            joueur_id = player_data["player"]["id"]
            stats = player_data["statistics"][0] if player_data["statistics"] else None
            
            if not stats:
                continue
            
            # Vérifier que le joueur est dans notre BDD
            c.execute("SELECT id FROM api_joueurs WHERE id = ?", (joueur_id,))
            if not c.fetchone():
                continue
            
            buts = stats["goals"].get("total") or 0
            passes = stats["goals"].get("assists") or 0
            note = float(stats["games"].get("rating") or 0)
            minutes = stats["games"].get("minutes") or 0
            titulaire = 1 if stats["games"].get("captain") or minutes >= 45 else 0
            
            try:
                c.execute('''INSERT OR IGNORE INTO joueurs_forme
                    (joueur_id, fixture_id, equipe_id, ligue_id, date, buts, passes, note, minutes, titulaire)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (joueur_id, fixture_id, equipe_id, ligue_id,
                     datetime.now().strftime("%Y-%m-%d"), buts, passes, note, minutes, titulaire))
                total += 1
            except:
                pass
    
    conn.commit()
    conn.close()
    return total

def bootstrap_forme():
    init_table()
    total_requetes = 0
    total_joueurs = 0
    
    for nom_ligue, ligue_id in LIGUES_CIBLES.items():
        print(f"\n📋 {nom_ligue}...")
        
        # Récupérer les 5 derniers fixtures
        fixtures = get_derniers_fixtures(ligue_id)
        total_requetes += 1
        print(f"   {len(fixtures)} matchs trouvés")
        
        # Pour chaque fixture récupérer les stats joueurs
        for fixture_id in fixtures:
            nb = get_stats_fixture(fixture_id, ligue_id)
            total_joueurs += nb
            total_requetes += 1
            print(f"   Fixture {fixture_id}: {nb} joueurs")
        
        print(f"   ✅ {nom_ligue} terminé — {total_requetes} requêtes utilisées")
    
    print(f"\n🏆 Bootstrap forme terminé !")
    print(f"   Total requêtes : {total_requetes}")
    print(f"   Total entrées : {total_joueurs}")

bootstrap_forme()