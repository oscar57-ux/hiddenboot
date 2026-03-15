import os
import requests
import sqlite3
import time
from datetime import datetime

API_KEY = os.environ.get("API_SPORTS_KEY", "")
headers = {"x-apisports-key": API_KEY}

SAISON = 2025

_SAISON_OVERRIDES = {71: 2025, 72: 2025, 128: 2025, 131: 2025, 239: 2025, 265: 2025, 268: 2025}

def _saison(ligue_id: int) -> int:
    return _SAISON_OVERRIDES.get(ligue_id, SAISON)


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
    print("✅ Table joueurs_forme prête")

# ── Fix 1 : retourne des tuples (fixture_id, fixture_date) ───────────────────

def get_derniers_fixtures(ligue_id, nb=5):
    """Récupère les fixtures des nb dernières journées d'une ligue.
    Retourne une liste de (fixture_id, fixture_date) triés du plus récent.
    """
    data = api_get("fixtures", {
        "league": ligue_id,
        "season": _saison(ligue_id),
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
        fixture_id   = match["fixture"]["id"]
        # Date réelle du match (ex: "2025-03-14T21:00:00+00:00") → "2025-03-14"
        fixture_date = (match["fixture"].get("date") or "")[:10] or datetime.now().strftime("%Y-%m-%d")
        fixtures.append((fixture_id, fixture_date))

    return fixtures[:nb * 12]


def get_stats_fixture(fixture_id, fixture_date, ligue_id):
    """Récupère les stats de tous les joueurs connus pour un match."""
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

            buts     = stats["goals"].get("total") or 0
            passes   = stats["goals"].get("assists") or 0
            note     = float(stats["games"].get("rating") or 0)
            minutes  = stats["games"].get("minutes") or 0
            titulaire = 1 if stats["games"].get("captain") or minutes >= 45 else 0

            try:
                # Fix : stocker la date réelle du match, pas aujourd'hui
                c.execute('''INSERT OR IGNORE INTO joueurs_forme
                    (joueur_id, fixture_id, equipe_id, ligue_id, date, buts, passes, note, minutes, titulaire)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (joueur_id, fixture_id, equipe_id, ligue_id,
                     fixture_date, buts, passes, note, minutes, titulaire))
                total += 1
            except Exception:
                pass

    conn.commit()
    conn.close()
    return total


# ── Fix 2 : recalcul série de victoires via /fixtures?team={id}&last=10 ───────

def _get_forme_equipe(equipe_id, ligue_id):
    """Retourne une chaîne de forme W/D/L (10 derniers matchs, du plus ancien au plus récent)."""
    data = api_get("fixtures", {
        "team":   equipe_id,
        "season": _saison(ligue_id),
        "last":   10,
        "status": "FT",
    })
    matchs = data.get("response", [])
    # Trier du plus ancien au plus récent
    matchs = sorted(matchs, key=lambda m: m["fixture"]["date"])
    result = []
    for match in matchs:
        home_id      = match["teams"]["home"]["id"]
        home_winner  = match["teams"]["home"]["winner"]
        away_winner  = match["teams"]["away"]["winner"]
        if equipe_id == home_id:
            if home_winner:   result.append("W")
            elif away_winner: result.append("L")
            else:             result.append("D")
        else:
            if away_winner:   result.append("W")
            elif home_winner: result.append("L")
            else:             result.append("D")
    return "".join(result)


def bootstrap_equipes_serie():
    """Met à jour classements.forme avec les 10 derniers résultats réels par équipe."""
    conn = sqlite3.connect("botfoot.db")
    c = conn.cursor()

    # Récupérer toutes les équipes qui ont un classement
    c.execute("""
        SELECT ae.id AS equipe_id, cl.ligue_id
        FROM api_equipes ae
        JOIN classements cl ON cl.equipe_id = ae.id
    """)
    equipes = c.fetchall()
    print(f"[serie] Mise à jour forme pour {len(equipes)} équipes...")

    updated = 0
    for eq in equipes:
        equipe_id = eq[0]
        ligue_id  = eq[1]
        forme = _get_forme_equipe(equipe_id, ligue_id)
        if forme:
            c.execute(
                "UPDATE classements SET forme = ? WHERE equipe_id = ? AND ligue_id = ?",
                (forme, equipe_id, ligue_id)
            )
            updated += 1

    conn.commit()
    conn.close()
    print(f"[serie] {updated} équipes mises à jour")


# ── Fonction principale ────────────────────────────────────────────────────────

def bootstrap_forme():
    init_table()

    # Fix 1 : vider la table pour repartir d'un état propre à chaque run
    conn = sqlite3.connect("botfoot.db")
    c = conn.cursor()
    c.execute("DELETE FROM joueurs_forme")
    conn.commit()
    conn.close()
    print("[bootstrap_forme] Table joueurs_forme vidée — recalcul propre")

    total_requetes = 0
    total_joueurs  = 0

    for nom_ligue, ligue_id in LIGUES_CIBLES.items():
        print(f"\n📋 {nom_ligue}...")

        fixtures = get_derniers_fixtures(ligue_id)
        total_requetes += 1
        print(f"   {len(fixtures)} matchs trouvés")

        for fixture_id, fixture_date in fixtures:
            nb = get_stats_fixture(fixture_id, fixture_date, ligue_id)
            total_joueurs  += nb
            total_requetes += 1
            print(f"   Fixture {fixture_id} ({fixture_date}): {nb} joueurs")

        print(f"   ✅ {nom_ligue} terminé — {total_requetes} requêtes utilisées")

    # Fix 2 : recalcul séries de victoires depuis l'API
    print("\n🔄 Recalcul séries de victoires équipes...")
    bootstrap_equipes_serie()

    print(f"\n🏆 Bootstrap forme terminé !")
    print(f"   Total requêtes : {total_requetes}")
    print(f"   Total entrées  : {total_joueurs}")


bootstrap_forme()
