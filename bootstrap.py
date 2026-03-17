import os
import requests
import sqlite3
import time
from datetime import datetime, date, timedelta

from database import get_conn, _is_pg, _ph, init_all_tables

API_KEY = os.environ.get("API_SPORTS_KEY", "")
headers = {"x-apisports-key": API_KEY}

def api_get(endpoint, params={}):
    response = requests.get(
        f"https://v3.football.api-sports.io/{endpoint}",
        headers=headers,
        params=params
    )
    time.sleep(0.15)
    return response.json()

def init_bdd():
    conn = get_conn()
    init_all_tables(conn)
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
    # UEFA
    "Champions League": 2,
    "Europa League": 3,
    "Conference League": 848,
}

SAISON = 2025

# Ligues sud-américaines dont la saison 2026 a démarré
_SAISON_OVERRIDES = {71: 2026, 72: 2026, 128: 2026, 131: 2026, 239: 2026, 242: 2026, 268: 2026}

def _saison(ligue_id: int) -> int:
    return _SAISON_OVERRIDES.get(ligue_id, SAISON)


def get_equipes_actives():
    """Retourne les team_ids des équipes qui ont joué hier ou aujourd'hui (dans nos ligues)."""
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    today_str  = date.today().strftime("%Y-%m-%d")

    ligue_ids_cibles = set(LIGUES_CIBLES.values())
    team_ids = set()

    for date_req in [yesterday, today_str]:
        data = api_get("fixtures", {"date": date_req, "timezone": "Europe/Paris"})
        for match in data.get("response", []):
            if match["league"]["id"] not in ligue_ids_cibles:
                continue
            team_ids.add(match["teams"]["home"]["id"])
            team_ids.add(match["teams"]["away"]["id"])

    print(f"[actifs] {len(team_ids)} équipes actives trouvées")
    return team_ids





def bootstrap_ligues():
    conn = get_conn()
    c    = conn.cursor()
    ph   = _ph(conn)
    for nom, ligue_id in LIGUES_CIBLES.items():
        c.execute(
            f"""INSERT INTO api_ligues (id, nom, pays, saison) VALUES ({ph},{ph},{ph},{ph})
                ON CONFLICT (id) DO UPDATE SET nom=EXCLUDED.nom, saison=EXCLUDED.saison""",
            (ligue_id, nom, "", SAISON),
        )
    conn.commit()
    conn.close()
    print(f"✅ {len(LIGUES_CIBLES)} ligues insérées")

def bootstrap_equipes():
    conn = get_conn()
    c    = conn.cursor()
    ph   = _ph(conn)
    total = 0

    for nom_ligue, ligue_id in LIGUES_CIBLES.items():
        print(f"  Équipes {nom_ligue}...")
        data = api_get("teams", {"league": ligue_id, "season": _saison(ligue_id)})
        for team in data.get("response", []):
            c.execute(
                f"""INSERT INTO api_equipes (id, nom, ligue_id, pays) VALUES ({ph},{ph},{ph},{ph})
                    ON CONFLICT (id) DO UPDATE SET
                        nom=EXCLUDED.nom, ligue_id=EXCLUDED.ligue_id, pays=EXCLUDED.pays""",
                (team["team"]["id"], team["team"]["name"], ligue_id, team["team"]["country"]),
            )
            total += 1
        conn.commit()

    conn.close()
    print(f"✅ {total} équipes insérées")

UEFA_LIGUE_IDS = {2, 3, 848}  # UCL / UEL / UECL — stats joueurs via ligue domestique


def bootstrap_joueurs():
    conn = get_conn()
    c    = conn.cursor()
    ph   = _ph(conn)
    total = 0
    postes_cibles = ["Attacker", "Midfielder"]

    for nom_ligue, ligue_id in LIGUES_CIBLES.items():
        if ligue_id in UEFA_LIGUE_IDS:
            print(f"  Joueurs {nom_ligue} → ignoré (UEFA, stats déjà dans la ligue domestique)")
            continue
        print(f"  Joueurs {nom_ligue}...")
        page = 1

        while True:
            data = api_get("players", {
                "league": ligue_id,
                "season": _saison(ligue_id),
                "page": page,
            })

            if not data.get("response"):
                break

            for item in data["response"]:
                joueur = item["player"]
                stats  = item["statistics"][0] if item["statistics"] else None
                if not stats:
                    continue

                poste = stats.get("games", {}).get("position", "")
                if poste not in postes_cibles:
                    continue

                matchs    = stats["games"].get("appearences") or 0
                buts      = stats["goals"].get("total")       or 0
                passes    = stats["goals"].get("assists")     or 0
                note      = float(stats["games"].get("rating") or 0)
                minutes   = stats["games"].get("minutes")     or 0
                equipe_id = stats["team"]["id"]
                ratio     = round(buts / matchs, 2) if matchs > 0 else 0
                score     = round((buts * 3) + (ratio * 10) + note, 2)

                c.execute(
                    f"""INSERT INTO api_joueurs
                        (id, nom, age, nationalite, poste, equipe_id, ligue_id,
                         matchs, buts, passes, note, minutes, ratio, score, saison, date_maj)
                        VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
                        ON CONFLICT (id) DO UPDATE SET
                            nom=EXCLUDED.nom, age=EXCLUDED.age,
                            nationalite=EXCLUDED.nationalite, poste=EXCLUDED.poste,
                            equipe_id=EXCLUDED.equipe_id, ligue_id=EXCLUDED.ligue_id,
                            matchs=EXCLUDED.matchs, buts=EXCLUDED.buts,
                            passes=EXCLUDED.passes, note=EXCLUDED.note,
                            minutes=EXCLUDED.minutes, ratio=EXCLUDED.ratio,
                            score=EXCLUDED.score, saison=EXCLUDED.saison,
                            date_maj=EXCLUDED.date_maj""",
                    (joueur["id"], joueur["name"], joueur.get("age"),
                     joueur.get("nationality"), poste, equipe_id, ligue_id,
                     matchs, buts, passes, note, minutes, ratio, score,
                     SAISON, datetime.now().strftime("%Y-%m-%d %H:%M")),
                )
                total += 1

            total_pages = data.get("paging", {}).get("total", 1)
            if page >= total_pages:
                break
            page += 1
            conn.commit()

    conn.close()
    print(f"✅ {total} joueurs offensifs insérés")

def bootstrap_joueurs_actifs():
    """Met à jour uniquement les joueurs des équipes actives (joué hier/aujourd'hui).
    Économie estimée : -50% de requêtes API vs bootstrap_joueurs() complet.
    """
    team_ids = get_equipes_actives()
    if not team_ids:
        print("⚠️ Aucune équipe active trouvée, skip mise à jour joueurs")
        return

    conn = get_conn()
    c    = conn.cursor()
    ph   = _ph(conn)
    total = 0
    postes_cibles = ["Attacker", "Midfielder"]

    for team_id in team_ids:
        c.execute(f"SELECT ligue_id FROM api_equipes WHERE id = {ph}", (team_id,))
        row = c.fetchone()
        if not row:
            continue
        ligue_id = row["ligue_id"]

        page = 1
        while True:
            data = api_get("players", {
                "team":   team_id,
                "season": _saison(ligue_id),
                "page":   page,
            })

            if not data.get("response"):
                break

            for item in data["response"]:
                joueur = item["player"]

                # Préférer la stat de ligue domestique (pas UCL/UEL/UECL)
                stats = None
                for s in item.get("statistics", []):
                    if s.get("league", {}).get("id") not in UEFA_LIGUE_IDS:
                        stats = s
                        break
                if stats is None:
                    stats = item["statistics"][0] if item["statistics"] else None
                if not stats:
                    continue

                poste = stats.get("games", {}).get("position", "")
                if poste not in postes_cibles:
                    continue

                matchs          = stats["games"].get("appearences") or 0
                buts            = stats["goals"].get("total")       or 0
                passes          = stats["goals"].get("assists")     or 0
                note            = float(stats["games"].get("rating") or 0)
                minutes         = stats["games"].get("minutes")     or 0
                equipe_id       = stats["team"]["id"]
                ligue_id_joueur = stats.get("league", {}).get("id") or ligue_id
                ratio           = round(buts / matchs, 2) if matchs > 0 else 0
                score           = round((buts * 3) + (ratio * 10) + note, 2)

                c.execute(
                    f"""INSERT INTO api_joueurs
                        (id, nom, age, nationalite, poste, equipe_id, ligue_id,
                         matchs, buts, passes, note, minutes, ratio, score, saison, date_maj)
                        VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
                        ON CONFLICT (id) DO UPDATE SET
                            nom=EXCLUDED.nom, age=EXCLUDED.age,
                            nationalite=EXCLUDED.nationalite, poste=EXCLUDED.poste,
                            equipe_id=EXCLUDED.equipe_id, ligue_id=EXCLUDED.ligue_id,
                            matchs=EXCLUDED.matchs, buts=EXCLUDED.buts,
                            passes=EXCLUDED.passes, note=EXCLUDED.note,
                            minutes=EXCLUDED.minutes, ratio=EXCLUDED.ratio,
                            score=EXCLUDED.score, saison=EXCLUDED.saison,
                            date_maj=EXCLUDED.date_maj""",
                    (joueur["id"], joueur["name"], joueur.get("age"),
                     joueur.get("nationality"), poste, equipe_id, ligue_id_joueur,
                     matchs, buts, passes, note, minutes, ratio, score,
                     SAISON, datetime.now().strftime("%Y-%m-%d %H:%M")),
                )
                total += 1

            total_pages = data.get("paging", {}).get("total", 1)
            if page >= total_pages:
                break
            page += 1
            conn.commit()

    conn.close()
    print(f"✅ {total} joueurs mis à jour (équipes actives uniquement)")


def bootstrap_classements():
    conn = get_conn()
    c    = conn.cursor()
    ph   = _ph(conn)

    init_all_tables(conn)

    c.execute("DELETE FROM classements")
    conn.commit()

    total = 0
    for nom_ligue, ligue_id in LIGUES_CIBLES.items():
        print(f"  Classement {nom_ligue}...")
        data = api_get("standings", {"league": ligue_id, "season": _saison(ligue_id)})

        try:
            standings = data["response"][0]["league"]["standings"][0]
            for team in standings:
                c.execute(f"SELECT id FROM api_equipes WHERE id = {ph}", (team["team"]["id"],))
                if not c.fetchone():
                    continue

                forme = team.get("form", "")
                h = team.get("home", {})
                a = team.get("away", {})

                c.execute(
                    f"""INSERT INTO classements
                        (equipe_id, ligue_id, rang, points, victoires, nuls, defaites,
                         buts_pour, buts_contre, diff_buts, forme, date_maj,
                         buts_dom, buts_enc_dom, matchs_dom,
                         buts_ext, buts_enc_ext, matchs_ext)
                        VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},
                                {ph},{ph},{ph},{ph},{ph},{ph})""",
                    (team["team"]["id"], ligue_id, team["rank"], team["points"],
                     team["all"]["win"], team["all"]["draw"], team["all"]["lose"],
                     team["all"]["goals"]["for"], team["all"]["goals"]["against"],
                     team["goalsDiff"], forme,
                     datetime.now().strftime("%Y-%m-%d %H:%M"),
                     h.get("goals", {}).get("for", 0),
                     h.get("goals", {}).get("against", 0),
                     h.get("played", 0),
                     a.get("goals", {}).get("for", 0),
                     a.get("goals", {}).get("against", 0),
                     a.get("played", 0)),
                )
                total += 1
        except Exception:
            pass

        conn.commit()

    conn.close()
    print(f"✅ {total} classements insérés")

def run_all():
    """Lance le bootstrap complet : BDD + ligues + équipes + joueurs + classements."""
    print("🚀 Démarrage du bootstrap...")
    print("="*50)

    init_bdd()

    print("\n📋 Étape 1/4 - Ligues...")
    bootstrap_ligues()

    print("\n👥 Étape 2/4 - Équipes...")
    bootstrap_equipes()

    print("\n⚽ Étape 3/4 - Joueurs...")
    bootstrap_joueurs()

    print("\n🏆 Étape 4/4 - Classements...")
    bootstrap_classements()

    print("\n🏆 Bootstrap terminé !")

    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) AS n FROM api_ligues")
    nb_ligues  = c.fetchone()["n"]
    c.execute("SELECT COUNT(*) AS n FROM api_equipes")
    nb_equipes = c.fetchone()["n"]
    c.execute("SELECT COUNT(*) AS n FROM api_joueurs")
    nb_joueurs = c.fetchone()["n"]
    conn.close()

    print(f"\n📊 Résumé BDD :")
    print(f"   {nb_ligues} ligues")
    print(f"   {nb_equipes} équipes")
    print(f"   {nb_joueurs} joueurs offensifs")


if __name__ == "__main__":
    run_all()