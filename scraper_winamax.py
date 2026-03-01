#!/usr/bin/env python3
"""
Scraper cotes Winamax (endpoints publics, sans compte ni Selenium).

Usage :
    python scraper_winamax.py

La fonction run() est importée par generateur_paris.py.
"""

import requests
import sqlite3
import logging
from datetime import date, datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE = "https://www.winamax.fr/appsports"
HDR  = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/122.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Referer":         "https://www.winamax.fr/paris-sportifs/",
    "Origin":          "https://www.winamax.fr",
}


# ── Base de données ────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect("botfoot.db")
    conn.row_factory = sqlite3.Row
    return conn


def init_table():
    conn = get_db()
    conn.cursor().execute("""
        CREATE TABLE IF NOT EXISTS cotes_winamax (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            date         TEXT,
            home         TEXT,
            away         TEXT,
            ligue        TEXT,
            cote_home    REAL,
            cote_nul     REAL,
            cote_away    REAL,
            cote_over25  REAL,
            cote_under25 REAL,
            match_id     TEXT,
            timestamp    TEXT,
            UNIQUE(date, home, away)
        )
    """)
    conn.commit()
    conn.close()


# ── HTTP helpers ───────────────────────────────────────────────────────────────

def _fetch(url, params=None):
    try:
        r = requests.get(url, headers=HDR, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError as e:
        log.warning(f"HTTP {e.response.status_code} → {url}")
    except Exception as e:
        log.warning(f"Erreur fetch {url}: {e}")
    return None


# ── Découverte du sport Football ───────────────────────────────────────────────

def _get_football_id():
    data = _fetch(f"{BASE}/sports")
    if not data:
        log.info("Impossible de contacter /sports — fallback sportId=1")
        return 1
    log.info(f"/sports réponse (extrait): {str(data)[:300]}")
    sports = data if isinstance(data, list) else data.get("sports", data.get("data", []))
    if isinstance(sports, list):
        for s in sports:
            if not isinstance(s, dict):
                continue
            nom = str(s.get("name") or s.get("sportName") or s.get("label") or "").lower()
            if "football" in nom or "soccer" in nom:
                sid = s.get("id") or s.get("sportId") or s.get("sport_id")
                log.info(f"Football trouvé : id={sid}")
                return sid
    log.info("Football non trouvé dans la liste — fallback id=1")
    return 1


# ── Récupération des matchs du jour ───────────────────────────────────────────

def _fetch_today_matches(sport_id):
    today = date.today().strftime("%Y-%m-%d")
    patterns = [
        (f"{BASE}/sports/{sport_id}/events",       {"date": today}),
        (f"{BASE}/sports/{sport_id}/matches",       {"date": today}),
        (f"{BASE}/sports/{sport_id}/competitions",  None),
        (f"{BASE}/matches",                         {"sportId": sport_id, "date": today}),
        (f"{BASE}/events",                          {"sportId": sport_id, "date": today}),
    ]
    for url, params in patterns:
        data = _fetch(url, params)
        if not data:
            continue
        log.info(f"Réponse via {url} (extrait): {str(data)[:150]}")
        matches = _flatten(data)
        if matches:
            log.info(f"{len(matches)} objet(s) match trouvés via {url}")
            return matches
    return []


def _flatten(data):
    """Extrait récursivement une liste plate d'objets match depuis n'importe quelle structure."""
    if isinstance(data, list):
        # Si c'est une liste de compétitions (elles-mêmes avec un champ matches), on récurse
        if data and isinstance(data[0], dict):
            for key in ("matches", "events", "items"):
                if key in data[0]:
                    combined = []
                    for item in data:
                        combined.extend(_flatten(item.get(key, [])))
                    return combined
        return data
    if isinstance(data, dict):
        for key in ("matches", "events", "items", "data", "results"):
            val = data.get(key)
            if isinstance(val, list) and val:
                return _flatten(val)
    return []


# ── Parsing des cotes pour un match ───────────────────────────────────────────

def _safe_float(v):
    try:
        return round(float(v), 2)
    except (TypeError, ValueError):
        return None


def _extract_match(m, today):
    if not isinstance(m, dict):
        return None

    # Filtre date
    raw_date = (m.get("date") or m.get("startDate") or
                m.get("scheduledDate") or m.get("matchDate") or "")
    if raw_date and today not in str(raw_date)[:10]:
        return None

    # Noms d'équipes
    def team_name(t):
        if isinstance(t, dict):
            return t.get("name") or t.get("teamName") or ""
        return str(t) if t else ""

    home = team_name(m.get("homeTeam") or m.get("home") or
                     m.get("team1")    or m.get("localTeam") or {})
    away = team_name(m.get("awayTeam") or m.get("away") or
                     m.get("team2")    or m.get("visitorTeam") or {})
    if not home or not away:
        return None

    # Marchés
    cote_home = cote_nul = cote_away = cote_over = cote_under = None
    markets = (m.get("bets") or m.get("odds") or
               m.get("markets") or m.get("betGroups") or [])
    if isinstance(markets, dict):
        markets = list(markets.values())
    if not isinstance(markets, list):
        markets = []

    for mkt in markets:
        if not isinstance(mkt, dict):
            continue
        label = str(mkt.get("label") or mkt.get("name") or
                    mkt.get("marketName") or "").lower()
        sels  = (mkt.get("selections") or mkt.get("outcomes") or
                 mkt.get("odds") or [])
        if not isinstance(sels, list):
            continue

        # ── 1N2 ──
        is_1n2 = (
            any(k in label for k in ("1n2", "1 x 2", "résultat", "match result"))
            or (("winner" in label or "vainqueur" in label) and len(sels) == 3)
        )
        if is_1n2:
            for sel in sels:
                if not isinstance(sel, dict):
                    continue
                slabel = str(sel.get("label") or sel.get("name") or "").strip()
                cote   = _safe_float(sel.get("price") or sel.get("odds") or
                                     sel.get("odd")   or sel.get("value"))
                if not cote:
                    continue
                if slabel in ("1", "Domicile", "Home"):
                    cote_home = cote
                elif slabel in ("N", "X", "Nul", "Draw"):
                    cote_nul  = cote
                elif slabel in ("2", "Extérieur", "Away"):
                    cote_away = cote
                else:
                    # Attribution positionnelle si labels non reconnus
                    if cote_home is None:   cote_home = cote
                    elif cote_nul is None:  cote_nul  = cote
                    elif cote_away is None: cote_away = cote

        # ── Over/Under 2.5 ──
        is_ou = (
            "2.5" in label
            or ("over" in label and "under" in label)
            or any(k in label for k in ("plus/moins", "total buts", "nombre de buts"))
        )
        if is_ou:
            for sel in sels:
                if not isinstance(sel, dict):
                    continue
                slabel = str(sel.get("label") or sel.get("name") or "").lower()
                cote   = _safe_float(sel.get("price") or sel.get("odds") or
                                     sel.get("odd")   or sel.get("value"))
                if not cote:
                    continue
                if any(k in slabel for k in ("over", "plus", "+2", "sup")):
                    cote_over  = cote
                elif any(k in slabel for k in ("under", "moins", "-2", "inf")):
                    cote_under = cote

    # Ignorer si aucune cote exploitable
    if not any([cote_home, cote_nul, cote_away, cote_over, cote_under]):
        return None

    return {
        "home":        home,
        "away":        away,
        "ligue":       str(m.get("competitionName") or m.get("competition") or
                          m.get("league") or m.get("tournamentName") or ""),
        "cote_home":   cote_home,
        "cote_nul":    cote_nul,
        "cote_away":   cote_away,
        "cote_over25": cote_over,
        "cote_under25":cote_under,
        "match_id":    str(m.get("id") or m.get("matchId") or m.get("eventId") or ""),
    }


# ── Sauvegarde ────────────────────────────────────────────────────────────────

def _save(cotes):
    if not cotes:
        return 0
    today = date.today().strftime("%Y-%m-%d")
    ts    = datetime.now().strftime("%Y-%m-%d %H:%M")
    conn  = get_db()
    c     = conn.cursor()
    c.execute("DELETE FROM cotes_winamax WHERE date = ?", (today,))
    n = 0
    for co in cotes:
        if not co.get("home") or not co.get("away"):
            continue
        c.execute("""
            INSERT OR REPLACE INTO cotes_winamax
            (date, home, away, ligue, cote_home, cote_nul, cote_away,
             cote_over25, cote_under25, match_id, timestamp)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (today, co["home"], co["away"], co.get("ligue", ""),
              co.get("cote_home"), co.get("cote_nul"), co.get("cote_away"),
              co.get("cote_over25"), co.get("cote_under25"),
              co.get("match_id", ""), ts))
        n += 1
    conn.commit()
    conn.close()
    log.info(f"{n} matchs avec cotes sauvegardés")
    return n


# ── Point d'entrée ────────────────────────────────────────────────────────────

def run():
    init_table()
    today = date.today().strftime("%Y-%m-%d")
    log.info(f"=== Scraping Winamax {today} ===")

    sport_id = _get_football_id()
    raw      = _fetch_today_matches(sport_id)

    cotes = []
    for m in raw:
        result = _extract_match(m, today)
        if result:
            cotes.append(result)

    n = _save(cotes)
    if n == 0:
        log.warning(
            "Aucune cote enregistrée. Winamax peut filtrer les requêtes automatiques "
            "ou avoir modifié sa structure d'API. "
            "Les paris seront générés sur la base des probabilités HiddenScout seules."
        )
    return n


if __name__ == "__main__":
    n = run()
    print(f"\n{'✅' if n else '⚠️ '} {n} matchs avec cotes sauvegardés")
    if n == 0:
        print("Vérifiez les logs ci-dessus pour diagnostiquer le problème Winamax.")
