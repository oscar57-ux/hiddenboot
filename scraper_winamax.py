#!/usr/bin/env python3
"""
Scraper cotes Winamax (endpoints publics, sans compte ni Selenium).

Usage :
    python scraper_winamax.py

La fonction run() est importée par generateur_paris.py.
"""

import os
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


def get_pg():
    """Connexion PostgreSQL (Railway). Fallback SQLite si aucune DATABASE_URL."""
    db_url = os.environ.get("DATABASE_PUBLIC_URL") or os.environ.get("DATABASE_URL", "")
    if not db_url:
        conn = sqlite3.connect("botfoot.db")
        conn.row_factory = sqlite3.Row
        return conn
    import psycopg2, psycopg2.extras
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    if "sslmode" not in db_url:
        sep = "&" if "?" in db_url else "?"
        db_url += f"{sep}sslmode=require"
    conn = psycopg2.connect(db_url)
    conn.cursor_factory = psycopg2.extras.RealDictCursor
    return conn


def _is_pg(conn):
    try:
        import psycopg2
        return isinstance(conn, psycopg2.extensions.connection)
    except Exception:
        return False


def _ph(conn):
    return "%s" if _is_pg(conn) else "?"


NOUVEAU_SCHEMA = [
    "cote_1", "cote_x", "cote_2",
    "cote_1x", "cote_x2", "cote_12",
    "cote_plus25", "cote_moins25",
    "cote_btts_oui", "cote_btts_non",
]


def init_table():
    conn = get_pg()
    cur = conn.cursor()
    pg = _is_pg(conn)

    if pg:
        # PostgreSQL : créer si n'existe pas, puis ajouter colonnes manquantes
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cotes_winamax (
                id           SERIAL PRIMARY KEY,
                date         TEXT,
                home         TEXT,
                away         TEXT,
                ligue        TEXT,
                cote_1       REAL,
                cote_x       REAL,
                cote_2       REAL,
                cote_1x      REAL,
                cote_x2      REAL,
                cote_12      REAL,
                cote_plus25  REAL,
                cote_moins25 REAL,
                cote_btts_oui REAL,
                cote_btts_non REAL,
                match_id     TEXT,
                timestamp    TEXT,
                UNIQUE(date, home, away)
            )
        """)
        # Migration : ajouter colonnes manquantes
        for col in NOUVEAU_SCHEMA:
            try:
                cur.execute(f"ALTER TABLE cotes_winamax ADD COLUMN IF NOT EXISTS {col} REAL")
            except Exception:
                pass
        # Supprimer anciens alias si présents (ne bloque pas si absent)
        for old in ("cote_home", "cote_nul", "cote_away", "cote_over25", "cote_under25"):
            try:
                cur.execute(f"ALTER TABLE cotes_winamax DROP COLUMN IF EXISTS {old}")
            except Exception:
                pass
    else:
        # SQLite : vérifier le schéma actuel
        cur.execute("PRAGMA table_info(cotes_winamax)")
        cols = [row[1] for row in cur.fetchall()]
        if cols and "cote_1" not in cols:
            # Ancien schéma → drop et recrée (données quotidiennes, pas de perte)
            log.info("Migration cotes_winamax : ancien schéma détecté, recréation de la table")
            cur.execute("DROP TABLE IF EXISTS cotes_winamax")
            cols = []
        if not cols:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cotes_winamax (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    date          TEXT,
                    home          TEXT,
                    away          TEXT,
                    ligue         TEXT,
                    cote_1        REAL,
                    cote_x        REAL,
                    cote_2        REAL,
                    cote_1x       REAL,
                    cote_x2       REAL,
                    cote_12       REAL,
                    cote_plus25   REAL,
                    cote_moins25  REAL,
                    cote_btts_oui REAL,
                    cote_btts_non REAL,
                    match_id      TEXT,
                    timestamp     TEXT,
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
    cote_1 = cote_x = cote_2 = None
    cote_1x = cote_x2 = cote_12 = None
    cote_plus25 = cote_moins25 = None
    cote_btts_oui = cote_btts_non = None

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

        def _cote_sel(sel):
            return _safe_float(sel.get("price") or sel.get("odds") or
                               sel.get("odd")   or sel.get("value"))

        # ── 1N2 (résultat final) ──
        is_1n2 = (
            any(k in label for k in ("1n2", "1 x 2", "résultat", "match result"))
            or (("winner" in label or "vainqueur" in label) and len(sels) == 3)
        )
        # Exclure les marchés Double Chance qui contiennent aussi "1x"/"x2"
        is_dc_label = any(k in label for k in ("double chance", "double résultat"))
        if is_1n2 and not is_dc_label:
            pos = 0
            for sel in sels:
                if not isinstance(sel, dict):
                    continue
                slabel = str(sel.get("label") or sel.get("name") or "").strip()
                cote   = _cote_sel(sel)
                if not cote:
                    continue
                if slabel in ("1", "Domicile", "Home"):
                    cote_1 = cote
                elif slabel in ("N", "X", "Nul", "Draw"):
                    cote_x = cote
                elif slabel in ("2", "Extérieur", "Away"):
                    cote_2 = cote
                else:
                    # Attribution positionnelle si labels non reconnus
                    if pos == 0:   cote_1 = cote
                    elif pos == 1: cote_x = cote
                    elif pos == 2: cote_2 = cote
                    pos += 1

        # ── Double Chance ──
        is_dc = is_dc_label or (
            any(k in label for k in ("double chance",))
        )
        if is_dc:
            for sel in sels:
                if not isinstance(sel, dict):
                    continue
                slabel = str(sel.get("label") or sel.get("name") or "").strip().lower()
                cote   = _cote_sel(sel)
                if not cote:
                    continue
                if slabel in ("1x", "domicile/nul", "home/draw", "1 ou x"):
                    cote_1x = cote
                elif slabel in ("x2", "nul/extérieur", "draw/away", "x ou 2"):
                    cote_x2 = cote
                elif slabel in ("12", "domicile/extérieur", "home/away", "1 ou 2"):
                    cote_12 = cote

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
                cote   = _cote_sel(sel)
                if not cote:
                    continue
                if any(k in slabel for k in ("over", "plus", "+2", "sup", "más")):
                    cote_plus25  = cote
                elif any(k in slabel for k in ("under", "moins", "-2", "inf", "menos")):
                    cote_moins25 = cote

        # ── Les deux équipes marquent (BTTS) ──
        is_btts = any(k in label for k in (
            "les deux équipes marquent", "both teams", "btts",
            "gg/ng", "les 2 équipes", "les deux équipes",
        ))
        if is_btts:
            for sel in sels:
                if not isinstance(sel, dict):
                    continue
                slabel = str(sel.get("label") or sel.get("name") or "").lower()
                cote   = _cote_sel(sel)
                if not cote:
                    continue
                if any(k in slabel for k in ("oui", "yes", "gg", "true")):
                    cote_btts_oui = cote
                elif any(k in slabel for k in ("non", "no", "ng", "false")):
                    cote_btts_non = cote

    # Ignorer si aucune cote exploitable
    if not any([cote_1, cote_x, cote_2, cote_plus25, cote_moins25,
                cote_1x, cote_x2, cote_12, cote_btts_oui, cote_btts_non]):
        return None

    return {
        "home":          home,
        "away":          away,
        "ligue":         str(m.get("competitionName") or m.get("competition") or
                             m.get("league") or m.get("tournamentName") or ""),
        "cote_1":        cote_1,
        "cote_x":        cote_x,
        "cote_2":        cote_2,
        "cote_1x":       cote_1x,
        "cote_x2":       cote_x2,
        "cote_12":       cote_12,
        "cote_plus25":   cote_plus25,
        "cote_moins25":  cote_moins25,
        "cote_btts_oui": cote_btts_oui,
        "cote_btts_non": cote_btts_non,
        "match_id":      str(m.get("id") or m.get("matchId") or m.get("eventId") or ""),
    }


# ── Sauvegarde ────────────────────────────────────────────────────────────────

def _save(cotes):
    if not cotes:
        return 0
    today = date.today().strftime("%Y-%m-%d")
    ts    = datetime.now().strftime("%Y-%m-%d %H:%M")
    conn  = get_pg()
    c     = conn.cursor()
    ph    = _ph(conn)

    c.execute(f"DELETE FROM cotes_winamax WHERE date = {ph}", (today,))
    n = 0
    for co in cotes:
        if not co.get("home") or not co.get("away"):
            continue
        if _is_pg(conn):
            c.execute("""
                INSERT INTO cotes_winamax
                (date, home, away, ligue,
                 cote_1, cote_x, cote_2,
                 cote_1x, cote_x2, cote_12,
                 cote_plus25, cote_moins25,
                 cote_btts_oui, cote_btts_non,
                 match_id, timestamp)
                VALUES (%s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s, %s,%s, %s,%s)
                ON CONFLICT (date, home, away) DO UPDATE SET
                    cote_1=EXCLUDED.cote_1, cote_x=EXCLUDED.cote_x, cote_2=EXCLUDED.cote_2,
                    cote_1x=EXCLUDED.cote_1x, cote_x2=EXCLUDED.cote_x2, cote_12=EXCLUDED.cote_12,
                    cote_plus25=EXCLUDED.cote_plus25, cote_moins25=EXCLUDED.cote_moins25,
                    cote_btts_oui=EXCLUDED.cote_btts_oui, cote_btts_non=EXCLUDED.cote_btts_non,
                    timestamp=EXCLUDED.timestamp
            """, (
                today, co["home"], co["away"], co.get("ligue", ""),
                co.get("cote_1"), co.get("cote_x"), co.get("cote_2"),
                co.get("cote_1x"), co.get("cote_x2"), co.get("cote_12"),
                co.get("cote_plus25"), co.get("cote_moins25"),
                co.get("cote_btts_oui"), co.get("cote_btts_non"),
                co.get("match_id", ""), ts,
            ))
        else:
            c.execute("""
                INSERT OR REPLACE INTO cotes_winamax
                (date, home, away, ligue,
                 cote_1, cote_x, cote_2,
                 cote_1x, cote_x2, cote_12,
                 cote_plus25, cote_moins25,
                 cote_btts_oui, cote_btts_non,
                 match_id, timestamp)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                today, co["home"], co["away"], co.get("ligue", ""),
                co.get("cote_1"), co.get("cote_x"), co.get("cote_2"),
                co.get("cote_1x"), co.get("cote_x2"), co.get("cote_12"),
                co.get("cote_plus25"), co.get("cote_moins25"),
                co.get("cote_btts_oui"), co.get("cote_btts_non"),
                co.get("match_id", ""), ts,
            ))
        n += 1
        # Log des cotes trouvées pour ce match
        cotes_trouvees = {k: v for k, v in co.items()
                          if k.startswith("cote_") and v is not None}
        log.info(f"  {co['home']} vs {co['away']} → {cotes_trouvees}")
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


# Alias pour les tests rapides : railway run python -c "from scraper_winamax import scraper_cotes; scraper_cotes()"
scraper_cotes = run


if __name__ == "__main__":
    n = run()
    print(f"\n{'✅' if n else '⚠️ '} {n} matchs avec cotes sauvegardés")
    if n == 0:
        print("Vérifiez les logs ci-dessus pour diagnostiquer le problème Winamax.")
