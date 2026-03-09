"""
Génère les paris du jour via Claude API et les sauvegarde dans paris_jour.
Catégories basées sur la probabilité HiddenScout :
  SAFE    >= 80%
  TENTANT  65-79%
  FUN      55-64%
  < 55%  : ignoré
"""
import os
import json
import sqlite3
import re
from collections import defaultdict
from datetime import date, datetime

import functools

import anthropic
import requests as req

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
API_SPORTS_KEY = os.environ.get("API_SPORTS_KEY", "")

LIGUES_IDS = {
    61: "Ligue 1", 62: "Ligue 2",
    39: "Premier League", 40: "Championship",
    140: "La Liga", 141: "Segunda División",
    135: "Serie A", 136: "Serie B",
    78: "Bundesliga", 79: "2. Bundesliga",
    94: "Primeira Liga", 95: "Liga Portugal 2",
    88: "Eredivisie", 89: "Eerste Divisie",
    144: "Jupiler Pro League",
    203: "Süper Lig", 204: "TFF 1. Lig",
    235: "Premier Liga (Russie)", 236: "FNL",
    106: "Ekstraklasa", 107: "I Liga",
    286: "Superliga (Serbie)",
    283: "Liga I (Roumanie)",
    197: "Super League (Grèce)",
    210: "HNL (Croatie)",
    218: "Bundesliga (Autriche)",
    207: "Super League (Suisse)",
    179: "Premiership (Écosse)",
    71: "Brasileirão Série A", 72: "Série B",
    128: "Liga Profesional (Argentine)", 131: "Primera Nacional",
    239: "Liga BetPlay (Colombie)",
    265: "Primera División (Chili)",
    268: "Primera División (Uruguay)",
}

# Ligues disponibles sur Winamax — seuls les matchs de ces ligues sont éligibles aux paris
LIGUES_WINAMAX = {
    # France
    61: "Ligue 1",
    62: "Ligue 2",
    # Angleterre
    39: "Premier League",
    40: "Championship",
    # Espagne
    140: "La Liga",
    141: "La Liga 2",
    # Allemagne
    78: "Bundesliga",
    79: "2. Bundesliga",
    # Italie
    135: "Serie A",
    136: "Serie B",
    # Pays-Bas
    88: "Eredivisie",
    # Belgique
    144: "Pro League",
    # Portugal
    94: "Primeira Liga",
    # Turquie
    203: "Süper Lig",
    # Pologne
    106: "Ekstraklasa",
    # Serbie
    286: "Super Liga",
    # Grèce
    197: "Super League 1",
    # Suisse
    207: "Super League",
    # Roumanie
    283: "Liga 1",
    # Écosse
    179: "Premiership",
    # Autriche
    218: "Bundesliga autrichienne",
    # Croatie
    210: "HNL",
    # UEFA
    2:   "Champions League",
    3:   "Europa League",
    848: "Conference League",
}

# Mots-clés indiquant une équipe réserve/espoirs — ces matchs sont exclus
_RESERVE_KEYWORDS = ("jong", "b team", "reservas", "espoirs", "u23", "u21")


def get_db():
    conn = sqlite3.connect("botfoot.db")
    conn.row_factory = sqlite3.Row
    return conn


def get_pg():
    """Connexion PostgreSQL (Railway). Fallback SQLite si aucune DATABASE_URL."""
    import psycopg2, psycopg2.extras
    db_url = os.environ.get("DATABASE_PUBLIC_URL") or os.environ.get("DATABASE_URL", "")
    if not db_url:
        conn = sqlite3.connect("botfoot.db")
        conn.row_factory = sqlite3.Row
        return conn
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


def _get_matchs_depuis_predictions(c_pg, ph, today):
    try:
        c_pg.execute(f"""
            SELECT home, away, ligue, ligue_id, pct_home, pct_nul, pct_away
            FROM predictions
            WHERE date = {ph} AND statut = 'en_attente'
            ORDER BY ligue, home
        """, (today,))
        matchs = [dict(r) for r in c_pg.fetchall()]
        # Exclure les matchs dont les probas sont les valeurs par défaut (45/25/30)
        # → signifie qu'aucune donnée de classements n'était disponible (qualite_donnees = "faible")
        avant = len(matchs)
        matchs = [
            m for m in matchs
            if not (m["pct_home"] == 45 and m["pct_nul"] == 25 and m["pct_away"] == 30)
        ]
        if avant != len(matchs):
            print(f"[generateur] {avant - len(matchs)} matchs exclus (données insuffisantes) — {len(matchs)} retenus")
        return matchs
    except Exception:
        return []


def _get_matchs_depuis_api(c, today):
    try:
        resp = req.get(
            "https://v3.football.api-sports.io/fixtures",
            headers={"x-apisports-key": API_SPORTS_KEY},
            params={"date": today, "timezone": "Europe/Paris"},
            timeout=10,
        )
        data = resp.json()
    except Exception:
        return []

    try:
        c.execute("SELECT id FROM api_ligues")
        ligues_suivies = {row["id"] for row in c.fetchall()}
    except Exception:
        ligues_suivies = set()
    if not ligues_suivies:
        ligues_suivies = set(LIGUES_IDS.keys())

    matchs = []
    for match in data.get("response", []):
        ligue_id = match["league"]["id"]
        if ligue_id not in ligues_suivies:
            continue
        matchs.append({
            "home": match["teams"]["home"]["name"],
            "away": match["teams"]["away"]["name"],
            "ligue": match["league"]["name"],
            "ligue_id": ligue_id,
            "pct_home": 45,
            "pct_nul": 25,
            "pct_away": 30,
        })
    return matchs


def _extraire_json(raw: str) -> dict | None:
    # Supprimer tout bloc markdown (```json ... ```)
    raw = re.sub(r"```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
    raw = raw.replace("```", "").strip()
    start = raw.find("{")
    end = raw.rfind("}") + 1
    if start == -1 or end == 0:
        print(f"[claude] Aucun JSON trouvé dans la réponse : {raw[:300]}")
        return None
    candidate = raw[start:end]

    # Nettoyage : virgules trailing, commentaires JS
    candidate = re.sub(r",\s*([}\]])", r"\1", candidate)          # virgules trailing
    candidate = re.sub(r"//[^\n]*", "", candidate)                 # commentaires //
    candidate = re.sub(r"/\*.*?\*/", "", candidate, flags=re.S)   # commentaires /* */

    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        pass

    try:
        import json5
        return json5.loads(candidate)
    except Exception:
        pass

    print(f"[claude] JSON invalide reçu : {candidate[:500]}")
    return None


def _get_rang_equipe(c, equipe_nom, ligue_id):
    """Retourne le rang (1=leader) de l'équipe dans sa ligue via SQLite classements."""
    try:
        c.execute("""
            SELECT cl.rang FROM classements cl
            JOIN api_equipes e ON cl.equipe_id = e.id
            WHERE cl.ligue_id = ? AND e.nom LIKE ?
            LIMIT 1
        """, (ligue_id, f"%{equipe_nom[:10]}%"))
        row = c.fetchone()
        return row["rang"] if row else None
    except Exception:
        return None


def _get_classement_details(c, equipe_nom, ligue_id):
    """Rang, forme 5J, moyennes buts marqués/encaissés depuis classements SQLite."""
    try:
        c.execute("""
            SELECT cl.rang, cl.forme, cl.buts_pour, cl.buts_contre,
                   (cl.victoires + cl.nuls + cl.defaites) AS nb_matchs
            FROM classements cl
            JOIN api_equipes e ON cl.equipe_id = e.id
            WHERE cl.ligue_id = ? AND e.nom LIKE ?
            LIMIT 1
        """, (ligue_id, f"%{equipe_nom[:10]}%"))
        row = c.fetchone()
        if row:
            nb = row["nb_matchs"] or 1
            return {
                "rang":     row["rang"] if row["rang"] else "?",
                "forme":    (row["forme"] or "?")[-5:],  # 5 derniers résultats
                "buts_moy": round(row["buts_pour"]   / nb, 2) if nb > 0 else "?",
                "buts_enc": round(row["buts_contre"] / nb, 2) if nb > 0 else "?",
            }
    except Exception:
        pass
    return {"rang": "?", "forme": "?", "buts_moy": "?", "buts_enc": "?"}


def _get_cotes_match_winamax(conn, home, away, today):
    """Retourne le dict complet des cotes Winamax pour ce match (toutes colonnes)."""
    ph   = _ph(conn)
    like = "ILIKE" if _is_pg(conn) else "LIKE"
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT cote_1, cote_x, cote_2, cote_1x, cote_x2, cote_12,
                   cote_plus25, cote_moins25, cote_btts_oui, cote_btts_non
            FROM cotes_winamax
            WHERE date = {ph} AND home {like} {ph} AND away {like} {ph}
            LIMIT 1
        """, (today, f"%{home[:8]}%", f"%{away[:8]}%"))
        row = cur.fetchone()
        if row:
            return dict(row)
    except Exception:
        pass
    return {}


def _migrer_paris_jour(c_pg):
    """Ajoute heure et value_bet à paris_jour si manquantes (PostgreSQL uniquement)."""
    for col, typ in [("heure", "TEXT"), ("value_bet", "BOOLEAN"), ("valeur_du_jour", "TEXT")]:
        try:
            c_pg.execute(f"ALTER TABLE paris_jour ADD COLUMN IF NOT EXISTS {col} {typ}")
        except Exception:
            pass


# Schéma JSON du prompt Claude — défini hors f-string pour éviter
# que Pylance interprète "value_bet": true comme du code Python invalide.
_PROMPT_JSON_SCHEMA = """\
{
  "resume": "Résumé en 1 phrase de la journée",
  "valeur_du_jour": "Meilleur value bet en 1 phrase",
  "paris": [
    {
      "categorie": "safe|tentant|fun",
      "match": "Equipe A vs Equipe B",
      "ligue": "Nom ligue",
      "heure": "18:00",
      "type_pari": "Type concis",
      "description": "Description courte (max 15 mots)",
      "probabilite_hiddenscout": 82,
      "cote": 1.45,
      "value_bet": true,
      "forme_domicile": "WWDWW",
      "forme_exterieur": "WDLWL",
      "classement": "3ème vs 15ème",
      "raisonnement": "1-2 phrases max : proba + forme + value bet"
    }
  ]
}"""


def _init_paris_combi_table(c_pg, pg):
    """Crée la table paris_combi (date + type = clé unique)."""
    if pg:
        c_pg.execute("""
            CREATE TABLE IF NOT EXISTS paris_combi (
                id               SERIAL PRIMARY KEY,
                date             DATE,
                type             VARCHAR(20) DEFAULT 'safe',
                selections       JSONB,
                cote_combinee    FLOAT,
                probabilite_jointe FLOAT,
                mise_suggeree    FLOAT,
                gain_potentiel   FLOAT,
                description      TEXT,
                resultat         VARCHAR(20) DEFAULT 'en_attente',
                created_at       TIMESTAMP DEFAULT NOW(),
                UNIQUE(date, type)
            )
        """)
        # Migration tables existantes
        for sql in [
            "ALTER TABLE paris_combi ADD COLUMN IF NOT EXISTS type VARCHAR(20) DEFAULT 'safe'",
            "ALTER TABLE paris_combi DROP CONSTRAINT IF EXISTS paris_combi_date_key",
            "ALTER TABLE paris_combi ADD CONSTRAINT paris_combi_date_type_key UNIQUE (date, type)",
        ]:
            try:
                c_pg.execute(sql)
            except Exception:
                pass
    else:
        # SQLite : recréer si la colonne type manque
        need_recreate = False
        try:
            c_pg.execute("SELECT type FROM paris_combi LIMIT 1")
        except Exception:
            need_recreate = True
        if need_recreate:
            c_pg.execute("DROP TABLE IF EXISTS paris_combi")
        c_pg.execute("""
            CREATE TABLE IF NOT EXISTS paris_combi (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                date               TEXT,
                type               TEXT DEFAULT 'safe',
                selections         TEXT,
                cote_combinee      REAL,
                probabilite_jointe REAL,
                mise_suggeree      REAL,
                gain_potentiel     REAL,
                description        TEXT,
                resultat           TEXT DEFAULT 'en_attente',
                created_at         TEXT,
                UNIQUE(date, type)
            )
        """)


def _construire_combis(paris_valides):
    """
    Retourne (combi_safe, combi_mixte).
    combi_safe  : 2-4 SAFE (proba >= 80, cote > 1.0), mise 5€
    combi_mixte : 2-4 paris (proba >= 70, SAFE+TENTANT), matchs différents du combi_safe, mise 3€
    """
    def _build(candidats, nom, mise, min_sel=2):
        vus = set()
        selections = []
        for p in candidats:
            match_key = (p.get("match") or "").strip().lower()
            if match_key and match_key not in vus:
                vus.add(match_key)
                selections.append(p)
            if len(selections) == 4:
                break
        if len(selections) < min_sel:
            print(f"[combi-{nom}] {len(selections)} éligibles — minimum {min_sel} requis, non généré")
            return None, set()

        cotes  = [round(float(p["cote"]), 2) for p in selections]
        probas = [int(p.get("probabilite_hiddenscout", 80)) for p in selections]
        cote_comb = round(functools.reduce(lambda a, b: a * b, cotes), 2)

        prob_joint = probas[0]
        for pr in probas[1:]:
            prob_joint = round(prob_joint * pr / 100, 1)

        gain = round(mise * cote_comb, 2)
        sels_list = [
            {
                "match":                   p.get("match", ""),
                "ligue":                   p.get("ligue", ""),
                "type_pari":               p.get("type_pari", ""),
                "cote":                    round(float(p["cote"]), 2),
                "probabilite_hiddenscout": int(p.get("probabilite_hiddenscout", 80)),
            }
            for p in selections
        ]
        used = {(p.get("match") or "").strip().lower() for p in selections}
        print(f"[combi-{nom}] {len(selections)} sél | cote={cote_comb} | prob={prob_joint}% | gain={gain}€")
        return {
            "selections":         sels_list,
            "cote_combinee":      cote_comb,
            "probabilite_jointe": prob_joint,
            "mise_suggeree":      mise,
            "gain_potentiel":     gain,
            "description":        f"Combi {len(selections)} sélections — cote {cote_comb}",
        }, used

    # ── Debug : état de tous les paris valides ────────────────────────────────
    print(f"[combi] Total paris_valides : {len(paris_valides)}")
    for p in paris_valides:
        proba = p.get("probabilite_hiddenscout", 0)
        cote  = p.get("cote")
        cat   = p.get("categorie", "?")
        cote_ok = cote is not None and float(cote or 0) > 1.0
        print(f"[combi]   cat={cat} proba={proba} cote={cote} (cote>1={cote_ok}) | {p.get('match')} | {p.get('type_pari')}")

    # COMBI 1 — Full SAFE
    safe_candidats = sorted(
        [p for p in paris_valides
         if int(p.get("probabilite_hiddenscout", 0) or 0) >= 80
         and float(p.get("cote") or 0) > 1.0],
        key=lambda p: int(p.get("probabilite_hiddenscout", 0) or 0),
        reverse=True,
    )
    print(f"[combi] SAFE eligibles (proba>=80, cote>1) : {len(safe_candidats)}")
    for p in safe_candidats:
        print(f"[combi]   -> proba={p.get('probabilite_hiddenscout')} cote={p.get('cote')} | {p.get('match')}")

    combi_safe, used_safe = _build(safe_candidats, "safe", 5.00)

    # COMBI 2 — SAFE + TENTANT (proba >= 70), matchs différents du combi_safe
    mixte_candidats = sorted(
        [p for p in paris_valides
         if int(p.get("probabilite_hiddenscout", 0) or 0) >= 70
         and float(p.get("cote") or 0) > 1.0
         and (p.get("match") or "").strip().lower() not in used_safe],
        key=lambda p: int(p.get("probabilite_hiddenscout", 0) or 0),
        reverse=True,
    )
    print(f"[combi] MIXTE eligibles (proba>=70, cote>1, hors safe) : {len(mixte_candidats)}")
    for p in mixte_candidats:
        print(f"[combi]   -> proba={p.get('probabilite_hiddenscout')} cote={p.get('cote')} | {p.get('match')}")

    combi_mixte, _ = _build(mixte_candidats, "mixte", 3.00)

    return combi_safe, combi_mixte


def _sauvegarder_combi(c_pg, ph, pg, combi, combi_type, today, now_ts):
    """Upsert du combi (safe ou mixte) dans paris_combi."""
    sels_json = json.dumps(combi["selections"], ensure_ascii=False)
    if pg:
        c_pg.execute(f"""
            INSERT INTO paris_combi
                (date, type, selections, cote_combinee, probabilite_jointe,
                 mise_suggeree, gain_potentiel, description, resultat, created_at)
            VALUES ({ph}, {ph}, {ph}::jsonb, {ph}, {ph}, {ph}, {ph}, {ph}, 'en_attente', NOW())
            ON CONFLICT (date, type) DO UPDATE SET
                selections         = EXCLUDED.selections,
                cote_combinee      = EXCLUDED.cote_combinee,
                probabilite_jointe = EXCLUDED.probabilite_jointe,
                gain_potentiel     = EXCLUDED.gain_potentiel,
                description        = EXCLUDED.description,
                created_at         = NOW()
        """, (
            today, combi_type, sels_json,
            combi["cote_combinee"], combi["probabilite_jointe"],
            combi["mise_suggeree"], combi["gain_potentiel"],
            combi["description"],
        ))
    else:
        c_pg.execute(f"DELETE FROM paris_combi WHERE date = {ph} AND type = {ph}", (today, combi_type))
        c_pg.execute("""
            INSERT INTO paris_combi
                (date, type, selections, cote_combinee, probabilite_jointe,
                 mise_suggeree, gain_potentiel, description, resultat, created_at)
            VALUES (?,?,?,?,?,?,?,?,'en_attente',?)
        """, (
            today, combi_type, sels_json,
            combi["cote_combinee"], combi["probabilite_jointe"],
            combi["mise_suggeree"], combi["gain_potentiel"],
            combi["description"], now_ts,
        ))
    print(f"[combi-{combi_type}] sauvegardé pour {today}")


def _appliquer_multiplicateur_classement(pct_home, pct_nul, pct_away,
                                          rang_home, rang_away,
                                          home_nom="dom", away_nom="ext"):
    """
    Booste la probabilité de l'équipe MIEUX classée si l'écart de position >= 10.
    Le multiplicateur s'applique sur les probas (espace probabilité, pas lambdas bruts).
    Renormalise pour que pct_home + pct_nul + pct_away = 100.

    Seuils :
      écart >= 10 → ×1.20
      écart >= 15 → ×1.35
      écart >= 18 → ×1.50

    Si rang inconnu pour l'une ou l'autre → multiplicateur = 1.0 (neutre).
    """
    if rang_home is None or rang_away is None:
        print(f"[classement] {home_nom} pos=? vs {away_nom} pos=? → données manquantes, multiplicateur=1.00")
        return pct_home, pct_nul, pct_away

    ecart = abs(rang_home - rang_away)
    print(f"[classement] {home_nom} pos={rang_home} vs {away_nom} pos={rang_away} → écart={ecart}", end="")

    if ecart < 10:
        print(" multiplicateur=1.00 (écart insuffisant)")
        return pct_home, pct_nul, pct_away

    mult = 1.50 if ecart >= 18 else 1.35 if ecart >= 15 else 1.20
    # rang 1 = meilleur : l'équipe avec le plus petit rang est la favorite
    home_est_favorite = rang_home < rang_away

    p_h = pct_home / 100.0
    p_n = pct_nul / 100.0
    p_a = pct_away / 100.0

    if home_est_favorite:
        p_h_new = p_h * mult
        total = p_h_new + p_n + p_a
        r_h = round(p_h_new / total * 100)
        r_n = round(p_n / total * 100)
        r_a = 100 - r_h - r_n
        cote = "dom"
    else:
        p_a_new = p_a * mult
        total = p_h + p_n + p_a_new
        r_a = round(p_a_new / total * 100)
        r_n = round(p_n / total * 100)
        r_h = 100 - r_a - r_n
        cote = "ext"

    print(f" multiplicateur={mult} (boost {cote}) | avant={pct_home}/{pct_nul}/{pct_away} → après={r_h}/{r_n}/{r_a}")
    return r_h, r_n, r_a


# ── Mapping type_pari → colonne cotes_winamax ─────────────────────────────────

_TYPE_PARI_TO_COL = {
    "victoire domicile":              "cote_1",
    "victoire à domicile":            "cote_1",
    "victoire extérieure":            "cote_2",
    "victoire à l'extérieur":         "cote_2",
    "victoire exterieure":            "cote_2",
    "nul":                            "cote_x",
    "match nul":                      "cote_x",
    "double chance 1x":               "cote_1x",
    "double chance 1 x":              "cote_1x",
    "double chance x2":               "cote_x2",
    "double chance x 2":              "cote_x2",
    "double chance 12":               "cote_12",
    "double chance 1 2":              "cote_12",
    "plus de 2.5 buts":               "cote_plus25",
    "plus 2.5 buts":                  "cote_plus25",
    "over 2.5":                       "cote_plus25",
    "moins de 2.5 buts":              "cote_moins25",
    "moins 2.5 buts":                 "cote_moins25",
    "under 2.5":                      "cote_moins25",
    "les deux équipes marquent oui":  "cote_btts_oui",
    "les deux équipes marquent - oui":"cote_btts_oui",
    "btts oui":                       "cote_btts_oui",
    "les deux équipes marquent non":  "cote_btts_non",
    "les deux équipes marquent - non":"cote_btts_non",
    "btts non":                       "cote_btts_non",
}


def _type_pari_to_col(type_pari: str) -> str | None:
    """Retourne la colonne cotes_winamax correspondant au type de pari."""
    key = type_pari.lower().strip()
    return _TYPE_PARI_TO_COL.get(key)


def _get_cote_winamax(conn, match: str, type_pari: str, today: str) -> float | None:
    """
    Cherche la cote Winamax réelle pour ce match et ce type de pari.
    `match` est au format "Equipe A vs Equipe B".
    Retourne None si introuvable.
    """
    col = _type_pari_to_col(type_pari)
    if not col:
        return None

    # Extraire home/away depuis "Equipe A vs Equipe B"
    parts = [p.strip() for p in match.split(" vs ", 1)]
    if len(parts) != 2:
        return None
    home_q, away_q = parts

    ph = _ph(conn)
    try:
        cur = conn.cursor()
        # Matching souple : ILIKE (pg) ou LIKE (sqlite), les 6 premiers caractères
        if _is_pg(conn):
            cur.execute(
                f"""SELECT {col} FROM cotes_winamax
                    WHERE date = {ph}
                      AND home ILIKE {ph}
                      AND away ILIKE {ph}
                    LIMIT 1""",
                (today, f"%{home_q[:8]}%", f"%{away_q[:8]}%"),
            )
        else:
            cur.execute(
                f"""SELECT {col} FROM cotes_winamax
                    WHERE date = {ph}
                      AND home LIKE {ph}
                      AND away LIKE {ph}
                    LIMIT 1""",
                (today, f"%{home_q[:8]}%", f"%{away_q[:8]}%"),
            )
        row = cur.fetchone()
        if row:
            val = row[col] if isinstance(row, dict) else row[0]
            return float(val) if val is not None else None
    except Exception as e:
        print(f"[winamax] erreur lookup cote ({match}, {type_pari}): {e}")
    return None


def _categorie_depuis_proba(proba: int) -> str | None:
    if proba >= 80:
        return "safe"
    if proba >= 65:
        return "tentant"
    if proba >= 55:
        return "fun"
    return None


def generer_paris() -> int:
    today = date.today().strftime("%Y-%m-%d")
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # SQLite pour les données bootstrap (api_ligues)
    conn = get_db()
    c = conn.cursor()
    # PostgreSQL pour les données persistantes (predictions, paris_jour)
    conn_pg = get_pg()
    c_pg = conn_pg.cursor()
    ph = _ph(conn_pg)

    # Migration + init tables
    pg = _is_pg(conn_pg)
    if pg:
        _migrer_paris_jour(c_pg)
    _init_paris_combi_table(c_pg, pg)
    try:
        conn_pg.commit()
    except Exception:
        pass

    # 1. Matchs — d'abord depuis predictions (PG), sinon depuis l'API (SQLite pour les ligues)
    matchs = _get_matchs_depuis_predictions(c_pg, ph, today)
    if not matchs:
        matchs = _get_matchs_depuis_api(c, today)
    if not matchs:
        conn.close()
        conn_pg.close()
        return 0

    # 1.5a. Filtrage whitelist Winamax + exclusion équipes réserves
    nb_total = len(matchs)
    matchs_filtres = []
    for m in matchs:
        ligue_id = m.get("ligue_id")
        if ligue_id not in LIGUES_WINAMAX:
            continue
        home_l = (m.get("home") or "").lower()
        away_l = (m.get("away") or "").lower()
        if any(kw in home_l or kw in away_l for kw in _RESERVE_KEYWORDS):
            continue
        matchs_filtres.append(m)
    matchs = matchs_filtres
    print(f"[whitelist] {len(matchs)} matchs retenus / {nb_total} total")

    if not matchs:
        conn.close()
        conn_pg.close()
        return 0

    # 1.5b. Ajustement classement + collecte données complètes pour le prompt
    # Seuils multiplicateur : écart >= 10 → ×1.20 | >= 15 → ×1.35 | >= 18 → ×1.50
    print(f"[classement] ajustement sur {len(matchs)} matchs…")
    for m in matchs:
        det_h = _get_classement_details(c, m["home"], m["ligue_id"])
        det_a = _get_classement_details(c, m["away"], m["ligue_id"])
        rang_h = det_h["rang"] if isinstance(det_h["rang"], int) else None
        rang_a = det_a["rang"] if isinstance(det_a["rang"], int) else None
        m["pct_home"], m["pct_nul"], m["pct_away"] = _appliquer_multiplicateur_classement(
            m["pct_home"], m["pct_nul"], m["pct_away"],
            rang_h, rang_a,
            home_nom=m["home"], away_nom=m["away"],
        )
        m["_det_home"] = det_h
        m["_det_away"] = det_a

    # 2. Construire le prompt enrichi avec toutes les données disponibles
    try:
        _cur = conn_pg.cursor()
        _cur.execute(f"SELECT COUNT(*) AS n FROM cotes_winamax WHERE date = {ph}", (today,))
        _row = _cur.fetchone()
        nb_cotes_db = (_row["n"] if isinstance(_row, dict) else _row[0]) if _row else 0
        print(f"[cotes] {nb_cotes_db} cotes disponibles en BDD pour {today}")
    except Exception:
        pass

    blocs = []
    for m in matchs[:30]:
        ph_dom  = m["pct_home"]
        ph_nul  = m["pct_nul"]
        ph_ext  = m["pct_away"]
        det_h   = m.get("_det_home", {"rang": "?", "forme": "?", "buts_moy": "?", "buts_enc": "?"})
        det_a   = m.get("_det_away", {"rang": "?", "forme": "?", "buts_moy": "?", "buts_enc": "?"})
        heure   = m.get("heure", "?")

        # Cotes Winamax pour ce match
        cw = _get_cotes_match_winamax(conn_pg, m["home"], m["away"], today)

        # Value bet : comparer la proba dominante à la cote implicite Winamax
        dom_proba, dom_col = max(
            (ph_dom, "cote_1"), (ph_nul, "cote_x"), (ph_ext, "cote_2"),
            key=lambda x: x[0],
        )
        cote_dom = cw.get(dom_col)
        if cote_dom and cote_dom > 1:
            cote_impl = round(100 / cote_dom, 1)
            value_bet  = dom_proba > cote_impl
            vb_str     = f"{'✅ OUI' if value_bet else '❌ NON'} (HiddenScout={dom_proba}% vs implicite={cote_impl}%)"
        else:
            cote_impl = "?"
            value_bet  = False
            vb_str     = f"? (pas de cote Winamax)"

        def _fmt(v):
            return str(v) if v is not None else "?"

        blocs.append(
            f"{m['home']} vs {m['away']} ({m['ligue']}) — {heure}\n"
            f"  Probas HiddenScout : dom={ph_dom}% nul={ph_nul}% ext={ph_ext}%\n"
            f"  Classement : {det_h['rang']}ème vs {det_a['rang']}ème\n"
            f"  Forme dom (5J) : {det_h['forme']} | Forme ext (5J) : {det_a['forme']}\n"
            f"  Buts marqués/match : dom={det_h['buts_moy']} ext={det_a['buts_moy']}\n"
            f"  Buts encaissés/match : dom={det_h['buts_enc']} ext={det_a['buts_enc']}\n"
            f"  Cotes Winamax : 1={_fmt(cw.get('cote_1'))} X={_fmt(cw.get('cote_x'))} "
            f"2={_fmt(cw.get('cote_2'))} 1X={_fmt(cw.get('cote_1x'))} "
            f"X2={_fmt(cw.get('cote_x2'))} +2.5={_fmt(cw.get('cote_plus25'))}\n"
            f"  Value bet : {vb_str}"
        )
        # Stocker pour l'enrichissement post-Claude
        m["_cotes_winamax"] = cw
        m["_value_bet"]     = value_bet

    matchs_text = "\n\n".join(blocs)

    prompt = f"""Tu es un analyste sportif expert travaillant avec HiddenScout.
Tu reçois les données complètes des matchs du {today}.

MATCHS DU JOUR :
{matchs_text}

RÈGLES STRICTES :
1. Catégories basées UNIQUEMENT sur probabilité_hiddenscout :
   - SAFE    : >= 80%
   - TENTANT : 65-79%
   - FUN     : 55-64%
   - Ignorer < 55%

2. PRIORISE les value bets (quand HiddenScout > cote implicite Winamax)

3. Maximum 2 paris par match, maximum 2 buteurs par match

4. Types autorisés :
   Victoire domicile / Victoire extérieure / Nul /
   Double chance 1X / Double chance X2 / Double chance 12 /
   Plus de 2.5 buts / Moins de 2.5 buts /
   Les deux équipes marquent oui / Les deux équipes marquent non

5. Dans le raisonnement, croiser OBLIGATOIREMENT :
   - La proba Poisson
   - La forme récente
   - Le classement si disponible
   - Le value bet si applicable

6. Diversifie les matchs — ne concentre pas tous les paris sur 1-2 matchs

7. Si aucun pari >= 55% → "paris": []

8. Raisonnement : 1-2 phrases MAX par pari (sois concis, pas de répétition).

Réponds UNIQUEMENT en JSON valide sans markdown :
{_PROMPT_JSON_SCHEMA}

Génère entre 6 et 10 paris bien répartis entre les catégories disponibles."""

    # 3. Appeler Claude
    print("[claude-prompt] ========== PROMPT ENVOYÉ ==========")
    print(prompt[:3000])
    print("[claude-prompt] ========== FIN PROMPT ==========")

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=8000,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = message.content[0].text.strip()

    print("[claude-raw] ========== RÉPONSE BRUTE ==========")
    print(raw[:2000])
    print("[claude-raw] ========== FIN RÉPONSE ==========")

    # 4. Parser le JSON
    parsed = _extraire_json(raw)
    if parsed is None:
        print("[generateur] Claude n'a pas retourné de JSON valide")
        conn.close()
        conn_pg.close()
        return 0

    # 5. Post-traitement : appliquer les règles métier
    paris_bruts = parsed.get("paris", [])

    # Forcer la catégorie depuis probabilite_hiddenscout (ignore ce que Claude dit)
    paris_valides = []
    paris_par_match = defaultdict(int)
    buteurs_par_match = defaultdict(int)

    # Trier par probabilite_hiddenscout décroissante
    paris_bruts.sort(key=lambda p: int(p.get("probabilite_hiddenscout", 0) or 0), reverse=True)

    for p in paris_bruts:
        try:
            proba_hs = int(p.get("probabilite_hiddenscout", 0) or 0)
        except (TypeError, ValueError):
            proba_hs = 0

        cat = _categorie_depuis_proba(proba_hs)
        if cat is None:
            continue  # < 55%, on ignore

        match_key = p.get("match", "").strip().lower()

        # Max 2 paris par match
        if paris_par_match[match_key] >= 2:
            continue

        # Max 2 buteurs par match
        type_lower = (p.get("type_pari", "") or "").lower()
        is_buteur = "buteur" in type_lower or "marquer" in type_lower or "scorer" in type_lower
        if is_buteur and buteurs_par_match[match_key] >= 2:
            continue

        p["categorie"] = cat
        paris_valides.append(p)
        paris_par_match[match_key] += 1
        if is_buteur:
            buteurs_par_match[match_key] += 1

    # 5b. Enrichir avec les cotes Winamax réelles
    nb_cotes_trouvees = 0
    for p in paris_valides:
        cote_win = _get_cote_winamax(conn_pg, p.get("match", ""), p.get("type_pari", ""), today)
        if cote_win is not None:
            old = p.get("cote")
            p["cote"] = cote_win
            nb_cotes_trouvees += 1
            print(f"[winamax] {p['match']} | {p['type_pari']} → cote réelle={cote_win} (Claude={old})")
        else:
            print(f"[winamax] {p['match']} | {p['type_pari']} → cote Winamax introuvable, cote Claude conservée")

    print(f"[winamax] {nb_cotes_trouvees}/{len(paris_valides)} cotes Winamax trouvées")

    # 6. Effacer et réinsérer
    c_pg.execute(f"DELETE FROM paris_jour WHERE date = {ph}", (today,))

    resume = parsed.get("resume", "")
    if resume:
        c_pg.execute(
            f"""INSERT INTO paris_jour
               (date, categorie, match, ligue, type_pari, description, cote, probabilite, raisonnement, timestamp)
               VALUES ({ph},'resume','','','',{ph},0,0,'',{ph})""",
            (today, resume, now_ts),
        )

    valeur = parsed.get("valeur_du_jour", "")
    if valeur:
        c_pg.execute(
            f"""INSERT INTO paris_jour
               (date, categorie, match, ligue, type_pari, description, cote, probabilite, raisonnement, timestamp)
               VALUES ({ph},'valeur','','','',{ph},0,0,'',{ph})""",
            (today, valeur, now_ts),
        )

    combi = parsed.get("combi_du_jour", {})
    if combi and isinstance(combi, dict):
        try:
            c_pg.execute(
                f"""INSERT INTO paris_jour
                   (date, categorie, match, ligue, type_pari, description, cote, probabilite, raisonnement, timestamp)
                   VALUES ({ph},'combi','','','Combiné',{ph},{ph},{ph},{ph},{ph})""",
                (
                    today,
                    json.dumps(combi, ensure_ascii=False),
                    float(combi.get("cote_combinee", 0) or 0),
                    int(combi.get("probabilite_jointe", 0) or 0),
                    combi.get("description", ""),
                    now_ts,
                ),
            )
        except Exception as e:
            print(f"[generateur] combi_du_jour non sauvegardé : {e}")

    count = 0
    for p in paris_valides:
        try:
            cote = float(p.get("cote", 1.5))
        except (TypeError, ValueError):
            cote = 1.5
        proba_hs = int(p.get("probabilite_hiddenscout", p.get("probabilite", 60)) or 60)
        value_bet = bool(p.get("value_bet", False))
        heure_p   = str(p.get("heure", "") or "")

        # Insertion avec heure/value_bet si les colonnes existent
        try:
            c_pg.execute(
                f"""INSERT INTO paris_jour
                   (date, categorie, match, ligue, type_pari, description, cote,
                    probabilite, probabilite_hiddenscout, raisonnement, heure, value_bet, timestamp)
                   VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})""",
                (
                    today, p["categorie"],
                    p.get("match", ""), p.get("ligue", ""),
                    p.get("type_pari", ""), p.get("description", ""),
                    cote, proba_hs, proba_hs,
                    p.get("raisonnement", ""), heure_p, value_bet, now_ts,
                ),
            )
        except Exception:
            # Fallback sans les nouvelles colonnes
            c_pg.execute(
                f"""INSERT INTO paris_jour
                   (date, categorie, match, ligue, type_pari, description, cote,
                    probabilite, probabilite_hiddenscout, raisonnement, timestamp)
                   VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})""",
                (
                    today, p["categorie"],
                    p.get("match", ""), p.get("ligue", ""),
                    p.get("type_pari", ""), p.get("description", ""),
                    cote, proba_hs, proba_hs,
                    p.get("raisonnement", ""), now_ts,
                ),
            )
        count += 1

    conn_pg.commit()

    # ── Combis du jour ─────────────────────────────────────────────────────────
    combi_safe, combi_mixte = _construire_combis(paris_valides)
    for combi_obj, combi_type in [(combi_safe, "safe"), (combi_mixte, "mixte")]:
        if combi_obj:
            try:
                _sauvegarder_combi(c_pg, ph, pg, combi_obj, combi_type, today, now_ts)
                conn_pg.commit()
            except Exception as e:
                print(f"[combi-{combi_type}] erreur sauvegarde: {e}")

    # ── Sauvegarde automatique dans paris_historique ──────────────────────────
    heure_gen = now_ts[11:16]  # "HH:MM"
    for p in paris_valides:
        try:
            cote_h = float(p.get("cote", 1.5))
        except (TypeError, ValueError):
            cote_h = 1.5
        proba_h = int(p.get("probabilite_hiddenscout", p.get("probabilite", 60)) or 60)
        c_pg.execute(
            f"""INSERT INTO paris_historique
               (date, match, ligue, categorie, type_pari, description,
                cote, probabilite_hiddenscout, heure_generation, gagne)
               VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},NULL)
               ON CONFLICT (date, match, type_pari) DO UPDATE SET
                   categorie = EXCLUDED.categorie,
                   description = EXCLUDED.description,
                   cote = EXCLUDED.cote,
                   probabilite_hiddenscout = EXCLUDED.probabilite_hiddenscout,
                   heure_generation = EXCLUDED.heure_generation""",
            (
                today, p.get("match", ""), p.get("ligue", ""),
                p["categorie"], p.get("type_pari", ""), p.get("description", ""),
                cote_h, proba_h, heure_gen,
            ),
        )
    conn_pg.commit()
    conn.close()
    conn_pg.close()
    return count
