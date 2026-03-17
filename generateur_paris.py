"""
Génère les paris du jour via Claude API et les sauvegarde dans paris_jour.
Classification contextuelle HiddenScout :
  SAFE    >= 80% ET au moins 1 critère contexte (écart rang >= 8, top-5, forme >= 4W/5)
  TENTANT >= 80% sans critère OU 65-79%
  FUN      55-64%
  < 55%  : ignoré
"""
import os
import json
import sqlite3
import re
from collections import defaultdict
from datetime import date, datetime

import unicodedata
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
    """Connexion unifiée — PostgreSQL en prod, SQLite en local (alias de get_pg)."""
    return get_pg()


def get_pg():
    """Connexion PostgreSQL (Railway). Fallback SQLite si aucune DATABASE_URL.
    Le curseur PG accepte '?' comme placeholder (auto-converti en '%s').
    """
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

    class _CompatCursor(psycopg2.extras.RealDictCursor):
        def execute(self, query, vars=None):
            if isinstance(query, str):
                query = query.replace("?", "%s")
            return super().execute(query, vars)
        def executemany(self, query, vars_list):
            if isinstance(query, str):
                query = query.replace("?", "%s")
            return super().executemany(query, vars_list)

    conn = psycopg2.connect(db_url)
    conn.cursor_factory = _CompatCursor
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
    """Rang, forme, stats complètes + dom/ext depuis classements SQLite."""
    _default = {
        "rang": "?", "points": 0, "forme": "?", "serie_wins": 0,
        "mj": "?", "victoires": "?", "nuls": "?", "defaites": "?",
        "buts_moy": "?", "buts_enc": "?",
        "buts_dom": "?", "buts_enc_dom": "?",
        "buts_ext": "?", "buts_enc_ext": "?",
        "equipe_id": None,
    }
    try:
        c.execute("""
            SELECT cl.rang, cl.forme, cl.buts_pour, cl.buts_contre,
                   cl.victoires, cl.nuls, cl.defaites, cl.points, cl.equipe_id,
                   (cl.victoires + cl.nuls + cl.defaites) AS nb_matchs
            FROM classements cl
            JOIN api_equipes e ON cl.equipe_id = e.id
            WHERE cl.ligue_id = ? AND e.nom LIKE ?
            LIMIT 1
        """, (ligue_id, f"%{equipe_nom[:10]}%"))
        row = c.fetchone()
        # Fallback championnat domestique (coupe européenne → pas de classement direct)
        if not row:
            c.execute("""
                SELECT e.ligue_id FROM api_equipes e WHERE e.nom LIKE ? LIMIT 1
            """, (f"%{equipe_nom[:10]}%",))
            ligue_row = c.fetchone()
            if ligue_row and ligue_row["ligue_id"]:
                ligue_id = ligue_row["ligue_id"]
                c.execute("""
                    SELECT cl.rang, cl.forme, cl.buts_pour, cl.buts_contre,
                           cl.victoires, cl.nuls, cl.defaites, cl.points, cl.equipe_id,
                           (cl.victoires + cl.nuls + cl.defaites) AS nb_matchs
                    FROM classements cl
                    WHERE cl.ligue_id = ? AND cl.equipe_id = (
                        SELECT id FROM api_equipes WHERE nom LIKE ? LIMIT 1
                    )
                    LIMIT 1
                """, (ligue_id, f"%{equipe_nom[:10]}%"))
                row = c.fetchone()
        if not row:
            return _default
        nb = row["nb_matchs"] or 1
        forme_raw = row["forme"] or ""
        serie = 0
        for ch in forme_raw:
            if ch == "W":
                serie += 1
            else:
                break
        result = {
            "rang":       row["rang"] if row["rang"] else "?",
            "points":     row["points"] or 0,
            "forme":      forme_raw[:5],
            "serie_wins": serie,
            "mj":         nb,
            "victoires":  row["victoires"] or 0,
            "nuls":       row["nuls"]      or 0,
            "defaites":   row["defaites"]  or 0,
            "buts_moy":   round(row["buts_pour"]   / nb, 2) if nb > 0 else "?",
            "buts_enc":   round(row["buts_contre"] / nb, 2) if nb > 0 else "?",
            "buts_dom":   "?", "buts_enc_dom": "?",
            "buts_ext":   "?", "buts_enc_ext": "?",
            "equipe_id":  row["equipe_id"],
        }
        # Stats dom/ext (colonnes optionnelles — présentes après migration bootstrap)
        try:
            c.execute("""
                SELECT cl.buts_dom, cl.buts_enc_dom, cl.matchs_dom,
                       cl.buts_ext, cl.buts_enc_ext, cl.matchs_ext
                FROM classements cl
                JOIN api_equipes e ON cl.equipe_id = e.id
                WHERE cl.ligue_id = ? AND e.nom LIKE ?
                LIMIT 1
            """, (ligue_id, f"%{equipe_nom[:10]}%"))
            r2 = c.fetchone()
            if r2:
                md = r2["matchs_dom"] or 1
                me = r2["matchs_ext"] or 1
                result["buts_dom"]     = round((r2["buts_dom"]     or 0) / md, 2)
                result["buts_enc_dom"] = round((r2["buts_enc_dom"] or 0) / md, 2)
                result["buts_ext"]     = round((r2["buts_ext"]     or 0) / me, 2)
                result["buts_enc_ext"] = round((r2["buts_enc_ext"] or 0) / me, 2)
        except Exception:
            pass
        return result
    except Exception:
        pass
    return _default


def _get_top_buteurs(c, equipe_id, n=3):
    """Top n buteurs d'une équipe : nom, buts saison, buts sur 5 derniers matchs."""
    if equipe_id is None:
        return []
    try:
        c.execute("""
            SELECT aj.id, aj.nom, aj.buts, aj.matchs
            FROM api_joueurs aj
            WHERE aj.equipe_id = ? AND aj.poste = 'Attacker' AND aj.buts > 0
            ORDER BY aj.buts DESC
            LIMIT ?
        """, (equipe_id, n))
        joueurs = [dict(row) for row in c.fetchall()]
        for j in joueurs:
            try:
                c.execute("""
                    SELECT SUM(buts) AS buts_5m FROM (
                        SELECT buts FROM joueurs_forme
                        WHERE joueur_id = ?
                        ORDER BY date DESC
                        LIMIT 5
                    )
                """, (j["id"],))
                row = c.fetchone()
                j["buts_5m"] = (row["buts_5m"] or 0) if row else 0
            except Exception:
                j["buts_5m"] = 0
        return joueurs
    except Exception:
        return []


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


def _normaliser(s: str) -> str:
    """Minuscules + supprime accents + strip pour comparaison floue."""
    s = s.lower().strip()
    s = unicodedata.normalize("NFD", s)
    return "".join(c for c in s if unicodedata.category(c) != "Mn")


def _get_cote_winamax(conn, match: str, type_pari: str, today: str) -> float | None:
    """
    Cherche la cote pour ce match via fuzzy matching sur home/away.
    Utilise rapidfuzz.fuzz.partial_ratio >= 75 comme seuil.
    """
    col = _type_pari_to_col(type_pari)
    if not col:
        return None

    parts = [p.strip() for p in match.split(" vs ", 1)]
    if len(parts) != 2:
        return None
    home_q, away_q = _normaliser(parts[0]), _normaliser(parts[1])

    ph = _ph(conn)
    try:
        from rapidfuzz import fuzz
        cur = conn.cursor()
        cur.execute(
            f"SELECT home, away, {col} FROM cotes_winamax WHERE date = {ph}",
            (today,),
        )
        rows = cur.fetchall()

        best_score = 0
        best_val   = None
        for row in rows:
            if isinstance(row, dict):
                r_home, r_away, r_val = row["home"], row["away"], row[col]
            else:
                r_home, r_away, r_val = row[0], row[1], row[2]

            score = (
                fuzz.partial_ratio(home_q, _normaliser(r_home))
                + fuzz.partial_ratio(away_q, _normaliser(r_away))
            ) / 2

            if score > best_score:
                best_score = score
                best_val   = r_val

        if best_score >= 75:
            if best_val is not None:
                print(f"[fuzzy] '{parts[0]} vs {parts[1]}' → score={best_score:.0f} col={col} val={best_val}")
                return float(best_val)
            return None  # match trouvé mais cote NULL pour ce type de pari

    except ImportError:
        # Fallback LIKE si rapidfuzz absent
        try:
            cur = conn.cursor()
            like = "ILIKE" if _is_pg(conn) else "LIKE"
            cur.execute(
                f"""SELECT {col} FROM cotes_winamax
                    WHERE date = {ph} AND home {like} {ph} AND away {like} {ph}
                    LIMIT 1""",
                (today, f"%{parts[0][:8]}%", f"%{parts[1][:8]}%"),
            )
            row = cur.fetchone()
            if row:
                val = row[col] if isinstance(row, dict) else row[0]
                return float(val) if val is not None else None
        except Exception:
            pass
    except Exception as e:
        print(f"[winamax] erreur lookup ({match}, {type_pari}): {e}")
    return None


def _categorie_depuis_proba(proba: int) -> str | None:
    """Classement simple sans contexte (utilisé pour les combis)."""
    if proba >= 80:
        return "safe"
    if proba >= 65:
        return "tentant"
    if proba >= 55:
        return "fun"
    return None


def _categorie_contextuelle(
    proba: int,
    rang_home, rang_away,
    forme_home: str, forme_away: str,
    pct_home: int, pct_away: int,
) -> str | None:
    """
    Classification SAFE/TENTANT/FUN avec contexte du match.

    SAFE    = proba >= 80% + au moins UNE condition :
              - écart classement >= 8 places
              - favori dans le top 5 de sa ligue
              - forme du favori >= 4W sur 5 derniers matchs
    TENTANT = proba >= 80% sans condition SAFE (ex: deux bas de tableau)
              OU proba 65-79%
    FUN     = proba 55-64%
    """
    if proba < 55:
        return None
    if proba < 65:
        return "fun"
    if proba < 80:
        return "tentant"

    # proba >= 80% : vérifier les conditions contextuelles
    fav_home = pct_home >= pct_away
    rang_fav  = rang_home if fav_home else rang_away
    rang_opp  = rang_away if fav_home else rang_home
    forme_fav = forme_home if fav_home else forme_away

    conditions = []

    # Condition 1 : écart de classement >= 8 places
    if isinstance(rang_fav, int) and isinstance(rang_opp, int):
        ecart = abs(rang_fav - rang_opp)
        if ecart >= 8:
            conditions.append(f"écart_rang={ecart}")

    # Condition 2 : favori dans le top 5 de sa ligue
    if isinstance(rang_fav, int) and rang_fav <= 5:
        conditions.append(f"top5 (rang={rang_fav})")

    # Condition 3 : forme du favori >= 4W sur les 5 derniers matchs
    if forme_fav and forme_fav != "?":
        last5 = (forme_fav[-5:] if len(forme_fav) >= 5 else forme_fav)
        if last5.count("W") >= 4:
            conditions.append(f"forme={last5}")

    cat = "safe" if conditions else "tentant"
    label = f"SAFE ({', '.join(conditions)})" if conditions else "TENTANT dégradé (proba>=80 sans contexte fort)"
    print(f"[categorie] proba={proba}% rang_fav={rang_fav} rang_opp={rang_opp} forme_fav={forme_fav} → {label}")
    return cat


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

    def _fmt(v):
        return str(v) if v is not None else "N/D"

    def _fmt_buteurs(joueurs):
        if not joueurs:
            return "?"
        return " | ".join(
            f"{j['nom']} ({j['buts']}G, {j['buts_5m']}/5M)" for j in joueurs
        )

    blocs = []
    for m in matchs[:30]:
        ph_dom  = m["pct_home"]
        ph_nul  = m["pct_nul"]
        ph_ext  = m["pct_away"]
        det_h   = m.get("_det_home", {})
        det_a   = m.get("_det_away", {})
        heure   = m.get("heure", "?")

        # Top buteurs
        top_h = _get_top_buteurs(c, det_h.get("equipe_id"))
        top_a = _get_top_buteurs(c, det_a.get("equipe_id"))

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
            vb_str     = "? (pas de cote Winamax)"

        # Écart de points
        pts_h = det_h.get("points")
        pts_a = det_a.get("points")
        if isinstance(pts_h, int) and isinstance(pts_a, int):
            diff = pts_h - pts_a
            ecart_pts = f"+{diff}" if diff > 0 else str(diff)
        else:
            ecart_pts = "?"

        blocs.append(
            f"{m['home']} vs {m['away']} ({m['ligue']}) — {heure}\n"
            f"  Probas HiddenScout : dom={ph_dom}% nul={ph_nul}% ext={ph_ext}%\n"
            f"  Classement : {det_h.get('rang', '?')}ème vs {det_a.get('rang', '?')}ème"
            f" | Points : {pts_h} vs {pts_a} (écart : {ecart_pts})\n"
            f"  MJ/V/N/D dom : {det_h.get('mj','?')}/{det_h.get('victoires','?')}/{det_h.get('nuls','?')}/{det_h.get('defaites','?')}"
            f" | ext : {det_a.get('mj','?')}/{det_a.get('victoires','?')}/{det_a.get('nuls','?')}/{det_a.get('defaites','?')}\n"
            f"  Forme dom (5J) : {det_h.get('forme','?')} (série {det_h.get('serie_wins',0)}V)"
            f" | Forme ext (5J) : {det_a.get('forme','?')} (série {det_a.get('serie_wins',0)}V)\n"
            f"  Stats domicile : {det_h.get('buts_dom','?')} buts/m marqués, {det_h.get('buts_enc_dom','?')} encaissés\n"
            f"  Stats extérieur : {det_a.get('buts_ext','?')} buts/m marqués, {det_a.get('buts_enc_ext','?')} encaissés\n"
            f"  Buts/match global : dom={det_h.get('buts_moy','?')} enc={det_h.get('buts_enc','?')}"
            f" | ext={det_a.get('buts_moy','?')} enc={det_a.get('buts_enc','?')}\n"
            f"  Top 3 buteurs dom : {_fmt_buteurs(top_h)}\n"
            f"  Top 3 buteurs ext : {_fmt_buteurs(top_a)}\n"
            f"  Info cotes Winamax : 1={_fmt(cw.get('cote_1'))} X={_fmt(cw.get('cote_x'))}"
            f" 2={_fmt(cw.get('cote_2'))} 1X={_fmt(cw.get('cote_1x'))}"
            f" X2={_fmt(cw.get('cote_x2'))} +2.5={_fmt(cw.get('cote_plus25'))}"
            f" BTTS={_fmt(cw.get('cote_btts_oui'))}\n"
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
1. Classification contextuelle des paris :
   - SAFE    : proba >= 80% ET au moins une condition forte :
               * Écart classement >= 8 places entre les deux équipes
               * Équipe favorite dans le top 5 de sa ligue
               * Forme du favori >= 4 victoires sur 5 derniers matchs
               * Série de victoires consécutives >= 4
   - TENTANT : proba >= 80% SANS condition forte (ex: deux équipes de bas de tableau)
               OU proba 65-79%
   - FUN     : proba 55-64%
   - Ignorer < 55%
   IMPORTANT : Si les deux équipes sont dans la moitié basse du classement,
   classer maximum TENTANT même si proba >= 80%.

2. Base ta décision PRINCIPALEMENT sur les statistiques :
   forme récente, série victoires, classement, écart de points, stats dom/ext, buts/match.
   Les cotes Winamax sont une information complémentaire.
   Un pari peut être proposé même sans cote disponible (noter "N/D" pour la cote).
   PRIORISE les value bets quand HiddenScout > cote implicite Winamax.

3. Maximum 2 paris par match, maximum 2 buteurs par match

4. Types autorisés :
   Victoire domicile / Victoire extérieure / Nul /
   Double chance 1X / Double chance X2 / Double chance 12 /
   Plus de 2.5 buts / Moins de 2.5 buts /
   Les deux équipes marquent oui / Les deux équipes marquent non

5. Dans le raisonnement, croiser OBLIGATOIREMENT :
   - La proba Poisson
   - La forme récente + série victoires
   - Le classement / écart de points si disponible
   - Les stats dom/ext si pertinentes (ex: équipe forte à domicile mais faible à l'ext)
   - Le value bet si applicable

6. Diversifie les matchs — ne concentre pas tous les paris sur 1-2 matchs

7. Si aucun pari >= 55% → "paris": []

8. Raisonnement : 1-2 phrases MAX par pari (sois concis, pas de répétition).

9. LIMITES PAR CATÉGORIE (maximums stricts) :
   - SAFE    : max 10 paris
   - TENTANT : max 5 paris
   - FUN     : max 5 paris
   - Total   : max 20 paris
   QUALITÉ AVANT QUANTITÉ : ne jamais forcer un pari pour atteindre un quota.
   Si seulement 3 paris méritent d'être proposés → retourne 3 paris, pas 20.
   Chaque pari doit être justifié par des données solides.

Réponds UNIQUEMENT en JSON valide sans markdown :
{_PROMPT_JSON_SCHEMA}"""

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

    # Index pour retrouver le contexte (rang, forme) de chaque match depuis les données calculées
    matchs_ctx: dict = {}
    for m in matchs:
        key = _normaliser(m["home"]) + "|" + _normaliser(m["away"])
        matchs_ctx[key] = m

    def _ctx_pour_match(match_str: str) -> dict:
        """Retourne le dict match enrichi (rang, forme, pct) ou {} si non trouvé."""
        parts = match_str.split(" vs ", 1)
        if len(parts) != 2:
            return {}
        key = _normaliser(parts[0]) + "|" + _normaliser(parts[1])
        m = matchs_ctx.get(key)
        if m:
            return m
        # Fallback : recherche partielle (noms légèrement différents)
        k0, k1 = _normaliser(parts[0]), _normaliser(parts[1])
        for mk, mv in matchs_ctx.items():
            h, a = mk.split("|", 1)
            if k0[:8] in h and k1[:8] in a:
                return mv
        return {}

    # Forcer la catégorie depuis probabilite_hiddenscout + contexte (ignore ce que Claude dit)
    paris_valides = []
    paris_par_match = defaultdict(int)
    buteurs_par_match = defaultdict(int)
    seen_match_type: set = set()  # (match_key, type_pari) — anti-doublon strict

    # Trier par probabilite_hiddenscout décroissante
    paris_bruts.sort(key=lambda p: int(p.get("probabilite_hiddenscout", 0) or 0), reverse=True)

    for p in paris_bruts:
        try:
            proba_hs = int(p.get("probabilite_hiddenscout", 0) or 0)
        except (TypeError, ValueError):
            proba_hs = 0

        # Classification contextuelle : rang + forme du favori
        ctx = _ctx_pour_match(p.get("match", ""))
        if ctx:
            det_h = ctx.get("_det_home", {})
            det_a = ctx.get("_det_away", {})
            cat = _categorie_contextuelle(
                proba_hs,
                rang_home=det_h.get("rang") if isinstance(det_h.get("rang"), int) else None,
                rang_away=det_a.get("rang") if isinstance(det_a.get("rang"), int) else None,
                forme_home=det_h.get("forme", ""),
                forme_away=det_a.get("forme", ""),
                pct_home=ctx.get("pct_home", 0),
                pct_away=ctx.get("pct_away", 0),
            )
        else:
            cat = _categorie_depuis_proba(proba_hs)

        if cat is None:
            continue  # < 55%, on ignore

        match_key  = p.get("match", "").strip().lower()
        type_lower = (p.get("type_pari", "") or "").lower()

        # Refuser si même (match, type_pari) déjà présent — ex: 2× "victoire domicile"
        dedup_key = (match_key, type_lower)
        if dedup_key in seen_match_type:
            continue

        # Max 2 paris par match (types différents autorisés)
        if paris_par_match[match_key] >= 2:
            continue

        # Max 2 buteurs par match
        is_buteur = "buteur" in type_lower or "marquer" in type_lower or "scorer" in type_lower
        if is_buteur and buteurs_par_match[match_key] >= 2:
            continue

        p["categorie"] = cat
        paris_valides.append(p)
        paris_par_match[match_key] += 1
        seen_match_type.add(dedup_key)
        if is_buteur:
            buteurs_par_match[match_key] += 1

    # 5b. Enrichir avec les cotes Winamax réelles + filtre BTTS hors plage
    nb_cotes_trouvees = 0
    paris_valides_filtrés = []
    for p in paris_valides:
        cote_win = _get_cote_winamax(conn_pg, p.get("match", ""), p.get("type_pari", ""), today)
        if cote_win is not None:
            old = p.get("cote")
            p["cote"] = cote_win
            nb_cotes_trouvees += 1
            print(f"[winamax] {p['match']} | {p['type_pari']} → cote réelle={cote_win} (Claude={old})")
        else:
            print(f"[winamax] {p['match']} | {p['type_pari']} → cote Winamax introuvable, cote Claude conservée")

        paris_valides_filtrés.append(p)

    paris_valides = paris_valides_filtrés
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
