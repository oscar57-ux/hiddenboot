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


def _extraire_json(raw: str) -> dict:
    raw = re.sub(r"```(?:json)?\s*", "", raw)
    raw = raw.replace("```", "").strip()
    start = raw.find("{")
    end = raw.rfind("}") + 1
    if start == -1 or end == 0:
        raise ValueError("Aucun JSON trouvé dans la réponse Claude")
    return json.loads(raw[start:end])


def _get_rang_equipe(c, equipe_nom, ligue_id):
    """Retourne le rang (1=leader) de l'équipe dans sa ligue via SQLite classements.
    Retourne None si aucune donnée disponible."""
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

    # 1. Matchs — d'abord depuis predictions (PG), sinon depuis l'API (SQLite pour les ligues)
    matchs = _get_matchs_depuis_predictions(c_pg, ph, today)
    if not matchs:
        matchs = _get_matchs_depuis_api(c, today)
    if not matchs:
        conn.close()
        conn_pg.close()
        return 0

    # 1.5. Ajustement classement — multiplicateur sur la proba de l'équipe mieux classée
    # Seuils : écart >= 10 → ×1.20 | >= 15 → ×1.35 | >= 18 → ×1.50
    # Si classement inconnu pour une équipe → neutre (×1.0)
    print(f"[classement] ajustement sur {len(matchs)} matchs…")
    for m in matchs:
        rang_h = _get_rang_equipe(c, m["home"], m["ligue_id"])
        rang_a = _get_rang_equipe(c, m["away"], m["ligue_id"])
        m["pct_home"], m["pct_nul"], m["pct_away"] = _appliquer_multiplicateur_classement(
            m["pct_home"], m["pct_nul"], m["pct_away"],
            rang_h, rang_a,
            home_nom=m["home"], away_nom=m["away"],
        )

    # 2. Construire le prompt — probabilité HiddenScout bien visible
    lignes = []
    for m in matchs[:30]:
        proba_dom = m["pct_home"]
        proba_ext = m["pct_away"]
        proba_nul = m["pct_nul"]
        # Déterminer le favori clair
        max_proba = max(proba_dom, proba_ext, proba_nul)
        favori = ""
        if proba_dom == max_proba:
            favori = f"Favori domicile ({proba_dom}%)"
        elif proba_ext == max_proba:
            favori = f"Favori extérieur ({proba_ext}%)"
        else:
            favori = f"Nul probable ({proba_nul}%)"
        lignes.append(
            f"- {m['home']} vs {m['away']} ({m['ligue']}) | "
            f"Poisson: Dom={proba_dom}% Nul={proba_nul}% Ext={proba_ext}% | {favori}"
        )
    matchs_text = "\n".join(lignes)

    prompt = f"""Tu es un analyste sportif expert. Tu reçois les probabilités calculées par le modèle Poisson/Monte Carlo HiddenScout pour les matchs du {today}.

MATCHS DU JOUR :
{matchs_text}

RÈGLES STRICTES À RESPECTER :
1. Catégories basées UNIQUEMENT sur la probabilité HiddenScout (pas la cote) :
   - SAFE    : probabilité_hiddenscout >= 80%
   - TENTANT : probabilité_hiddenscout 65-79%
   - FUN     : probabilité_hiddenscout 55-64%
   - Ignore TOUT pari dont la probabilité serait < 55%
2. Maximum 2 paris par match (tous types confondus)
3. Maximum 2 buteurs par match
4. Types autorisés : victoire domicile, victoire extérieure, nul, double chance (1X/X2/12), plus de 2.5 buts, moins de 2.5 buts, les deux équipes marquent (oui/non)
5. Classe par probabilité_hiddenscout décroissante dans chaque catégorie
6. Sélectionne les paris les plus solides selon nos probabilités, pas selon la cote
7. Si aucun pari n'atteint 55%, réponds avec "paris": []
8. Diversifie les matchs — ne concentre pas tous les paris sur 1-2 matchs

Réponds UNIQUEMENT en JSON valide, sans markdown, sans texte avant ou après :
{{
  "resume": "Résumé en 1 phrase de la journée de paris",
  "paris": [
    {{
      "categorie": "safe|tentant|fun",
      "match": "Equipe A vs Equipe B",
      "ligue": "Nom de la ligue",
      "type_pari": "Type de pari concis (ex: Victoire domicile, Plus de 2.5 buts)",
      "description": "Description courte et précise du pari",
      "probabilite_hiddenscout": 82,
      "cote": 1.45,
      "raisonnement": "Justification en 1 phrase basée sur les stats Poisson"
    }}
  ]
}}

Génère entre 4 et 10 paris bien répartis entre les catégories disponibles."""

    # 3. Appeler Claude
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = message.content[0].text.strip()

    # 4. Parser le JSON
    parsed = _extraire_json(raw)

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

    count = 0
    for p in paris_valides:
        try:
            cote = float(p.get("cote", 1.5))
        except (TypeError, ValueError):
            cote = 1.5
        proba_hs = int(p.get("probabilite_hiddenscout", p.get("probabilite", 60)) or 60)

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
