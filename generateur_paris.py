"""
Génère les paris du jour via Claude API et les sauvegarde dans paris_jour.
"""
import os
import json
import sqlite3
import re
from datetime import date, datetime

import anthropic
import requests as req

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
API_SPORTS_KEY = os.environ.get("API_SPORTS_KEY", "")

# Ligues suivies (fallback si api_ligues est vide)
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


def _get_matchs_depuis_predictions(c, today):
    """Récupère les matchs déjà sauvegardés dans predictions."""
    try:
        c.execute("""
            SELECT home, away, ligue, pct_home, pct_nul, pct_away
            FROM predictions
            WHERE date = ? AND statut = 'en_attente'
            ORDER BY ligue, home
        """, (today,))
        return [dict(r) for r in c.fetchall()]
    except Exception:
        return []


def _get_matchs_depuis_api(c, today):
    """Fallback : récupère les matchs depuis l'API-Sports."""
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

    # Ligues suivies depuis la DB, fallback sur LIGUES_IDS
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
            "pct_home": 45,
            "pct_nul": 25,
            "pct_away": 30,
        })
    return matchs


def _extraire_json(raw: str) -> dict:
    """Extrait le JSON depuis la réponse Claude (ignore éventuels blocs markdown)."""
    # Retirer les blocs ```json ... ```
    raw = re.sub(r"```(?:json)?\s*", "", raw)
    raw = raw.replace("```", "").strip()
    # Trouver le premier '{' et le dernier '}'
    start = raw.find("{")
    end = raw.rfind("}") + 1
    if start == -1 or end == 0:
        raise ValueError("Aucun JSON trouvé dans la réponse Claude")
    return json.loads(raw[start:end])


def generer_paris() -> int:
    """
    Génère les paris du jour via Claude et les sauvegarde dans paris_jour.
    Retourne le nombre de paris insérés.
    """
    today = date.today().strftime("%Y-%m-%d")
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = get_db()
    c = conn.cursor()

    # Créer la table si besoin
    c.execute("""CREATE TABLE IF NOT EXISTS paris_jour (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT, categorie TEXT, match TEXT, ligue TEXT,
        type_pari TEXT, description TEXT, cote REAL,
        probabilite INTEGER, raisonnement TEXT, timestamp TEXT
    )""")
    conn.commit()

    # 1. Récupérer les matchs
    matchs = _get_matchs_depuis_predictions(c, today)
    if not matchs:
        matchs = _get_matchs_depuis_api(c, today)

    if not matchs:
        conn.close()
        return 0

    # 2. Construire le prompt
    lignes = []
    for m in matchs[:30]:  # max 30 matchs dans le prompt
        lignes.append(
            f"- {m['home']} vs {m['away']} ({m['ligue']}) "
            f"— Proba: {m['pct_home']}% dom / {m['pct_nul']}% nul / {m['pct_away']}% ext"
        )
    matchs_text = "\n".join(lignes)

    prompt = f"""Tu es un expert en paris sportifs football. Voici les matchs du {today} avec leurs probabilités calculées par le modèle Poisson HiddenScout :

{matchs_text}

Génère des paris sportifs réalistes classés en 3 catégories :
- **safe** : cote 1.20–1.55, probabilité ≥ 75% (paris très sûrs)
- **cool** : cote 1.55–2.20, probabilité ≥ 60% (bon équilibre risque/rendement)
- **fun** : cote 2.20+, probabilité ≥ 50% (paris plus risqués mais intéressants)

Types de paris autorisés : victoire domicile, victoire extérieur, nul, les deux équipes marquent (oui/non), plus/moins de 2.5 buts, double chance.

Réponds UNIQUEMENT en JSON valide, sans markdown, sans texte avant ou après :
{{
  "resume": "Résumé en 1-2 phrases de la journée de paris",
  "paris": [
    {{
      "categorie": "safe|cool|fun",
      "match": "Equipe A vs Equipe B",
      "ligue": "Nom de la ligue",
      "type_pari": "Type de pari concis",
      "description": "Description courte du pari",
      "cote": 1.45,
      "probabilite": 78,
      "raisonnement": "Justification en 1-2 phrases"
    }}
  ]
}}

Génère entre 4 et 9 paris au total, bien répartis entre les catégories. La cote doit être cohérente avec la catégorie et la probabilité."""

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

    # 5. Effacer les paris existants du jour et réinsérer
    c.execute("DELETE FROM paris_jour WHERE date = ?", (today,))

    resume = parsed.get("resume", "")
    if resume:
        c.execute(
            """INSERT INTO paris_jour
               (date, categorie, match, ligue, type_pari, description, cote, probabilite, raisonnement, timestamp)
               VALUES (?, 'resume', '', '', '', ?, 0, 0, '', ?)""",
            (today, resume, now_ts),
        )

    count = 0
    for p in parsed.get("paris", []):
        cat = p.get("categorie", "cool")
        if cat not in ("safe", "cool", "fun"):
            cat = "cool"
        try:
            cote = float(p.get("cote", 1.5))
            proba = int(p.get("probabilite", 60))
        except (TypeError, ValueError):
            cote, proba = 1.5, 60

        c.execute(
            """INSERT INTO paris_jour
               (date, categorie, match, ligue, type_pari, description, cote, probabilite, raisonnement, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                today, cat,
                p.get("match", ""), p.get("ligue", ""),
                p.get("type_pari", ""), p.get("description", ""),
                cote, proba,
                p.get("raisonnement", ""), now_ts,
            ),
        )
        count += 1

    conn.commit()
    conn.close()
    return count
