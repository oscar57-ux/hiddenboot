from flask import Flask, render_template, jsonify
from datetime import date
import os
from datetime import date, datetime
import sqlite3

app = Flask(__name__)

DRAPEAUX_LIGUES = {
    61: "fr", 62: "fr",
    39: "gb-eng", 40: "gb-eng",
    140: "es", 141: "es",
    135: "it", 136: "it",
    78: "de", 79: "de",
    94: "pt", 95: "pt",
    88: "nl", 89: "nl",
    144: "be",
    203: "tr", 204: "tr",
    235: "ru", 236: "ru",
    106: "pl", 107: "pl",
    286: "rs",
    283: "ro",
    197: "gr",
    210: "hr",
    218: "at",
    207: "ch",
    179: "gb-sct",
    71: "br", 72: "br",
    128: "ar", 131: "ar",
    239: "co",
    265: "cl",
    268: "uy",
}

def get_db():
    conn = sqlite3.connect("botfoot.db")
    conn.row_factory = sqlite3.Row
    return conn

def calculer_score_joueur(buts, passes, note, matchs, joueur_id=None):
    ratio = buts / matchs if matchs > 0 else 0
    bonus_titulaire = 3 if matchs >= 15 else 0
    score_base = (buts * 2) + (passes * 1) + (note * 5) + (ratio * 4)
    score_forme = 0
    if joueur_id:
        conn = get_db()
        c = conn.cursor()
        try:
            c.execute("""
                SELECT buts, passes, note
                FROM joueurs_forme
                WHERE joueur_id = ?
                ORDER BY date DESC
                LIMIT 5
            """, (joueur_id,))
            derniers = c.fetchall()
            if derniers:
                buts_recents = sum(r["buts"] for r in derniers)
                passes_recentes = sum(r["passes"] for r in derniers)
                note_recente = sum(r["note"] for r in derniers) / len(derniers)
                ratio_recent = buts_recents / len(derniers)
                score_forme = (buts_recents * 2) + (passes_recentes * 1) + (note_recente * 3) + (ratio_recent * 7)
        except:
            pass
        conn.close()
    return round(score_base + score_forme + bonus_titulaire, 2)

def calculer_pourcentage_victoire(stats_home, stats_away):
    if not stats_home or not stats_away:
        return 50, 50
    conv = {"W": 3, "D": 1, "L": 0}
    forme_home = sum(conv.get(f, 0) for f in stats_home["forme_raw"])
    forme_away = sum(conv.get(f, 0) for f in stats_away["forme_raw"])
    total_forme = forme_home + forme_away
    score_forme_home = (forme_home / total_forme * 100) if total_forme > 0 else 50
    score_forme_away = (forme_away / total_forme * 100) if total_forme > 0 else 50
    max_rang = max(stats_home["rang"], stats_away["rang"])
    rang_score_home = max_rang - stats_home["rang"] + 1
    rang_score_away = max_rang - stats_away["rang"] + 1
    total_rang = rang_score_home + rang_score_away
    score_rang_home = (rang_score_home / total_rang * 100) if total_rang > 0 else 50
    score_rang_away = (rang_score_away / total_rang * 100) if total_rang > 0 else 50
    diff_home = stats_home["diff_buts"]
    diff_away = stats_away["diff_buts"]
    diff_total = abs(diff_home) + abs(diff_away)
    if diff_total > 0:
        score_diff_home = ((diff_home + diff_total) / (diff_total * 2)) * 100
        score_diff_away = ((diff_away + diff_total) / (diff_total * 2)) * 100
    else:
        score_diff_home = score_diff_away = 50
    score_home = (score_forme_home * 0.50) + (score_rang_home * 0.35) + (score_diff_home * 0.15)
    score_away = (score_forme_away * 0.50) + (score_rang_away * 0.35) + (score_diff_away * 0.15)
    score_home *= 1.08
    total = score_home + score_away
    pct_home = round((score_home / total) * 100)
    pct_away = 100 - pct_home
    return pct_home, pct_away

@app.route("/")
def dashboard():
    conn = get_db()
    c = conn.cursor()
    ligues_data = {}
    c.execute("SELECT id, nom FROM api_ligues")
    api_ligues = c.fetchall()
    for ligue in api_ligues:
        ligue_id = ligue["id"]
        ligue_nom = ligue["nom"]
        c.execute("""
            SELECT ae.nom, cl.rang, cl.points, cl.forme
            FROM classements cl
            JOIN api_equipes ae ON cl.equipe_id = ae.id
            WHERE cl.ligue_id = ?
            ORDER BY cl.rang ASC
        """, (ligue_id,))
        classement_rows = c.fetchall()
        if not classement_rows:
            continue
        classement = []
        for row in classement_rows:
            forme_raw = row["forme"] or ""
            conversion = {"W": "V", "D": "N", "L": "D"}
            forme = [conversion.get(f, "vide") for f in forme_raw[-5:]]
            while len(forme) < 5:
                forme.insert(0, "vide")
            classement.append({"nom": row["nom"], "rang": row["rang"], "points": row["points"], "forme": forme})
        forme_list = [{"nom": r["nom"], "score": r["points"]} for r in classement_rows]
        forme_list.sort(key=lambda x: x["score"], reverse=True)
        ligues_data[ligue_nom] = {"classement": classement, "forme": forme_list, "drapeau": DRAPEAUX_LIGUES.get(ligue_id, "")}
    conn.close()
    return render_template("dashboard.html", ligues=ligues_data)

@app.route("/pepites")
def pepites():
    conn = get_db()
    c = conn.cursor()
    regions = {
        "🌍 Europe Ouest": [61, 62, 39, 40, 140, 141, 135, 136, 78, 79, 94, 95, 88, 89, 144, 207, 218, 179],
        "🌏 Europe Est": [235, 236, 106, 107, 286, 283, 197, 210, 203, 204],
        "🌎 Amérique du Sud": [71, 72, 128, 131, 239, 265, 268]
    }
    resultats = {}
    for region, ligue_ids in regions.items():
        placeholders = ",".join("?" * len(ligue_ids))
        c.execute(f"""
            SELECT j.id as joueur_id, j.nom, j.nationalite, j.poste,
                   j.matchs, j.buts, j.passes, j.note, j.ratio,
                   e.nom as equipe, l.nom as ligue, l.id as ligue_id
            FROM api_joueurs j
            JOIN api_equipes e ON j.equipe_id = e.id
            JOIN api_ligues l ON j.ligue_id = l.id
            WHERE j.buts > 0 AND j.ligue_id IN ({placeholders})
            ORDER BY j.buts DESC
            LIMIT 100
        """, ligue_ids)
        rows = c.fetchall()
        joueurs = []
        for j in rows:
            score = calculer_score_joueur(j["buts"], j["passes"] or 0, j["note"] or 0, j["matchs"] or 1, j["joueur_id"])
            joueurs.append({**dict(j), "score": score, "drapeau": DRAPEAUX_LIGUES.get(j["ligue_id"], "")})
        joueurs.sort(key=lambda x: x["score"], reverse=True)
        resultats[region] = joueurs[:50]
    c.execute("""
        SELECT j.id as joueur_id, j.nom, j.nationalite, j.poste,
               j.matchs, j.buts, j.passes, j.note, j.ratio,
               e.nom as equipe, l.nom as ligue, l.id as ligue_id
        FROM api_joueurs j
        JOIN api_equipes e ON j.equipe_id = e.id
        JOIN api_ligues l ON j.ligue_id = l.id
        WHERE j.buts > 0
        ORDER BY j.buts DESC
        LIMIT 200
    """)
    rows = c.fetchall()
    joueurs_mondial = []
    for j in rows:
        score = calculer_score_joueur(j["buts"], j["passes"] or 0, j["note"] or 0, j["matchs"] or 1, j["joueur_id"])
        joueurs_mondial.append({**dict(j), "score": score, "drapeau": DRAPEAUX_LIGUES.get(j["ligue_id"], "")})
    joueurs_mondial.sort(key=lambda x: x["score"], reverse=True)
    resultats["🌐 Top Mondial"] = joueurs_mondial[:50]
    conn.close()
    return render_template("pepites.html", regions=resultats)

@app.route("/matchs")
def matchs():
    return render_template("matchs.html")

@app.route("/api/matchs-jour")
def api_matchs_jour():
    import requests as req
    today = date.today().strftime("%Y-%m-%d")
    API_KEY = "f0841753cabc35b8ecca13ee835435d1"
    api_headers = {"x-apisports-key": API_KEY}
    response = req.get(
        "https://v3.football.api-sports.io/fixtures",
        headers=api_headers,
        params={"date": today, "timezone": "Europe/Paris"}
    )
    data = response.json()
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT id FROM api_ligues")
    ligues_suivies = set(row["id"] for row in c.fetchall())

    def get_stats_equipe(equipe_id):
        c.execute("SELECT rang, points, forme, diff_buts FROM classements WHERE equipe_id = ?", (equipe_id,))
        row = c.fetchone()
        if not row:
            return None
        return {
            "rang": row["rang"],
            "points": row["points"],
            "forme_raw": row["forme"][-5:] if row["forme"] else "",
            "diff_buts": row["diff_buts"] or 0,
        }

    ligues = {}
    for match in data.get("response", []):
        ligue_id = match["league"]["id"]
        if ligue_id not in ligues_suivies:
            continue
        ligue_nom = match["league"]["name"]
        home_id = match["teams"]["home"]["id"]
        away_id = match["teams"]["away"]["id"]
        stats_home = get_stats_equipe(home_id)
        stats_away = get_stats_equipe(away_id)
        pct_home, pct_away = calculer_pourcentage_victoire(stats_home, stats_away)
        heure = match["fixture"]["date"][11:16]
        statut = match["fixture"]["status"]["short"]
        est_live = statut in ["1H", "2H", "HT", "ET", "P"]
        if ligue_nom not in ligues:
            ligues[ligue_nom] = []
        ligues[ligue_nom].append({
            "home": match["teams"]["home"]["name"],
            "away": match["teams"]["away"]["name"],
            "home_id": home_id,
            "away_id": away_id,
            "heure": heure,
            "statut": statut,
            "est_live": est_live,
            "goals_home": match["goals"]["home"],
            "goals_away": match["goals"]["away"],
            "pct_home": pct_home,
            "pct_away": pct_away,
            "rang_home": stats_home["rang"] if stats_home else "?",
            "rang_away": stats_away["rang"] if stats_away else "?",
            "forme_home": stats_home["forme_raw"] if stats_home else "",
            "forme_away": stats_away["forme_raw"] if stats_away else "",
            "drapeau": DRAPEAUX_LIGUES.get(ligue_id, ""),
        })
    conn.close()
    return jsonify({"ligues": ligues, "date": today})

@app.route("/analyse")
def analyse():
    import json
    conn = get_db()
    c = conn.cursor()

    # Top équipes forme positive
    c.execute("""
        SELECT ae.nom, cl.points as score_total, cl.ligue_id
        FROM classements cl
        JOIN api_equipes ae ON cl.equipe_id = ae.id
        ORDER BY cl.points DESC
        LIMIT 50
    """)
    equipes = c.fetchall()

    # Pires formes — scores négatifs FlashScore
    c.execute("""
        SELECT e.nom, s.score_total, eq.ligue_id
        FROM scores s
        JOIN equipes e ON s.equipe_id = e.id
        JOIN api_equipes eq ON eq.nom = e.nom
        WHERE s.score_total < 0
        ORDER BY s.score_total ASC
        LIMIT 5
    """)
    equipes_pires = c.fetchall()

    # Si pas assez de données FlashScore fallback
    if not equipes_pires:
        c.execute("""
            SELECT ae.nom, cl.points as score_total, cl.ligue_id
            FROM classements cl
            JOIN api_equipes ae ON cl.equipe_id = ae.id
            ORDER BY cl.points ASC
            LIMIT 5
        """)
        equipes_pires = c.fetchall()

    # Top buteurs
    c.execute("""
        SELECT j.id as joueur_id, j.nom, e.nom as equipe, j.buts, j.matchs, j.passes, j.note
        FROM api_joueurs j
        JOIN api_equipes e ON j.equipe_id = e.id
        WHERE j.buts > 0
        ORDER BY j.buts DESC
        LIMIT 5
    """)
    buteurs_rows = c.fetchall()
    buteurs = []
    for b in buteurs_rows:
        score = calculer_score_joueur(b["buts"], b["passes"] or 0, b["note"] or 0, b["matchs"] or 1, b["joueur_id"])
        buteurs.append({"nom": b["nom"], "equipe": b["equipe"], "buts": b["buts"], "score": score})

    # Buts par ligue
    c.execute("""
        SELECT l.nom as ligue, l.id as ligue_id, SUM(j.buts) as total_buts
        FROM api_joueurs j
        JOIN api_ligues l ON j.ligue_id = l.id
        GROUP BY l.nom
        ORDER BY total_buts DESC
    """)
    ligues_buts = c.fetchall()

    conn.close()
    return render_template("analyse.html",
    equipes=equipes,
    equipes_pires=equipes_pires,
    buteurs=buteurs,  # Direct, pas JSON
    equipes_json=json.dumps([{"nom": e["nom"], "score_total": e["score_total"]} for e in equipes]),
    buteurs_json=json.dumps(buteurs),
    ligues_json=json.dumps([{"ligue": l["ligue"], "total_buts": l["total_buts"], "drapeau": DRAPEAUX_LIGUES.get(l["ligue_id"], "")} for l in ligues_buts])
)

@app.route("/api/equipe/<int:equipe_id>/buteurs")
def api_buteurs_equipe(equipe_id):
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT j.id as joueur_id, j.nom, j.poste, j.matchs,
               j.buts, j.passes, j.note, j.ratio
        FROM api_joueurs j
        WHERE j.equipe_id = ? AND j.buts > 0
        ORDER BY j.buts DESC
        LIMIT 10
    """, (equipe_id,))
    joueurs = c.fetchall()
    result = []
    for j in joueurs:
        score = calculer_score_joueur(j["buts"], j["passes"] or 0, j["note"] or 0, j["matchs"] or 1, j["joueur_id"])
        ratio = j["buts"] / j["matchs"] if j["matchs"] > 0 else 0
        pct_but = min(95, round(ratio * 80))
        result.append({
            "joueur_id": j["joueur_id"],
            "nom": j["nom"],
            "poste": j["poste"],
            "matchs": j["matchs"],
            "buts": j["buts"],
            "passes": j["passes"] or 0,
            "note": round(float(j["note"]), 2) if j["note"] else 0,
            "score": score,
            "pct_but": pct_but
        })
    conn.close()
    return jsonify({"joueurs": result, "equipe_id": equipe_id})

@app.route("/resultats")
def resultats():
    return render_template("resultats.html")

@app.route("/api/sauvegarder-predictions")
def sauvegarder_predictions():
    """A appeler chaque jour avant les matchs pour sauvegarder les prédictions"""
    import requests as req
    today = date.today().strftime("%Y-%m-%d")
    API_KEY = "f0841753cabc35b8ecca13ee835435d1"
    api_headers = {"x-apisports-key": API_KEY}

    response = req.get(
        "https://v3.football.api-sports.io/fixtures",
        headers=api_headers,
        params={"date": today, "timezone": "Europe/Paris"}
    )
    data = response.json()

    conn = get_db()
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS predictions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fixture_id INTEGER UNIQUE,
        date TEXT,
        ligue TEXT,
        ligue_id INTEGER,
        home TEXT,
        away TEXT,
        pct_home INTEGER,
        pct_away INTEGER,
        score_home INTEGER,
        score_away INTEGER,
        statut TEXT DEFAULT "en_attente",
        prediction_correcte INTEGER DEFAULT NULL,
        date_maj TEXT
    )''')

    c.execute("SELECT id FROM api_ligues")
    ligues_suivies = set(row["id"] for row in c.fetchall())

    def get_stats_equipe(equipe_id):
        c.execute("SELECT rang, points, forme, diff_buts FROM classements WHERE equipe_id = ?", (equipe_id,))
        row = c.fetchone()
        if not row:
            return None
        return {
            "rang": row["rang"],
            "points": row["points"],
            "forme_raw": row["forme"][-5:] if row["forme"] else "",
            "diff_buts": row["diff_buts"] or 0,
        }

    total = 0
    for match in data.get("response", []):
        ligue_id = match["league"]["id"]
        if ligue_id not in ligues_suivies:
            continue

        home_id = match["teams"]["home"]["id"]
        away_id = match["teams"]["away"]["id"]
        stats_home = get_stats_equipe(home_id)
        stats_away = get_stats_equipe(away_id)
        pct_home, pct_away = calculer_pourcentage_victoire(stats_home, stats_away)
        fixture_id = match["fixture"]["id"]

        try:
            c.execute('''INSERT OR IGNORE INTO predictions
                (fixture_id, date, ligue, ligue_id, home, away, pct_home, pct_away, statut, date_maj)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, "en_attente", ?)''',
                (fixture_id, today, match["league"]["name"], ligue_id,
                 match["teams"]["home"]["name"], match["teams"]["away"]["name"],
                 pct_home, pct_away, datetime.now().strftime("%Y-%m-%d %H:%M")))
            total += 1
        except:
            pass

    conn.commit()
    conn.close()
    return jsonify({"sauvegarde": total, "date": today})

@app.route("/api/verifier-resultats")
def verifier_resultats():
    """A appeler chaque matin pour vérifier les résultats de la veille"""
    import requests as req
    from datetime import datetime, timedelta

    hier = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    API_KEY = "f0841753cabc35b8ecca13ee835435d1"
    api_headers = {"x-apisports-key": API_KEY}

    response = req.get(
        "https://v3.football.api-sports.io/fixtures",
        headers=api_headers,
        params={"date": hier, "timezone": "Europe/Paris"}
    )
    data = response.json()

    conn = get_db()
    c = conn.cursor()
    total_verifie = 0
    total_correct = 0

    for match in data.get("response", []):
        fixture_id = match["fixture"]["id"]
        statut = match["fixture"]["status"]["short"]

        if statut != "FT":
            continue

        goals_home = match["goals"]["home"] or 0
        goals_away = match["goals"]["away"] or 0

        # Vrai vainqueur
        if goals_home > goals_away:
            vrai_vainqueur = "home"
        elif goals_away > goals_home:
            vrai_vainqueur = "away"
        else:
            vrai_vainqueur = "nul"

        # Récupérer notre prédiction
        c.execute("SELECT pct_home, pct_away FROM predictions WHERE fixture_id = ?", (fixture_id,))
        pred = c.fetchone()

        if not pred:
            continue

        # Notre prédiction
        if pred["pct_home"] > pred["pct_away"]:
            notre_prediction = "home"
        elif pred["pct_away"] > pred["pct_home"]:
            notre_prediction = "away"
        else:
            notre_prediction = "nul"

        correct = 1 if notre_prediction == vrai_vainqueur else 0
        total_verifie += 1
        total_correct += correct

        c.execute('''UPDATE predictions SET
            score_home = ?,
            score_away = ?,
            statut = "termine",
            prediction_correcte = ?,
            date_maj = ?
            WHERE fixture_id = ?''',
            (goals_home, goals_away, correct,
             datetime.now().strftime("%Y-%m-%d %H:%M"), fixture_id))

    conn.commit()
    conn.close()

    precision = round((total_correct / total_verifie * 100)) if total_verifie > 0 else 0
    return jsonify({
        "verifie": total_verifie,
        "correct": total_correct,
        "precision": precision,
        "date": hier
    })

@app.route("/api/historique-predictions")
def historique_predictions():
    conn = get_db()
    c = conn.cursor()

    # Stats globales
    c.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN prediction_correcte = 1 THEN 1 ELSE 0 END) as correct,
            SUM(CASE WHEN statut = 'en_attente' THEN 1 ELSE 0 END) as en_attente
        FROM predictions
    """)
    stats = c.fetchone()

    # Précision par ligue
    c.execute("""
        SELECT ligue, ligue_id,
               COUNT(*) as total,
               SUM(CASE WHEN prediction_correcte = 1 THEN 1 ELSE 0 END) as correct
        FROM predictions
        WHERE statut = 'termine'
        GROUP BY ligue
        ORDER BY correct * 100 / COUNT(*) DESC
    """)
    par_ligue = c.fetchall()

    # Derniers matchs vérifiés
    c.execute("""
        SELECT * FROM predictions
        WHERE statut = 'termine'
        ORDER BY date DESC, fixture_id DESC
        LIMIT 50
    """)
    derniers = c.fetchall()

    # Matchs en attente (aujourd'hui)
    c.execute("""
        SELECT * FROM predictions
        WHERE statut = 'en_attente'
        ORDER BY date DESC
        LIMIT 50
    """)
    en_attente = c.fetchall()

    conn.close()

    precision_globale = round((stats["correct"] / stats["total"] * 100)) if stats["total"] and stats["correct"] else 0

    return jsonify({
        "stats": {
            "total": stats["total"] or 0,
            "correct": stats["correct"] or 0,
            "precision": precision_globale,
            "en_attente": stats["en_attente"] or 0
        },
        "par_ligue": [{"ligue": r["ligue"], "ligue_id": r["ligue_id"], "total": r["total"], "correct": r["correct"] or 0, "precision": round((r["correct"] or 0) / r["total"] * 100)} for r in par_ligue],
        "derniers": [dict(r) for r in derniers],
        "en_attente": [dict(r) for r in en_attente]
    })

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))