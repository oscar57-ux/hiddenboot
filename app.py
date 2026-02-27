from flask import Flask, render_template, jsonify
from datetime import date
import os
import sqlite3

app = Flask(__name__)

# ============================================================
# FORMULES HIDDENBOOT v2
# Score joueur = (buts x2) + (passes x1) + (note x5) + (ratio x8) + bonus titulaire
# Score equipe = forme 50% + classement 35% + diff buts 15% + avantage domicile +8%
# ============================================================

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

def calculer_score_joueur(buts, passes, note, matchs):
    """Formule v2 : buts x2 + passes x1 + note x5 + ratio x8 + bonus titulaire"""
    ratio = buts / matchs if matchs > 0 else 0
    bonus_titulaire = 3 if matchs >= 15 else 0
    score = (buts * 2) + (passes * 1) + (note * 5) + (ratio * 8) + bonus_titulaire
    return round(score, 2)

def calculer_pourcentage_victoire(stats_home, stats_away):
    """Formule v2 : forme 50% + classement 35% + diff buts 15% + domicile +8%"""
    if not stats_home or not stats_away:
        return 50, 50

    # Forme 5 derniers matchs (max 15pts)
    conv = {"W": 3, "D": 1, "L": 0}
    forme_home = sum(conv.get(f, 0) for f in stats_home["forme_raw"])
    forme_away = sum(conv.get(f, 0) for f in stats_away["forme_raw"])

    # Normaliser forme sur 100
    total_forme = forme_home + forme_away
    score_forme_home = (forme_home / total_forme * 100) if total_forme > 0 else 50
    score_forme_away = (forme_away / total_forme * 100) if total_forme > 0 else 50

    # Classement (1er = meilleur)
    max_rang = max(stats_home["rang"], stats_away["rang"])
    rang_score_home = max_rang - stats_home["rang"] + 1
    rang_score_away = max_rang - stats_away["rang"] + 1
    total_rang = rang_score_home + rang_score_away
    score_rang_home = (rang_score_home / total_rang * 100) if total_rang > 0 else 50
    score_rang_away = (rang_score_away / total_rang * 100) if total_rang > 0 else 50

    # Diff buts
    diff_home = stats_home["diff_buts"]
    diff_away = stats_away["diff_buts"]
    diff_total = abs(diff_home) + abs(diff_away)
    if diff_total > 0:
        score_diff_home = ((diff_home + diff_total) / (diff_total * 2)) * 100
        score_diff_away = ((diff_away + diff_total) / (diff_total * 2)) * 100
    else:
        score_diff_home = score_diff_away = 50

    # Score final pondéré
    score_home = (score_forme_home * 0.50) + (score_rang_home * 0.35) + (score_diff_home * 0.15)
    score_away = (score_forme_away * 0.50) + (score_rang_away * 0.35) + (score_diff_away * 0.15)

    # Avantage domicile +8%
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
            classement.append({
                "nom": row["nom"],
                "rang": row["rang"],
                "points": row["points"],
                "forme": forme
            })

        # Score de forme basé sur classement API
        forme_list = [{"nom": r["nom"], "score": r["points"]} for r in classement_rows]
        forme_list.sort(key=lambda x: x["score"], reverse=True)

        ligues_data[ligue_nom] = {
            "classement": classement,
            "forme": forme_list,
            "drapeau": DRAPEAUX_LIGUES.get(ligue_id, "")
        }

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
            LIMIT 50
        """, ligue_ids)
        rows = c.fetchall()
        joueurs = []
        for j in rows:
            score = calculer_score_joueur(j["buts"], j["passes"] or 0, j["note"] or 0, j["matchs"] or 1)
            joueurs.append({**dict(j), "score": score, "drapeau": DRAPEAUX_LIGUES.get(j["ligue_id"], "")})
        joueurs.sort(key=lambda x: x["score"], reverse=True)
        resultats[region] = joueurs

    c.execute("""
        SELECT j.id as joueur_id, j.nom, j.nationalite, j.poste,
               j.matchs, j.buts, j.passes, j.note, j.ratio,
               e.nom as equipe, l.nom as ligue, l.id as ligue_id
        FROM api_joueurs j
        JOIN api_equipes e ON j.equipe_id = e.id
        JOIN api_ligues l ON j.ligue_id = l.id
        WHERE j.buts > 0
        ORDER BY j.buts DESC
        LIMIT 100
    """)
    rows = c.fetchall()
    joueurs_mondial = []
    for j in rows:
        score = calculer_score_joueur(j["buts"], j["passes"] or 0, j["note"] or 0, j["matchs"] or 1)
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
        c.execute("""
            SELECT rang, points, forme, diff_buts
            FROM classements
            WHERE equipe_id = ?
        """, (equipe_id,))
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

    c.execute("""
        SELECT ae.nom, cl.points as score_total
        FROM classements cl
        JOIN api_equipes ae ON cl.equipe_id = ae.id
        ORDER BY cl.points DESC
        LIMIT 50
    """)
    equipes = c.fetchall()

    c.execute("""
        SELECT j.nom, e.nom as equipe, j.buts, j.matchs, j.passes, j.note
        FROM api_joueurs j
        JOIN api_equipes e ON j.equipe_id = e.id
        WHERE j.buts > 0
        ORDER BY j.buts DESC
        LIMIT 5
    """)
    buteurs_rows = c.fetchall()
    buteurs = []
    for b in buteurs_rows:
        score = calculer_score_joueur(b["buts"], b["passes"] or 0, b["note"] or 0, b["matchs"] or 1)
        buteurs.append({"nom": b["nom"], "equipe": b["equipe"], "buts": b["buts"], "score": score})

    c.execute("""
        SELECT l.nom as ligue, SUM(j.buts) as total_buts
        FROM api_joueurs j
        JOIN api_ligues l ON j.ligue_id = l.id
        GROUP BY l.nom
        ORDER BY total_buts DESC
    """)
    ligues_buts = c.fetchall()

    conn.close()

    return render_template("analyse.html",
        equipes=equipes,
        equipes_json=json.dumps([{"nom": e["nom"], "score_total": e["score_total"]} for e in equipes]),
        buteurs_json=json.dumps(buteurs),
        ligues_json=json.dumps([{"ligue": l["ligue"], "total_buts": l["total_buts"]} for l in ligues_buts])
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
        score = calculer_score_joueur(j["buts"], j["passes"] or 0, j["note"] or 0, j["matchs"] or 1)
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

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))