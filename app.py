from flask import Flask, render_template, jsonify
from datetime import date
import os
import sqlite3

app = Flask(__name__)

def get_db():
    conn = sqlite3.connect("botfoot.db")
    conn.row_factory = sqlite3.Row
    return conn

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

        c.execute("""
            SELECT ae.nom, s.score_total
            FROM scores s
            JOIN equipes e ON s.equipe_id = e.id
            JOIN api_equipes ae ON ae.nom = e.nom
            WHERE ae.ligue_id = ?
            ORDER BY s.score_total DESC
        """, (ligue_id,))
        forme_rows = c.fetchall()

        forme_list = [{"nom": r["nom"], "score": r["score_total"]} for r in forme_rows]

        if not forme_list:
            forme_list = [{"nom": r["nom"], "score": r["points"]} for r in classement_rows]

        ligues_data[ligue_nom] = {
            "classement": classement,
            "forme": forme_list
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
                   j.matchs, j.buts, j.passes, j.note, j.ratio, j.score,
                   e.nom as equipe, l.nom as ligue
            FROM api_joueurs j
            JOIN api_equipes e ON j.equipe_id = e.id
            JOIN api_ligues l ON j.ligue_id = l.id
            WHERE j.buts > 0 AND j.ligue_id IN ({placeholders})
            ORDER BY j.score DESC
            LIMIT 50
        """, ligue_ids)
        resultats[region] = c.fetchall()

    # Top global
    c.execute("""
        SELECT j.id as joueur_id, j.nom, j.nationalite, j.poste,
               j.matchs, j.buts, j.passes, j.note, j.ratio, j.score,
               e.nom as equipe, l.nom as ligue
        FROM api_joueurs j
        JOIN api_equipes e ON j.equipe_id = e.id
        JOIN api_ligues l ON j.ligue_id = l.id
        WHERE j.buts > 0
        ORDER BY j.score DESC
        LIMIT 50
    """)
    resultats["🌐 Top Mondial"] = c.fetchall()

    conn.close()
    return render_template("pepites.html", regions=resultats)
@app.route("/matchs")
def matchs():
    return render_template("matchs.html")



@app.route("/api/matchs-jour")
def api_matchs_jour():
    import requests as req
    from datetime import date

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

    # Récupérer toutes les ligues suivies
    c.execute("SELECT id FROM api_ligues")
    ligues_suivies = set(row["id"] for row in c.fetchall())

    def get_stats_equipe(equipe_id):
        # Classement
        c.execute("""
            SELECT rang, points, forme
            FROM classements
            WHERE equipe_id = ?
        """, (equipe_id,))
        row = c.fetchone()
        if not row:
            return None

        rang = row["rang"]
        forme_raw = row["forme"] or ""

        # Score forme 5 derniers matchs
        conversion = {"W": 3, "D": 1, "L": 0}
        forme_pts = sum(conversion.get(f, 0) for f in forme_raw[-5:])
        forme_max = 15  # 5 victoires = 15pts max

        # Score classement (inversé — 1er = meilleur)
        rang_score = max(0, 20 - rang)  # 1er = 19pts, 20ème = 0pt

        return {
            "rang": rang,
            "points": row["points"],
            "forme_raw": forme_raw[-5:],
            "forme_pts": forme_pts,
            "rang_score": rang_score,
            "total": forme_pts + rang_score
        }

    def calculer_pourcentage(stats_home, stats_away):
        if not stats_home or not stats_away:
            return 50, 50

        total = stats_home["total"] + stats_away["total"]
        if total == 0:
            return 50, 50

        pct_home = round((stats_home["total"] / total) * 100)
        pct_away = 100 - pct_home
        return pct_home, pct_away

    ligues = {}

    for match in data.get("response", []):
        ligue_id = match["league"]["id"]

        # Seulement nos ligues suivies
        if ligue_id not in ligues_suivies:
            continue

        ligue_nom = match["league"]["name"]
        home_id = match["teams"]["home"]["id"]
        away_id = match["teams"]["away"]["id"]

        stats_home = get_stats_equipe(home_id)
        stats_away = get_stats_equipe(away_id)

        pct_home, pct_away = calculer_pourcentage(stats_home, stats_away)

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
        })

    conn.close()
    return jsonify({"ligues": ligues, "date": today})

@app.route("/analyse")
def analyse():
    import json
    conn = get_db()
    c = conn.cursor()

    # Top équipes
    c.execute("""
        SELECT e.nom, s.score_total
        FROM scores s
        JOIN equipes e ON s.equipe_id = e.id
        ORDER BY s.score_total DESC
    """)
    equipes = c.fetchall()

    # Top buteurs
    c.execute("""
    SELECT j.nom, e.nom as equipe, j.buts, j.score
    FROM api_joueurs j
    JOIN api_equipes e ON j.equipe_id = e.id
    WHERE j.buts > 0
    ORDER BY j.buts DESC
    LIMIT 5
""")
    buteurs = c.fetchall()

    # Buts par ligue
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
        buteurs_json=json.dumps([{"nom": b["nom"], "equipe": b["equipe"], "buts": b["buts"]} for b in buteurs]),
        ligues_json=json.dumps([{"ligue": l["ligue"], "total_buts": l["total_buts"]} for l in ligues_buts])
    )

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
