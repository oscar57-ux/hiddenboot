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
    c.execute("""
        SELECT e.nom, l.nom as ligue, l.pays, s.score_total
        FROM scores s
        JOIN equipes e ON s.equipe_id = e.id
        JOIN ligues l ON e.ligue_id = l.id
        ORDER BY l.nom, s.score_total DESC
    """)
    equipes = c.fetchall()
    conn.close()

    ligues = {}
    for eq in equipes:
        if eq["ligue"] not in ligues:
            ligues[eq["ligue"]] = []
        ligues[eq["ligue"]].append(eq)

    return render_template("dashboard.html", ligues=ligues)

@app.route("/pepites")
def pepites():
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT j.nom, j.nationalite, j.equipe_id, j.ligue_id,
               j.matchs, j.buts, j.passes, j.note, j.ratio, j.score,
               e.nom as equipe, l.nom as ligue,
               j.id as joueur_id
        FROM api_joueurs j
        JOIN api_equipes e ON j.equipe_id = e.id
        JOIN api_ligues l ON j.ligue_id = l.id
        WHERE j.buts > 0
        ORDER BY j.score DESC
        LIMIT 100
    """)
    joueurs = c.fetchall()
    conn.close()
    return render_template("pepites.html", joueurs=joueurs)

@app.route("/matchs")
def matchs():
    return render_template("matchs.html")



@app.route("/api/matchs-jour")
def api_matchs_jour():
    today = date.today().strftime("%Y-%m-%d")
    
    API_KEY = "f0841753cabc35b8ecca13ee835435d1"
    headers = {"x-apisports-key": API_KEY}
    
    import requests as req
    response = req.get(
        "https://v3.football.api-sports.io/fixtures",
        headers=headers,
        params={"date": today, "timezone": "Europe/Paris"}
    )
    data = response.json()

    ligues = {}
    
    conn = get_db()
    c = conn.cursor()

    for match in data.get("response", []):
        ligue_nom = match["league"]["name"]
        ligue_id = match["league"]["id"]
        
        # On garde que nos ligues suivies
        c.execute("SELECT id FROM api_ligues WHERE id = ?", (ligue_id,))
        if not c.fetchone():
            continue

        if ligue_nom not in ligues:
            ligues[ligue_nom] = []

        home_id = match["teams"]["home"]["id"]
        away_id = match["teams"]["away"]["id"]

        # Récupérer les scores de forme
        c.execute("SELECT score_total FROM scores s JOIN equipes e ON s.equipe_id = e.id JOIN api_equipes ae ON ae.nom = e.nom WHERE ae.id = ?", (home_id,))
        score_home = c.fetchone()
        c.execute("SELECT score_total FROM scores s JOIN equipes e ON s.equipe_id = e.id JOIN api_equipes ae ON ae.nom = e.nom WHERE ae.id = ?", (away_id,))
        score_away = c.fetchone()

        heure = match["fixture"]["date"][11:16]
        statut = "live" if match["fixture"]["status"]["short"] in ["1H", "2H", "HT"] else "upcoming"

        ligues[ligue_nom].append({
            "home": match["teams"]["home"]["name"],
            "away": match["teams"]["away"]["name"],
            "heure": heure,
            "statut": statut,
            "goals_home": match["goals"]["home"] or 0,
            "goals_away": match["goals"]["away"] or 0,
            "score_home": score_home[0] if score_home else 0,
            "score_away": score_away[0] if score_away else 0,
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
