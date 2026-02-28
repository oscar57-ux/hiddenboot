from flask import Flask, render_template, jsonify, request, redirect, url_for
from datetime import date
import os
from datetime import date, datetime
import sqlite3
import math
import numpy as np

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

# ─── Modèles mathématiques ────────────────────────────────────────────────────

def poisson_pmf(lam, k):
    """P(X=k) pour une distribution de Poisson."""
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return (lam ** k) * math.exp(-lam) / math.factorial(k)


def calculer_forme_ponderee(forme_str):
    """
    Pondération exponentielle de la forme récente (W=3, D=1, L=0), décroissance w=0.9.
    Retourne un score moyen pondéré entre 0 et 3.
    """
    if not forme_str:
        return 1.5  # Valeur neutre
    conv = {"W": 3, "D": 1, "L": 0}
    resultats = [conv.get(f, 1) for f in forme_str[-10:]]
    n = len(resultats)
    if n == 0:
        return 1.5
    poids = [0.9 ** (n - 1 - i) for i in range(n)]
    return sum(r * w for r, w in zip(resultats, poids)) / sum(poids)


def calculer_proba_poisson(stats_home, stats_away, moy_ligue=1.35):
    """
    Modèle de Poisson double pour prédire le résultat d'un match.
    λ = (buts_marqués / moy_ligue) × (buts_encaissés_adversaire / moy_ligue) × facteur_domicile
    Retourne (p_home, p_nul, p_away) en pourcentages entiers (somme = 100).
    """
    if not stats_home or not stats_away:
        return 45, 25, 30

    matchs_home = max(1, stats_home.get("victoires", 0) + stats_home.get("nuls", 0) + stats_home.get("defaites", 0))
    matchs_away = max(1, stats_away.get("victoires", 0) + stats_away.get("nuls", 0) + stats_away.get("defaites", 0))

    bm_home = stats_home.get("buts_pour", 0) / matchs_home
    be_home = stats_home.get("buts_contre", 0) / matchs_home
    bm_away = stats_away.get("buts_pour", 0) / matchs_away
    be_away = stats_away.get("buts_contre", 0) / matchs_away

    moy = max(moy_ligue, 0.5)

    # λ = (buts_marqués / moy) × (buts_encaissés_adversaire / moy) × facteur_domicile
    lambda_home = (bm_home / moy) * (be_away / moy) * 1.3
    lambda_away = (bm_away / moy) * (be_home / moy) * 1.0

    # Ajustement léger par forme pondérée (±20%)
    fh = calculer_forme_ponderee(stats_home.get("forme_raw", ""))
    fa = calculer_forme_ponderee(stats_away.get("forme_raw", ""))
    denom = (fh + fa) / 2
    if denom > 0:
        lambda_home *= max(0.8, min(1.2, fh / denom))
        lambda_away *= max(0.8, min(1.2, fa / denom))

    lambda_home = max(0.1, min(4.0, lambda_home))
    lambda_away = max(0.1, min(4.0, lambda_away))

    # Double convolution de Poisson (jusqu'à 8 buts par équipe)
    p_home = p_nul = p_away = 0.0
    for i in range(9):
        for j in range(9):
            p = poisson_pmf(lambda_home, i) * poisson_pmf(lambda_away, j)
            if i > j:
                p_home += p
            elif i == j:
                p_nul += p
            else:
                p_away += p

    total = p_home + p_nul + p_away
    if total == 0:
        return 45, 25, 30

    r_home = round(p_home / total * 100)
    r_nul = round(p_nul / total * 100)
    r_away = 100 - r_home - r_nul
    return r_home, r_nul, r_away


def calculer_proba_buteur_mc(ratio_buts, buts_encaisses_adv=1.35, forme_str="",
                              est_domicile=True, part_buts=0.30, moy_ligue=1.35,
                              n_sims=10000):
    """
    Monte Carlo Poisson : probabilité qu'un buteur marque dans ce match.
    lambda_buteur = taux × facteur_opposition × (1 + 0.3×forme_norm) × facteur_domicile × part_buts
    Simule 10 000 fois avec bruit gaussien (μ=1, σ=0.15).
    Retourne (proba_pct, ci_bas_pct, ci_haut_pct).
    """
    taux = max(0.01, ratio_buts)
    facteur_opp = max(0.3, min(3.0, buts_encaisses_adv / max(moy_ligue, 0.5)))
    forme_norm = calculer_forme_ponderee(forme_str) / 3.0  # Normaliser entre 0 et 1
    facteur_dom = 1.15 if est_domicile else 1.0

    lambda_base = taux * facteur_opp * (1 + 0.3 * forme_norm) * facteur_dom * part_buts
    lambda_base = max(0.01, min(2.0, lambda_base))

    rng = np.random.default_rng(42)
    lambdas = lambda_base * rng.normal(loc=1.0, scale=0.15, size=n_sims)
    lambdas = np.clip(lambdas, 0.01, 5.0)
    probas = 1.0 - np.exp(-lambdas)

    return (
        round(float(np.mean(probas)) * 100, 1),
        round(float(np.percentile(probas, 2.5)) * 100),
        round(float(np.percentile(probas, 97.5)) * 100),
    )


# ──────────────────────────────────────────────────────────────────────────────

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


@app.route("/")
def index():
    return redirect(url_for("matchs"))


def _build_classements_data():
    """Construit les données ligues pour la page Classements."""
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
    return ligues_data


@app.route("/classements")
def classements():
    return render_template("classements.html", ligues=_build_classements_data())

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

    joueurs_en_feu = []
    try:
        c.execute("""
            WITH derniers AS (
                SELECT joueur_id, buts,
                       ROW_NUMBER() OVER (PARTITION BY joueur_id ORDER BY date DESC) AS rn
                FROM joueurs_forme
            )
            SELECT d.joueur_id, SUM(d.buts) AS buts_recents,
                   j.nom, j.poste, j.matchs,
                   j.buts, j.passes, j.note, j.ratio,
                   e.nom AS equipe, l.nom AS ligue, l.id AS ligue_id
            FROM derniers d
            JOIN api_joueurs j ON d.joueur_id = j.id
            JOIN api_equipes e ON j.equipe_id = e.id
            JOIN api_ligues  l ON j.ligue_id  = l.id
            WHERE d.rn <= 5
            GROUP BY d.joueur_id
            HAVING buts_recents >= 3
            ORDER BY buts_recents DESC, j.buts DESC
            LIMIT 50
        """)
        for j in c.fetchall():
            score = calculer_score_joueur(j["buts"], j["passes"] or 0, j["note"] or 0, j["matchs"] or 1, j["joueur_id"])
            joueurs_en_feu.append({**dict(j), "score": score, "drapeau": DRAPEAUX_LIGUES.get(j["ligue_id"], "")})
    except Exception:
        pass

    conn.close()
    return render_template("pepites.html", regions=resultats, joueurs_en_feu=joueurs_en_feu)

@app.route("/matchs")
def matchs():
    return render_template("matchs.html")

@app.route("/api/matchs-jour")
def api_matchs_jour():
    import requests as req
    date_param = request.args.get("date", "").strip()
    if date_param:
        try:
            datetime.strptime(date_param, "%Y-%m-%d")
            today = date_param
        except ValueError:
            today = date.today().strftime("%Y-%m-%d")
    else:
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
            SELECT rang, points, forme, diff_buts,
                   buts_pour, buts_contre, victoires, nuls, defaites, ligue_id
            FROM classements WHERE equipe_id = ?
        """, (equipe_id,))
        row = c.fetchone()
        if not row:
            return None
        return {
            "rang": row["rang"],
            "points": row["points"],
            "forme_raw": row["forme"] or "",
            "diff_buts": row["diff_buts"] or 0,
            "buts_pour": row["buts_pour"] or 0,
            "buts_contre": row["buts_contre"] or 0,
            "victoires": row["victoires"] or 0,
            "nuls": row["nuls"] or 0,
            "defaites": row["defaites"] or 0,
            "ligue_id": row["ligue_id"],
        }

    # Moyenne de buts par équipe par match, calculée par ligue
    c.execute("""
        SELECT ligue_id,
               CAST(SUM(buts_pour) AS REAL) / MAX(1, SUM(victoires + nuls + defaites)) AS moy
        FROM classements
        WHERE victoires + nuls + defaites > 0
        GROUP BY ligue_id
    """)
    moy_buts_par_ligue = {row["ligue_id"]: max(0.5, row["moy"] or 1.35) for row in c.fetchall()}

    def get_forme_detail(equipe_nom, forme_str):
        if not forme_str or not equipe_nom:
            return []
        form_letters = list(forme_str[-5:])
        c2 = conn.cursor()
        try:
            c2.execute("""
                SELECT home, away, score_home, score_away
                FROM predictions
                WHERE (home = ? OR away = ?) AND statut = 'termine'
                ORDER BY date DESC LIMIT 5
            """, (equipe_nom, equipe_nom))
            records = list(c2.fetchall())
            records.reverse()
        except Exception:
            records = []
        details = []
        for i, letter in enumerate(form_letters):
            label = {"W": "Victoire", "D": "Nul", "L": "Défaite"}.get(letter, "")
            d = {"result": letter, "label": label}
            if i < len(records):
                row = records[i]
                is_home_team = (row["home"] == equipe_nom)
                opp = row["away"] if is_home_team else row["home"]
                sh = row["score_home"] if is_home_team else row["score_away"]
                sa = row["score_away"] if is_home_team else row["score_home"]
                if sh is not None and sa is not None:
                    d["opponent"] = opp
                    d["score"] = f"{sh}-{sa}"
            details.append(d)
        return details

    ligues = {}
    for match in data.get("response", []):
        ligue_id = match["league"]["id"]
        if ligue_id not in ligues_suivies:
            continue
        ligue_nom = match["league"]["name"]
        home_id = match["teams"]["home"]["id"]
        away_id = match["teams"]["away"]["id"]
        home_name = match["teams"]["home"]["name"]
        away_name = match["teams"]["away"]["name"]
        fixture_id = match["fixture"]["id"]
        stats_home = get_stats_equipe(home_id)
        stats_away = get_stats_equipe(away_id)
        moy_ligue = moy_buts_par_ligue.get(ligue_id, 1.35)
        # Utiliser les probabilités sauvegardées si disponibles
        c_pred = conn.cursor()
        c_pred.execute(
            "SELECT pct_home, pct_nul, pct_away FROM predictions WHERE fixture_id = ?",
            (fixture_id,)
        )
        saved = c_pred.fetchone()
        if saved:
            pct_home = saved["pct_home"]
            pct_nul  = saved["pct_nul"]
            pct_away = saved["pct_away"]
        else:
            pct_home, pct_nul, pct_away = calculer_proba_poisson(stats_home, stats_away, moy_ligue)
        heure = match["fixture"]["date"][11:16]
        statut = match["fixture"]["status"]["short"]
        est_live = statut in ["1H", "2H", "HT", "ET", "P"]
        forme_raw_home = stats_home["forme_raw"] if stats_home else ""
        forme_raw_away = stats_away["forme_raw"] if stats_away else ""
        if ligue_nom not in ligues:
            ligues[ligue_nom] = []
        ligues[ligue_nom].append({
            "home": home_name,
            "away": away_name,
            "home_id": home_id,
            "away_id": away_id,
            "heure": heure,
            "statut": statut,
            "est_live": est_live,
            "goals_home": match["goals"]["home"],
            "goals_away": match["goals"]["away"],
            "pct_home": pct_home,
            "pct_nul": pct_nul,
            "pct_away": pct_away,
            "rang_home": stats_home["rang"] if stats_home else "?",
            "rang_away": stats_away["rang"] if stats_away else "?",
            "forme_home": forme_raw_home,
            "forme_away": forme_raw_away,
            "forme_detail_home": get_forme_detail(home_name, forme_raw_home),
            "forme_detail_away": get_forme_detail(away_name, forme_raw_away),
            "drapeau": DRAPEAUX_LIGUES.get(ligue_id, ""),
        })
    conn.close()
    return jsonify({"ligues": ligues, "date": today})

@app.route("/alertes")
def alertes():
    conn = get_db()
    c = conn.cursor()

    # ─── 1. Joueurs en feu ──────────────────────────────────────────
    joueurs_en_feu = []
    try:
        c.execute("""
            WITH derniers AS (
                SELECT joueur_id, buts, passes, note,
                       ROW_NUMBER() OVER (PARTITION BY joueur_id ORDER BY date DESC) AS rn
                FROM joueurs_forme
            )
            SELECT d.joueur_id,
                   SUM(d.buts)  AS buts_recents,
                   SUM(d.passes) AS passes_recentes,
                   ROUND(AVG(d.note), 1) AS note_moy,
                   j.nom, j.buts AS buts_saison,
                   e.nom AS equipe, l.nom AS ligue, l.id AS ligue_id
            FROM derniers d
            JOIN api_joueurs j ON d.joueur_id = j.id
            JOIN api_equipes e ON j.equipe_id = e.id
            JOIN api_ligues  l ON j.ligue_id  = l.id
            WHERE d.rn <= 5
            GROUP BY d.joueur_id
            HAVING buts_recents > 0
            ORDER BY buts_recents DESC, note_moy DESC
            LIMIT 10
        """)
        for row in c.fetchall():
            c2 = conn.cursor()
            c2.execute("""
                SELECT buts, passes FROM joueurs_forme
                WHERE joueur_id = ? ORDER BY date DESC LIMIT 5
            """, (row["joueur_id"],))
            forme = [{"buts": r["buts"] or 0, "passes": r["passes"] or 0} for r in c2.fetchall()]
            joueurs_en_feu.append({
                "joueur_id": row["joueur_id"],
                "buts_recents": row["buts_recents"],
                "passes_recentes": row["passes_recentes"] or 0,
                "note_moy": row["note_moy"],
                "nom": row["nom"],
                "buts_saison": row["buts_saison"] or 0,
                "equipe": row["equipe"],
                "ligue": row["ligue"],
                "drapeau": DRAPEAUX_LIGUES.get(row["ligue_id"], ""),
                "forme": forme,
            })
    except Exception:
        pass

    # ─── 2. Équipes en série ─────────────────────────────────────────
    equipes_serie = []
    try:
        c.execute("""
            SELECT ae.nom, cl.forme, cl.rang, cl.ligue_id, al.nom AS ligue_nom
            FROM classements cl
            JOIN api_equipes ae ON cl.equipe_id = ae.id
            JOIN api_ligues  al ON cl.ligue_id  = al.id
            WHERE cl.forme IS NOT NULL AND cl.forme != ''
        """)
        for row in c.fetchall():
            forme_str = row["forme"] or ""
            wins = 0
            for ch in reversed(forme_str):
                if ch == "W":
                    wins += 1
                else:
                    break
            if wins >= 2:
                equipes_serie.append({
                    "nom": row["nom"],
                    "wins_consecutifs": wins,
                    "rang": row["rang"],
                    "ligue_nom": row["ligue_nom"],
                    "drapeau": DRAPEAUX_LIGUES.get(row["ligue_id"], ""),
                    "forme_dots": list(forme_str[-5:]),
                })
        equipes_serie.sort(key=lambda x: x["wins_consecutifs"], reverse=True)
        equipes_serie = equipes_serie[:10]
    except Exception:
        pass

    # ─── 3. Pépites émergentes ───────────────────────────────────────
    pepites_emergentes = []
    try:
        c.execute("""
            WITH derniers AS (
                SELECT joueur_id, buts, passes, note,
                       ROW_NUMBER() OVER (PARTITION BY joueur_id ORDER BY date DESC) AS rn
                FROM joueurs_forme
            )
            SELECT d.joueur_id,
                   SUM(d.buts)   AS buts_5,
                   SUM(d.passes) AS passes_5,
                   ROUND(AVG(d.note), 1) AS note_5,
                   ROUND(SUM(d.buts) * 3.0 + SUM(d.passes) * 1.0 + AVG(d.note), 2) AS score_forme,
                   j.nom, j.buts AS buts_saison, j.passes AS passes_saison, j.matchs,
                   e.nom AS equipe, l.nom AS ligue, l.id AS ligue_id
            FROM derniers d
            JOIN api_joueurs j ON d.joueur_id = j.id
            JOIN api_equipes e ON j.equipe_id = e.id
            JOIN api_ligues  l ON j.ligue_id  = l.id
            WHERE d.rn <= 5 AND j.buts < 8
            GROUP BY d.joueur_id
            HAVING buts_5 > 0
            ORDER BY score_forme DESC
            LIMIT 10
        """)
        for row in c.fetchall():
            c2 = conn.cursor()
            c2.execute("""
                SELECT buts, passes FROM joueurs_forme
                WHERE joueur_id = ? ORDER BY date DESC LIMIT 5
            """, (row["joueur_id"],))
            forme = [{"buts": r["buts"] or 0, "passes": r["passes"] or 0} for r in c2.fetchall()]
            pepites_emergentes.append({
                "joueur_id": row["joueur_id"],
                "buts_5": row["buts_5"],
                "passes_5": row["passes_5"] or 0,
                "note_5": row["note_5"],
                "score_forme": row["score_forme"],
                "nom": row["nom"],
                "buts_saison": row["buts_saison"] or 0,
                "passes_saison": row["passes_saison"] or 0,
                "matchs": row["matchs"] or 0,
                "equipe": row["equipe"],
                "ligue": row["ligue"],
                "drapeau": DRAPEAUX_LIGUES.get(row["ligue_id"], ""),
                "forme": forme,
            })
    except Exception:
        pass

    # ─── 4. Joueurs à éviter ─────────────────────────────────────────
    joueurs_a_eviter = []
    try:
        c.execute("""
            WITH derniers AS (
                SELECT joueur_id, buts, passes, note,
                       ROW_NUMBER() OVER (PARTITION BY joueur_id ORDER BY date DESC) AS rn
                FROM joueurs_forme
            )
            SELECT d.joueur_id,
                   SUM(d.buts) AS buts_5,
                   j.nom, j.buts AS buts_saison,
                   e.nom AS equipe, l.nom AS ligue, l.id AS ligue_id
            FROM derniers d
            JOIN api_joueurs j ON d.joueur_id = j.id
            JOIN api_equipes e ON j.equipe_id = e.id
            JOIN api_ligues  l ON j.ligue_id  = l.id
            WHERE d.rn <= 5 AND j.buts > 5
            GROUP BY d.joueur_id
            HAVING buts_5 = 0
            ORDER BY j.buts DESC
            LIMIT 10
        """)
        for row in c.fetchall():
            c2 = conn.cursor()
            c2.execute("""
                SELECT buts, passes FROM joueurs_forme
                WHERE joueur_id = ? ORDER BY date DESC LIMIT 5
            """, (row["joueur_id"],))
            forme = [{"buts": r["buts"] or 0, "passes": r["passes"] or 0} for r in c2.fetchall()]
            joueurs_a_eviter.append({
                "joueur_id": row["joueur_id"],
                "buts_saison": row["buts_saison"] or 0,
                "nom": row["nom"],
                "equipe": row["equipe"],
                "ligue": row["ligue"],
                "drapeau": DRAPEAUX_LIGUES.get(row["ligue_id"], ""),
                "forme": forme,
            })
    except Exception:
        pass

    conn.close()
    return render_template("alertes.html",
        joueurs_en_feu=joueurs_en_feu,
        equipes_serie=equipes_serie,
        pepites_emergentes=pepites_emergentes,
        joueurs_a_eviter=joueurs_a_eviter,
    )

@app.route("/api/equipe/<int:equipe_id>/buteurs")
def api_buteurs_equipe(equipe_id):
    adversaire_id = request.args.get("adversaire_id", type=int)
    # Accepte is_home (nouveau) ou domicile (compatibilité)
    is_home_raw = request.args.get("is_home", request.args.get("domicile", "1"))
    est_domicile = is_home_raw == "1"

    conn = get_db()
    c = conn.cursor()

    # Stats défensives de l'adversaire et moyenne ligue
    be_adv = 1.35
    moy_ligue = 1.35
    if adversaire_id:
        c.execute("""
            SELECT buts_contre, victoires, nuls, defaites, ligue_id
            FROM classements WHERE equipe_id = ?
        """, (adversaire_id,))
        adv = c.fetchone()
        if adv:
            matchs_adv = max(1, (adv["victoires"] or 0) + (adv["nuls"] or 0) + (adv["defaites"] or 0))
            be_adv = (adv["buts_contre"] or 0) / matchs_adv
            c.execute("""
                SELECT CAST(SUM(buts_pour) AS REAL) / MAX(1, SUM(victoires + nuls + defaites)) AS moy
                FROM classements WHERE ligue_id = ?
            """, (adv["ligue_id"],))
            moy_row = c.fetchone()
            if moy_row and moy_row["moy"]:
                moy_ligue = max(0.5, moy_row["moy"])

    # Buts totaux de l'équipe (pour calculer la part par joueur)
    c.execute("""
        SELECT buts_pour FROM classements WHERE equipe_id = ?
    """, (equipe_id,))
    eq_row = c.fetchone()
    total_buts_equipe = max(1, eq_row["buts_pour"] or 1) if eq_row else 1

    # Tous les joueurs de l'équipe (pas seulement ceux avec des buts)
    c.execute("""
        SELECT j.id as joueur_id, j.nom, j.poste, j.matchs,
               j.buts, j.passes, j.note, j.ratio
        FROM api_joueurs j
        WHERE j.equipe_id = ?
        ORDER BY j.buts DESC
    """, (equipe_id,))
    joueurs = c.fetchall()

    result = []
    for j in joueurs:
        buts = j["buts"] or 0
        passes = j["passes"] or 0
        matchs_j = max(1, j["matchs"] or 1)
        ratio = float(j["ratio"]) if j["ratio"] else (buts / matchs_j)

        # Part des buts de l'équipe scorés par ce joueur
        # Minimum 3% pour les joueurs sans buts (permet la différenciation par forme)
        part = min(0.85, buts / total_buts_equipe) if buts > 0 else 0.03

        # 5 derniers matchs depuis joueurs_forme : forme string + données pour les dots
        forme_str = ""
        forme_recente = []
        try:
            c.execute("""
                SELECT buts, passes, note, date
                FROM joueurs_forme
                WHERE joueur_id = ? ORDER BY date DESC LIMIT 5
            """, (j["joueur_id"],))
            forme_rows = c.fetchall()
            for r in forme_rows:
                b = r["buts"] or 0
                p = r["passes"] or 0
                forme_recente.append({
                    "buts": b,
                    "passes": p,
                    "note": round(float(r["note"] or 0), 1),
                })
            if forme_rows:
                forme_str = "".join(
                    "W" if (r["buts"] or 0) > 0 else
                    ("D" if (r["passes"] or 0) > 0 else "L")
                    for r in forme_rows
                )
        except Exception:
            pass

        pct_but, ci_bas, ci_haut = calculer_proba_buteur_mc(
            ratio_buts=max(0.01, ratio),
            buts_encaisses_adv=be_adv,
            forme_str=forme_str,
            est_domicile=est_domicile,
            part_buts=part,
            moy_ligue=moy_ligue,
        )

        score = calculer_score_joueur(buts, passes, j["note"] or 0, matchs_j, j["joueur_id"])
        result.append({
            "joueur_id": j["joueur_id"],
            "nom": j["nom"],
            "poste": j["poste"],
            "matchs": j["matchs"],
            "buts": buts,
            "passes": passes,
            "note": round(float(j["note"]), 2) if j["note"] else 0,
            "score": score,
            "pct_but": pct_but,
            "ci_bas": ci_bas,
            "ci_haut": ci_haut,
            "forme_recente": forme_recente,
        })

    # Trier par probabilité de marquer — retourner le top 3
    result.sort(key=lambda x: x["pct_but"], reverse=True)
    conn.close()
    return jsonify({"joueurs": result[:3], "equipe_id": equipe_id})

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
        pct_nul INTEGER DEFAULT 0,
        pct_away INTEGER,
        score_home INTEGER,
        score_away INTEGER,
        statut TEXT DEFAULT "en_attente",
        prediction_correcte INTEGER DEFAULT NULL,
        date_maj TEXT
    )''')
    # Migration si la colonne pct_nul n'existe pas encore
    try:
        c.execute("ALTER TABLE predictions ADD COLUMN pct_nul INTEGER DEFAULT 0")
    except Exception:
        pass

    c.execute("SELECT id FROM api_ligues")
    ligues_suivies = set(row["id"] for row in c.fetchall())

    def get_stats_equipe(equipe_id):
        c.execute("""
            SELECT rang, points, forme, diff_buts,
                   buts_pour, buts_contre, victoires, nuls, defaites, ligue_id
            FROM classements WHERE equipe_id = ?
        """, (equipe_id,))
        row = c.fetchone()
        if not row:
            return None
        return {
            "rang": row["rang"],
            "points": row["points"],
            "forme_raw": row["forme"] or "",
            "diff_buts": row["diff_buts"] or 0,
            "buts_pour": row["buts_pour"] or 0,
            "buts_contre": row["buts_contre"] or 0,
            "victoires": row["victoires"] or 0,
            "nuls": row["nuls"] or 0,
            "defaites": row["defaites"] or 0,
            "ligue_id": row["ligue_id"],
        }

    # Moyenne buts par ligue
    c.execute("""
        SELECT ligue_id,
               CAST(SUM(buts_pour) AS REAL) / MAX(1, SUM(victoires + nuls + defaites)) AS moy
        FROM classements WHERE victoires + nuls + defaites > 0
        GROUP BY ligue_id
    """)
    moy_buts_par_ligue = {row["ligue_id"]: max(0.5, row["moy"] or 1.35) for row in c.fetchall()}

    total = 0
    for match in data.get("response", []):
        ligue_id = match["league"]["id"]
        if ligue_id not in ligues_suivies:
            continue

        home_id = match["teams"]["home"]["id"]
        away_id = match["teams"]["away"]["id"]
        stats_home = get_stats_equipe(home_id)
        stats_away = get_stats_equipe(away_id)
        moy_ligue = moy_buts_par_ligue.get(ligue_id, 1.35)
        pct_home, pct_nul, pct_away = calculer_proba_poisson(stats_home, stats_away, moy_ligue)
        fixture_id = match["fixture"]["id"]

        try:
            c.execute('''INSERT OR IGNORE INTO predictions
                (fixture_id, date, ligue, ligue_id, home, away, pct_home, pct_nul, pct_away, statut, date_maj)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, "en_attente", ?)''',
                (fixture_id, today, match["league"]["name"], ligue_id,
                 match["teams"]["home"]["name"], match["teams"]["away"]["name"],
                 pct_home, pct_nul, pct_away, datetime.now().strftime("%Y-%m-%d %H:%M")))
            total += 1
        except Exception:
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
        c.execute("SELECT pct_home, pct_nul, pct_away FROM predictions WHERE fixture_id = ?", (fixture_id,))
        pred = c.fetchone()

        if not pred:
            continue

        # Notre prédiction (3 issues : domicile / nul / extérieur)
        pcts = {
            "home": pred["pct_home"] or 0,
            "nul": pred["pct_nul"] or 0,
            "away": pred["pct_away"] or 0,
        }
        notre_prediction = max(pcts, key=pcts.get)

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