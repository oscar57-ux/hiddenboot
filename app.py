from flask import Flask, render_template, jsonify, request, redirect, url_for
from datetime import date
import os
from datetime import date, datetime
import sqlite3
import math
import numpy as np

app = Flask(__name__)

API_SPORTS_KEY = os.environ.get("API_SPORTS_KEY", "")

# Timestamps des dernières exécutions scheduler
_last_save_time = None    # datetime
_last_verify_time = None  # datetime

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
                              n_sims=10000, buts_saison=0, buts_recents=0):
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
    # Cap à 1.2 : 1 - e^(-1.2) ≈ 70 %, évite les probas irréalistes
    lambda_base = max(0.01, min(1.2, lambda_base))

    if lambda_base < 0.05:
        print(f"[lambda_warn] lambda={lambda_base:.4f} ratio={ratio_buts:.3f} part={part_buts:.3f} opp={buts_encaisses_adv:.2f}")

    rng = np.random.default_rng(42)
    lambdas = lambda_base * rng.normal(loc=1.0, scale=0.15, size=n_sims)
    lambdas = np.clip(lambdas, 0.01, 5.0)
    probas = 1.0 - np.exp(-lambdas)

    mean_pct = min(70.0, round(float(np.mean(probas)) * 100, 1))

    # Planchers minimum selon le profil du joueur
    plancher = 0.0
    if buts_saison > 3:
        plancher = max(plancher, 5.0)
    if buts_recents >= 3:
        plancher = max(plancher, 10.0)
    mean_pct = max(mean_pct, plancher)

    return (
        mean_pct,
        round(float(np.percentile(probas, 2.5)) * 100),
        min(70, round(float(np.percentile(probas, 97.5)) * 100)),
    )


# ──────────────────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect("botfoot.db")
    conn.row_factory = sqlite3.Row
    return conn


def get_pg():
    """Connexion PostgreSQL (Railway). Fallback SQLite si aucune DATABASE_URL."""
    import psycopg2, psycopg2.extras
    db_url = os.environ.get("DATABASE_PUBLIC_URL") or os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("[get_pg] Aucune DATABASE_PUBLIC_URL ni DATABASE_URL — fallback SQLite")
        conn = sqlite3.connect("botfoot.db")
        conn.row_factory = sqlite3.Row
        return conn
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    if "sslmode" not in db_url:
        sep = "&" if "?" in db_url else "?"
        db_url += f"{sep}sslmode=require"
    try:
        conn = psycopg2.connect(db_url)
        conn.cursor_factory = psycopg2.extras.RealDictCursor
        print(f"[get_pg] Connexion PostgreSQL OK — url: {db_url[:30]}...")
        return conn
    except Exception as e:
        print(f"[get_pg] ERREUR connexion PostgreSQL: {e} — url: {db_url[:30]}...")
        raise


def _is_pg(conn):
    """Vrai si la connexion est PostgreSQL (psycopg2)."""
    try:
        import psycopg2
        return isinstance(conn, psycopg2.extensions.connection)
    except Exception:
        return False


def _ph(conn):
    """Retourne le placeholder correct selon le type de connexion."""
    return "%s" if _is_pg(conn) else "?"


def init_pg_tables():
    """Crée les tables persistantes dans PostgreSQL si elles n'existent pas."""
    try:
        conn = get_pg()
        c = conn.cursor()
        if _is_pg(conn):
            c.execute("""CREATE TABLE IF NOT EXISTS predictions (
                id SERIAL PRIMARY KEY,
                fixture_id INTEGER UNIQUE,
                date TEXT, ligue TEXT, ligue_id INTEGER,
                home TEXT, away TEXT,
                pct_home INTEGER, pct_nul INTEGER DEFAULT 0, pct_away INTEGER,
                score_home INTEGER, score_away INTEGER,
                statut TEXT DEFAULT 'en_attente',
                prediction_correcte INTEGER DEFAULT NULL,
                date_maj TEXT
            )""")
            c.execute("""CREATE TABLE IF NOT EXISTS paris_jour (
                id SERIAL PRIMARY KEY,
                date TEXT, categorie TEXT, match TEXT, ligue TEXT,
                type_pari TEXT, description TEXT, cote REAL,
                probabilite INTEGER, raisonnement TEXT, timestamp TEXT,
                probabilite_hiddenscout INTEGER DEFAULT NULL
            )""")
            c.execute("""CREATE TABLE IF NOT EXISTS paris_historique (
                id SERIAL PRIMARY KEY,
                date TEXT, match TEXT, ligue TEXT, categorie TEXT,
                type_pari TEXT, description TEXT, cote REAL,
                probabilite_hiddenscout INTEGER DEFAULT NULL,
                heure_generation TEXT DEFAULT NULL,
                score_reel TEXT, gagne INTEGER DEFAULT NULL,
                UNIQUE(date, match, type_pari)
            )""")
            # Migrations colonnes pour table existante
            for col_sql in [
                "ALTER TABLE paris_historique ADD COLUMN IF NOT EXISTS probabilite_hiddenscout INTEGER DEFAULT NULL",
                "ALTER TABLE paris_historique ADD COLUMN IF NOT EXISTS heure_generation TEXT DEFAULT NULL",
            ]:
                try:
                    c.execute(col_sql)
                except Exception:
                    pass
            c.execute("""CREATE TABLE IF NOT EXISTS predictions_buteurs (
                id SERIAL PRIMARY KEY,
                joueur_id INTEGER, nom TEXT, equipe TEXT, ligue TEXT, ligue_id INTEGER,
                fixture_id INTEGER, date TEXT,
                match_home TEXT, match_away TEXT, est_home INTEGER,
                probabilite REAL, intervalle_bas INTEGER, intervalle_haut INTEGER,
                forme_snapshot TEXT,
                a_marque INTEGER DEFAULT NULL, buts_reels INTEGER DEFAULT NULL,
                statut TEXT DEFAULT 'en_attente',
                UNIQUE(joueur_id, fixture_id)
            )""")
            conn.commit()
            print("[pg] tables PostgreSQL initialisées")
        conn.close()
    except Exception as e:
        print(f"[pg] erreur init_pg_tables: {e}")


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
        joueurs = joueurs[:50]
        # Normaliser scores 0-100 dans la région
        if joueurs:
            s_max = joueurs[0]["score"]
            s_min = joueurs[-1]["score"]
            s_range = s_max - s_min if s_max > s_min else 1
            for j2 in joueurs:
                j2["score"] = min(100, round((j2["score"] - s_min) / s_range * 100))
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
        LIMIT 200
    """)
    rows = c.fetchall()
    joueurs_mondial = []
    for j in rows:
        score = calculer_score_joueur(j["buts"], j["passes"] or 0, j["note"] or 0, j["matchs"] or 1, j["joueur_id"])
        joueurs_mondial.append({**dict(j), "score": score, "drapeau": DRAPEAUX_LIGUES.get(j["ligue_id"], "")})
    joueurs_mondial.sort(key=lambda x: x["score"], reverse=True)
    joueurs_mondial = joueurs_mondial[:50]
    if joueurs_mondial:
        s_max = joueurs_mondial[0]["score"]
        s_min = joueurs_mondial[-1]["score"]
        s_range = s_max - s_min if s_max > s_min else 1
        for jm in joueurs_mondial:
            jm["score"] = min(100, round((jm["score"] - s_min) / s_range * 100))
    resultats["🌐 Top Mondial"] = joueurs_mondial

    joueurs_en_feu = []
    try:
        c.execute("""
            WITH derniers AS (
                SELECT joueur_id, buts, passes,
                       ROW_NUMBER() OVER (PARTITION BY joueur_id ORDER BY date DESC) AS rn
                FROM joueurs_forme
            )
            SELECT d.joueur_id, SUM(d.buts) AS buts_recents,
                   SUM(d.passes) AS passes_recentes,
                   j.nom, j.poste, j.matchs,
                   j.buts, j.passes, j.note, j.ratio,
                   j.equipe_id,
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
            c2 = conn.cursor()
            c2.execute("""
                SELECT buts, passes FROM joueurs_forme
                WHERE joueur_id = ? ORDER BY date DESC LIMIT 5
            """, (j["joueur_id"],))
            forme = [{"buts": r["buts"] or 0, "passes": r["passes"] or 0} for r in c2.fetchall()]
            score = calculer_score_joueur(j["buts"], j["passes"] or 0, j["note"] or 0, j["matchs"] or 1, j["joueur_id"])
            joueurs_en_feu.append({
                **dict(j),
                "score": score,
                "drapeau": DRAPEAUX_LIGUES.get(j["ligue_id"], ""),
                "forme": forme,
            })
    except Exception:
        pass

    conn.close()
    return render_template("pepites.html", regions=resultats, joueurs_en_feu=joueurs_en_feu)

@app.route("/matchs")
def matchs():
    return render_template("matchs.html")

@app.route("/api/debug")
def api_debug():
    """Endpoint de diagnostic pour vérifier l'état sur Railway."""
    pub_url = os.environ.get("DATABASE_PUBLIC_URL", "")
    priv_url = os.environ.get("DATABASE_URL", "")
    db_url = pub_url or priv_url
    info = {
        "api_key_set": bool(API_SPORTS_KEY),
        "api_key_prefix": API_SPORTS_KEY[:6] + "..." if API_SPORTS_KEY else "VIDE",
        "DATABASE_PUBLIC_URL_set": bool(pub_url),
        "DATABASE_URL_set": bool(priv_url),
        "db_url_found": bool(db_url),
        "db_url_prefix": db_url[:30] if db_url else "AUCUNE",
    }
    # SQLite bootstrap
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) n FROM api_ligues"); info["api_ligues"] = c.fetchone()[0]
        c.execute("SELECT COUNT(*) n FROM classements"); info["classements"] = c.fetchone()[0]
        conn.close()
        info["sqlite"] = "ok"
    except Exception as e:
        info["sqlite"] = str(e)
    # PostgreSQL persistant
    try:
        conn_pg = get_pg()
        info["db_type"] = "postgresql" if _is_pg(conn_pg) else "sqlite"
        c_pg = conn_pg.cursor()
        ph = _ph(conn_pg)
        c_pg.execute("SELECT COUNT(*) as n FROM predictions")
        info["predictions_count"] = c_pg.fetchone()["n"]
        conn_pg.close()
        info["pg"] = "ok"
    except Exception as e:
        info["db_type"] = "error"
        info["pg"] = str(e)
    try:
        import requests as req
        r = req.get(
            "https://v3.football.api-sports.io/fixtures",
            headers={"x-apisports-key": API_SPORTS_KEY},
            params={"date": date.today().strftime("%Y-%m-%d"), "timezone": "Europe/Paris"},
            timeout=8,
        )
        d = r.json()
        info["api_status"] = r.status_code
        info["api_results"] = d.get("results", 0)
        info["api_errors"] = d.get("errors", [])
    except Exception as e:
        info["api_call"] = str(e)
    return jsonify(info)


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
    API_KEY = API_SPORTS_KEY
    if not API_KEY:
        print("[matchs-jour] ERREUR: API_SPORTS_KEY non definie")
        return jsonify({"ligues": {}, "date": today, "error": "API_SPORTS_KEY manquante"})
    api_headers = {"x-apisports-key": API_KEY}
    try:
        response = req.get(
            "https://v3.football.api-sports.io/fixtures",
            headers=api_headers,
            params={"date": today, "timezone": "Europe/Paris"},
            timeout=15,
        )
        data = response.json()
    except Exception as e:
        print(f"[matchs-jour] ERREUR appel API: {e}")
        return jsonify({"ligues": {}, "date": today, "error": str(e)})
    print(f"[matchs-jour] API results={data.get('results',0)} errors={data.get('errors')}")
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT id FROM api_ligues")
    ligues_suivies = set(row["id"] for row in c.fetchall())
    # Fallback : si la table api_ligues est vide, utiliser les IDs du dictionnaire DRAPEAUX_LIGUES
    if not ligues_suivies:
        ligues_suivies = set(DRAPEAUX_LIGUES.keys())

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
        saved = None
        try:
            c_pred = conn.cursor()
            c_pred.execute(
                "SELECT pct_home, pct_nul, pct_away FROM predictions WHERE fixture_id = ?",
                (fixture_id,)
            )
            saved = c_pred.fetchone()
        except Exception:
            pass
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
            "fixture_id": fixture_id,
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
    # Noms d'équipes transmis par le frontend pour fallback si l'ID ne matche pas
    equipe_nom = request.args.get("equipe_nom", "").strip()
    adv_nom    = request.args.get("adv_nom", "").strip()
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
        # Fallback par nom si l'ID fixture ne correspond pas à notre table
        if not adv and adv_nom:
            c.execute("""
                SELECT cl.buts_contre, cl.victoires, cl.nuls, cl.defaites, cl.ligue_id
                FROM classements cl
                JOIN api_equipes e ON cl.equipe_id = e.id
                WHERE e.nom LIKE ?
                LIMIT 1
            """, (f"%{adv_nom}%",))
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

    # Buts totaux de l'équipe saison (pour calculer la part par joueur)
    c.execute("SELECT buts_pour FROM classements WHERE equipe_id = ?", (equipe_id,))
    eq_row = c.fetchone()
    # Fallback par nom si l'ID fixture ne correspond pas
    if not eq_row and equipe_nom:
        c.execute("""
            SELECT cl.buts_pour FROM classements cl
            JOIN api_equipes e ON cl.equipe_id = e.id
            WHERE e.nom LIKE ?
            LIMIT 1
        """, (f"%{equipe_nom}%",))
        eq_row = c.fetchone()
    total_buts_equipe = max(1, eq_row["buts_pour"] or 1) if eq_row else 1

    # Joueurs de l'équipe ayant joué ≥ 5 matchs (stats saison depuis api_joueurs)
    c.execute("""
        SELECT j.id as joueur_id, j.nom, j.poste, j.matchs,
               j.buts, j.passes, j.note, j.ratio,
               e.nom as equipe, l.nom as ligue, l.id as ligue_id
        FROM api_joueurs j
        JOIN api_equipes e ON j.equipe_id = e.id
        JOIN api_ligues l ON j.ligue_id = l.id
        WHERE j.equipe_id = ? AND j.matchs >= 5
        ORDER BY j.buts DESC
    """, (equipe_id,))
    joueurs = c.fetchall()
    # Fallback par nom d'équipe si l'ID fixture ne matche pas api_joueurs
    if not joueurs and equipe_nom:
        c.execute("""
            SELECT j.id as joueur_id, j.nom, j.poste, j.matchs,
                   j.buts, j.passes, j.note, j.ratio,
                   e.nom as equipe, l.nom as ligue, l.id as ligue_id
            FROM api_joueurs j
            JOIN api_equipes e ON j.equipe_id = e.id
            JOIN api_ligues l ON j.ligue_id = l.id
            WHERE e.nom LIKE ? AND j.matchs >= 5
            ORDER BY j.buts DESC
        """, (f"%{equipe_nom}%",))
        joueurs = c.fetchall()

    result = []
    for j in joueurs:
        buts = j["buts"] or 0
        passes = j["passes"] or 0
        matchs_j = max(1, j["matchs"] or 1)
        ratio = float(j["ratio"]) if j["ratio"] else (buts / matchs_j)

        # Part des buts de l'équipe scorés par ce joueur (buts_joueur / buts_equipe_saison)
        # Cap à 0.50 : même le meilleur buteur ne peut pas scorer 100% des buts de son équipe
        # Minimum 3% pour les joueurs sans buts (permet la différenciation par forme)
        part = min(0.50, buts / total_buts_equipe) if buts > 0 else 0.03

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

        buts_recents_j = sum(r["buts"] or 0 for r in forme_recente) if forme_recente else 0
        pct_but, ci_bas, ci_haut = calculer_proba_buteur_mc(
            ratio_buts=max(0.01, ratio),
            buts_encaisses_adv=be_adv,
            forme_str=forme_str,
            est_domicile=est_domicile,
            part_buts=part,
            moy_ligue=moy_ligue,
            buts_saison=buts,
            buts_recents=buts_recents_j,
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
            "equipe": j["equipe"],
            "ligue": j["ligue"],
            "drapeau": DRAPEAUX_LIGUES.get(j["ligue_id"], ""),
        })

    # Trier par probabilité — top 15 (inline prend les 3 premiers, onglet buteurs filtre >20%)
    result.sort(key=lambda x: x["pct_but"], reverse=True)
    conn.close()
    return jsonify({"joueurs": result[:15], "equipe_id": equipe_id})

@app.route("/api/prochain-match/<int:equipe_id>")
def api_prochain_match(equipe_id):
    """Retourne le prochain match d'une équipe (dans les 7 jours) via l'API externe."""
    import requests as req
    from datetime import datetime, timedelta
    API_KEY = API_SPORTS_KEY
    try:
        response = req.get(
            "https://v3.football.api-sports.io/fixtures",
            headers={"x-apisports-key": API_KEY},
            params={"team": equipe_id, "next": 1, "timezone": "Europe/Paris"},
            timeout=5
        )
        data = response.json()
    except Exception:
        return jsonify({"match": None})

    matches = data.get("response", [])
    if not matches:
        return jsonify({"match": None})

    m = matches[0]
    try:
        dt = datetime.fromisoformat(m["fixture"]["date"])
    except Exception:
        return jsonify({"match": None})

    # Ignorer si le match est dans plus de 7 jours
    now = datetime.now(dt.tzinfo)
    if (dt - now).total_seconds() < 0 or (dt - now).total_seconds() > 7 * 86400:
        return jsonify({"match": None})

    home_id  = m["teams"]["home"]["id"]
    home_nom = m["teams"]["home"]["name"]
    away_nom = m["teams"]["away"]["name"]
    is_home  = (home_id == equipe_id)
    adversaire = away_nom if is_home else home_nom

    JOURS_FR = ["Lun", "Mar", "Mer", "Jeu", "Ven", "Sam", "Dim"]
    MOIS_FR  = ["jan", "fév", "mars", "avr", "mai", "juin",
                "juil", "août", "sep", "oct", "nov", "déc"]
    date_label = f"{JOURS_FR[dt.weekday()]} {dt.day} {MOIS_FR[dt.month - 1]}"
    heure = f"{dt.hour:02d}h{dt.minute:02d}"

    return jsonify({
        "match": {
            "date_label": date_label,
            "heure": heure,
            "adversaire": adversaire,
            "is_home": is_home,
        }
    })


@app.route("/api/verifier-paris")
def api_verifier_paris():
    """Vérifie les paris de la veille (ou ?date=) via les scores réels en DB."""
    conn = get_pg()
    c = conn.cursor()
    ph = _ph(conn)

    from datetime import timedelta
    date_param = request.args.get("date", "").strip()
    try:
        datetime.strptime(date_param, "%Y-%m-%d")
        cible = date_param
    except (ValueError, TypeError):
        cible = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    # Récupérer les paris du jour cible (hors résumé)
    c.execute(f"""
        SELECT * FROM paris_jour
        WHERE date = {ph} AND categorie IN ('safe','tentant','cool','fun')
    """, (cible,))
    paris_cible = c.fetchall()

    verifie = 0
    gagne = 0
    for pari in paris_cible:
        match_str = pari["match"] or ""
        # Tenter de trouver le résultat dans predictions
        # Format match: "Home vs Away" ou "Home - Away"
        sep = " vs " if " vs " in match_str else " - "
        parts = match_str.split(sep, 1)
        if len(parts) != 2:
            continue
        home_q, away_q = parts[0].strip(), parts[1].strip()
        print(f"[verifier-paris] lookup '{home_q[:15]}' vs '{away_q[:15]}' pour {cible}")
        c.execute(f"""
            SELECT score_home, score_away, statut FROM predictions
            WHERE date = {ph} AND statut = 'termine'
              AND (home LIKE {ph} OR home LIKE {ph})
              AND (away LIKE {ph} OR away LIKE {ph})
            LIMIT 1
        """, (cible,
              f"%{home_q[:10]}%", f"{home_q[:10]}%",
              f"%{away_q[:10]}%", f"{away_q[:10]}%"))
        res = c.fetchone()
        if not res:
            print(f"[verifier-paris] aucun résultat trouvé pour '{home_q}' vs '{away_q}'")
            continue

        sh = res["score_home"] or 0
        sa = res["score_away"] or 0
        score_reel = f"{sh}-{sa}"
        total = sh + sa
        type_pari = (pari["type_pari"] or "").lower()

        gagne_pari = None
        if "domicile" in type_pari or "victoire 1" in type_pari or type_pari in ("1", "home"):
            gagne_pari = 1 if sh > sa else 0
        elif "extérieur" in type_pari or "exterieur" in type_pari or type_pari in ("2", "away"):
            gagne_pari = 1 if sa > sh else 0
        elif "nul" in type_pari or type_pari == "x":
            gagne_pari = 1 if sh == sa else 0
        elif "2.5" in type_pari:
            if "plus" in type_pari or "over" in type_pari or "+" in type_pari:
                gagne_pari = 1 if total > 2 else 0
            elif "moins" in type_pari or "under" in type_pari:
                gagne_pari = 1 if total < 3 else 0
        elif "1.5" in type_pari:
            if "plus" in type_pari or "over" in type_pari or "+" in type_pari:
                gagne_pari = 1 if total > 1 else 0
        elif "btts" in type_pari or "les deux" in type_pari:
            gagne_pari = 1 if sh > 0 and sa > 0 else 0
        elif "double chance" in type_pari:
            if "1x" in type_pari:
                gagne_pari = 1 if sh >= sa else 0
            elif "x2" in type_pari:
                gagne_pari = 1 if sa >= sh else 0
            elif "12" in type_pari:
                gagne_pari = 1 if sh != sa else 0

        if gagne_pari is not None:
            print(f"[verifier-paris] '{pari['match']}' type='{type_pari}' → {score_reel} → gagne={gagne_pari}")
            try:
                c.execute(f"""INSERT INTO paris_historique
                    (date, match, ligue, categorie, type_pari, description, cote, score_reel, gagne)
                    VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
                    ON CONFLICT (date, match, type_pari) DO UPDATE SET
                        score_reel = EXCLUDED.score_reel,
                        gagne = EXCLUDED.gagne
                """, (cible, pari["match"], pari["ligue"], pari["categorie"],
                      pari["type_pari"], pari["description"], pari["cote"],
                      score_reel, gagne_pari))
            except Exception as e:
                print(f"[verifier-paris] ERREUR INSERT: {e}")
            verifie += 1
            gagne += gagne_pari

    conn.commit()
    conn.close()
    return jsonify({"verifie": verifie, "gagne": gagne, "date": cible})


@app.route("/api/paris-historique")
def api_paris_historique():
    conn = get_pg()
    c = conn.cursor()
    try:
        c.execute("""
            SELECT * FROM paris_historique
            ORDER BY date DESC, categorie ASC
            LIMIT 200
        """)
        rows = [dict(r) for r in c.fetchall()]
        c.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN gagne=1 THEN 1 ELSE 0 END) as gagne,
                   SUM(CASE WHEN gagne=1 THEN cote ELSE 0 END) as gains,
                   COUNT(*) as mises
            FROM paris_historique WHERE gagne IS NOT NULL
        """)
        gs = c.fetchone()
    except Exception:
        rows = []
        gs = None
    conn.close()
    total = gs["total"] or 0 if gs else 0
    n_gagne = gs["gagne"] or 0 if gs else 0
    gains = float(gs["gains"] or 0) if gs else 0
    mises = float(gs["mises"] or 0) if gs else 0
    roi = round((gains - mises) / mises * 100, 1) if mises > 0 else 0
    return jsonify({
        "paris": rows,
        "stats": {
            "total": total,
            "gagne": n_gagne,
            "taux": round(n_gagne / total * 100) if total > 0 else 0,
            "roi": roi,
        }
    })


@app.route("/paris")
def paris():
    from datetime import timedelta
    conn = get_pg()
    c = conn.cursor()
    ph = _ph(conn)
    today = date.today().strftime("%Y-%m-%d")
    try:
        c.execute(f"""
            SELECT * FROM paris_jour
            WHERE date = {ph} AND categorie IN ('safe','tentant','fun','cool')
            ORDER BY CASE categorie
                WHEN 'safe' THEN 1 WHEN 'tentant' THEN 2 WHEN 'cool' THEN 2 WHEN 'fun' THEN 3 END,
                COALESCE(probabilite_hiddenscout, probabilite) DESC
        """, (today,))
        paris_today = [dict(r) for r in c.fetchall()]
        c.execute(f"SELECT description FROM paris_jour WHERE date = {ph} AND categorie = 'resume'", (today,))
        row = c.fetchone()
        resume = row["description"] if row else None
        c.execute(f"SELECT MAX(timestamp) AS ts FROM paris_jour WHERE date = {ph}", (today,))
        row = c.fetchone()
        derniere_gen = row["ts"] if row and row["ts"] else None
        sept_j = (date.today() - timedelta(days=7)).strftime("%Y-%m-%d")
        c.execute(f"""
            SELECT date,
                SUM(CASE WHEN categorie='safe' THEN 1 ELSE 0 END) AS n_safe,
                SUM(CASE WHEN categorie='cool' THEN 1 ELSE 0 END) AS n_cool,
                SUM(CASE WHEN categorie='fun'  THEN 1 ELSE 0 END) AS n_fun
            FROM paris_jour
            WHERE date >= {ph} AND date < {ph} AND categorie IN ('safe','cool','fun')
            GROUP BY date ORDER BY date DESC
        """, (sept_j, today))
        historique = [dict(r) for r in c.fetchall()]
    except Exception:
        paris_today = []
        resume = None
        derniere_gen = None
        historique = []
    # Historique paris ✅/❌
    paris_histo = []
    histo_stats = {"total": 0, "gagne": 0, "taux": 0, "roi": 0}
    try:
        c.execute("""
            SELECT * FROM paris_historique ORDER BY date DESC, id DESC LIMIT 200
        """)
        paris_histo = [dict(r) for r in c.fetchall()]
        # ROI réel : gain net = (cote-1) si gagné, -1 si perdu
        c.execute("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN gagne=1 THEN 1 ELSE 0 END) as n_gagne,
                   SUM(CASE WHEN gagne=1 THEN cote - 1 ELSE -1 END) as net_profit
            FROM paris_historique WHERE gagne IS NOT NULL
        """)
        gs = c.fetchone()
        if gs and gs["total"]:
            n_g = gs["n_gagne"] or 0
            tot = gs["total"] or 1
            net = float(gs["net_profit"] or 0)
            roi = round(net / tot * 100, 1)
            histo_stats = {"total": tot, "gagne": n_g, "taux": round(n_g/tot*100), "roi": roi}
    except Exception:
        pass
    conn.close()
    return render_template("paris.html",
        safe_paris=[p for p in paris_today if p["categorie"] == "safe"],
        tentant_paris=[p for p in paris_today if p["categorie"] in ("tentant", "cool")],
        fun_paris=[p for p in paris_today if p["categorie"] == "fun"],
        resume=resume,
        derniere_gen=derniere_gen,
        date_today=today,
        historique=historique,
        has_paris=bool(paris_today),
        aucun_pari_solide=bool(paris_today) is False,
        paris_histo=paris_histo,
        histo_stats=histo_stats,
    )


@app.route("/api/paris-jour")
def api_paris_jour():
    today = date.today().strftime("%Y-%m-%d")
    conn = get_pg()
    c = conn.cursor()
    ph = _ph(conn)
    try:
        c.execute(f"""
            SELECT * FROM paris_jour WHERE date = {ph}
            ORDER BY CASE categorie
                WHEN 'safe' THEN 1 WHEN 'cool' THEN 2 WHEN 'fun' THEN 3 ELSE 4 END,
                probabilite DESC
        """, (today,))
        paris = [dict(r) for r in c.fetchall()]
    except Exception:
        paris = []
    conn.close()
    return jsonify({"paris": paris, "date": today})


@app.route("/api/generer-paris")
def api_generer_paris():
    try:
        from generateur_paris import generer_paris
        n = generer_paris()
        return jsonify({"status": "ok", "paris": n, "message": f"{n} paris générés"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/resultats")
def resultats():
    return render_template("resultats.html")

@app.route("/api/sauvegarder-predictions")
def sauvegarder_predictions():
    """A appeler chaque jour avant les matchs pour sauvegarder les prédictions"""
    import requests as req
    today = date.today().strftime("%Y-%m-%d")
    API_KEY = API_SPORTS_KEY
    api_headers = {"x-apisports-key": API_KEY}

    response = req.get(
        "https://v3.football.api-sports.io/fixtures",
        headers=api_headers,
        params={"date": today, "timezone": "Europe/Paris"}
    )
    data = response.json()

    # SQLite pour les données bootstrap (classements, joueurs, etc.)
    conn = get_db()
    c = conn.cursor()
    # PostgreSQL pour les données persistantes (predictions, buteurs)
    conn_pg = get_pg()
    c_pg = conn_pg.cursor()

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

    ph = _ph(conn_pg)
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
            c_pg.execute(f'''INSERT INTO predictions
                (fixture_id, date, ligue, ligue_id, home, away, pct_home, pct_nul, pct_away, statut, date_maj)
                VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},'en_attente',{ph})
                ON CONFLICT (fixture_id) DO NOTHING''',
                (fixture_id, today, match["league"]["name"], ligue_id,
                 match["teams"]["home"]["name"], match["teams"]["away"]["name"],
                 pct_home, pct_nul, pct_away, datetime.now().strftime("%Y-%m-%d %H:%M")))
            total += 1
        except Exception:
            pass

    conn_pg.commit()

    # ── Sauvegarde des buteurs prédits ─────────────────────────────────────────
    try:
        for match in data.get("response", []):
            ligue_id = match["league"]["id"]
            if ligue_id not in ligues_suivies:
                continue
            home_id  = match["teams"]["home"]["id"]
            away_id  = match["teams"]["away"]["id"]
            home_name = match["teams"]["home"]["name"]
            away_name = match["teams"]["away"]["name"]
            fixture_id = match["fixture"]["id"]
            ligue_nom_b = match["league"]["name"]

            # Moyenne buts ligue (SQLite)
            c.execute("""
                SELECT CAST(SUM(buts_pour) AS REAL) / MAX(1, SUM(victoires + nuls + defaites)) AS moy
                FROM classements WHERE ligue_id = ?
            """, (ligue_id,))
            moy_row2 = c.fetchone()
            moy_ligue_b = max(0.5, (moy_row2["moy"] or 1.35) if moy_row2 and moy_row2["moy"] else 1.35)

            for (eq_id, adv_id, est_home_b, eq_name_b) in [
                (home_id, away_id, True, home_name),
                (away_id, home_id, False, away_name),
            ]:
                # Stats défensives adversaire (SQLite)
                c.execute("""
                    SELECT buts_contre, victoires, nuls, defaites
                    FROM classements WHERE equipe_id = ?
                """, (adv_id,))
                adv_row = c.fetchone()
                if adv_row:
                    matchs_adv = max(1, (adv_row["victoires"] or 0) + (adv_row["nuls"] or 0) + (adv_row["defaites"] or 0))
                    be_adv_b = (adv_row["buts_contre"] or 0) / matchs_adv
                else:
                    be_adv_b = moy_ligue_b

                # Buts totaux équipe (SQLite)
                c.execute("SELECT buts_pour FROM classements WHERE equipe_id = ?", (eq_id,))
                eq_row2 = c.fetchone()
                total_buts_eq = max(1, eq_row2["buts_pour"] or 1) if eq_row2 else 1

                # Joueurs (matchs >= 5) (SQLite)
                c.execute("""
                    SELECT j.id as joueur_id, j.nom, j.matchs, j.buts, j.passes, j.note, j.ratio,
                           e.nom as equipe
                    FROM api_joueurs j
                    JOIN api_equipes e ON j.equipe_id = e.id
                    WHERE j.equipe_id = ? AND j.matchs >= 5
                    ORDER BY j.buts DESC
                """, (eq_id,))
                joueurs_b = c.fetchall()

                for jb in joueurs_b:
                    buts_j = jb["buts"] or 0
                    matchs_j = max(1, jb["matchs"] or 1)
                    ratio_j = float(jb["ratio"]) if jb["ratio"] else (buts_j / matchs_j)
                    part_j = min(0.50, buts_j / total_buts_eq) if buts_j > 0 else 0.03

                    forme_str_b = ""
                    forme_recente_b = []
                    try:
                        c.execute("""
                            SELECT buts, passes, note, date FROM joueurs_forme
                            WHERE joueur_id = ? ORDER BY date DESC LIMIT 5
                        """, (jb["joueur_id"],))
                        fr = c.fetchall()
                        forme_recente_b = [{"buts": r["buts"] or 0, "passes": r["passes"] or 0, "note": round(float(r["note"] or 0), 1)} for r in fr]
                        if fr:
                            forme_str_b = "".join(
                                "W" if (r["buts"] or 0) > 0 else ("D" if (r["passes"] or 0) > 0 else "L")
                                for r in fr
                            )
                    except Exception:
                        pass

                    buts_rec_b = sum(r["buts"] for r in forme_recente_b) if forme_recente_b else 0
                    pct_b, ci_bas_b, ci_haut_b = calculer_proba_buteur_mc(
                        ratio_buts=max(0.01, ratio_j),
                        buts_encaisses_adv=be_adv_b,
                        forme_str=forme_str_b,
                        est_domicile=est_home_b,
                        part_buts=part_j,
                        moy_ligue=moy_ligue_b,
                        buts_saison=buts_j,
                        buts_recents=buts_rec_b,
                    )

                    if pct_b >= 20:
                        import json as _json
                        try:
                            c_pg.execute(f"""INSERT INTO predictions_buteurs
                                (joueur_id, nom, equipe, ligue, ligue_id, fixture_id, date,
                                 match_home, match_away, est_home, probabilite, intervalle_bas,
                                 intervalle_haut, forme_snapshot, statut)
                                VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},'en_attente')
                                ON CONFLICT (joueur_id, fixture_id) DO NOTHING
                            """, (
                                jb["joueur_id"], jb["nom"], jb["equipe"] or eq_name_b,
                                ligue_nom_b, ligue_id, fixture_id, today,
                                home_name, away_name, 1 if est_home_b else 0,
                                pct_b, ci_bas_b, ci_haut_b,
                                _json.dumps(forme_recente_b),
                            ))
                        except Exception:
                            pass
        conn_pg.commit()
    except Exception as _be:
        print(f"[sauvegarder] erreur buteurs: {_be}")

    conn.close()
    conn_pg.close()
    return jsonify({"sauvegarde": total, "date": today})

@app.route("/api/verifier-resultats")
def verifier_resultats():
    """A appeler chaque matin pour vérifier les résultats de la veille"""
    import requests as req
    from datetime import datetime, timedelta

    date_param = request.args.get("date", "").strip()
    try:
        datetime.strptime(date_param, "%Y-%m-%d")
        cible = date_param
    except (ValueError, TypeError):
        cible = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    API_KEY = API_SPORTS_KEY
    api_headers = {"x-apisports-key": API_KEY}

    response = req.get(
        "https://v3.football.api-sports.io/fixtures",
        headers=api_headers,
        params={"date": cible, "timezone": "Europe/Paris"}
    )
    data = response.json()

    conn = get_pg()
    c = conn.cursor()
    ph = _ph(conn)
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
        c.execute(f"SELECT pct_home, pct_nul, pct_away FROM predictions WHERE fixture_id = {ph}", (fixture_id,))
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

        c.execute(f'''UPDATE predictions SET
            score_home = {ph},
            score_away = {ph},
            statut = 'termine',
            prediction_correcte = {ph},
            date_maj = {ph}
            WHERE fixture_id = {ph}''',
            (goals_home, goals_away, correct,
             datetime.now().strftime("%Y-%m-%d %H:%M"), fixture_id))

    conn.commit()
    conn.close()

    precision = round((total_correct / total_verifie * 100)) if total_verifie > 0 else 0
    return jsonify({
        "verifie": total_verifie,
        "correct": total_correct,
        "precision": precision,
        "date": cible
    })

@app.route("/api/historique-predictions")
def historique_predictions():
    conn = get_pg()
    c = conn.cursor()

    seuil = max(0, min(100, int(request.args.get("seuil", 0) or 0)))
    seuil_sql = f"AND GREATEST(pct_home, pct_away) >= {seuil}" if seuil > 0 else ""

    date_param = request.args.get("date", "").strip()
    try:
        datetime.strptime(date_param, "%Y-%m-%d")
        date_filtre = date_param
    except (ValueError, TypeError):
        date_filtre = None

    # Stats globales (matchs terminés + seuil)
    c.execute(f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN prediction_correcte = 1 THEN 1 ELSE 0 END) as correct,
            SUM(CASE WHEN statut = 'en_attente' THEN 1 ELSE 0 END) as en_attente
        FROM predictions
        WHERE statut = 'termine' {seuil_sql}
    """)
    stats_row = c.fetchone()
    c.execute("SELECT COUNT(*) as n FROM predictions WHERE statut = 'en_attente'")
    attente_row = c.fetchone()

    # Précision par ligue (terminés + seuil)
    c.execute(f"""
        SELECT ligue, ligue_id,
               COUNT(*) as total,
               SUM(CASE WHEN prediction_correcte = 1 THEN 1 ELSE 0 END) as correct
        FROM predictions
        WHERE statut = 'termine' {seuil_sql}
        GROUP BY ligue
        ORDER BY correct * 100 / COUNT(*) DESC
    """)
    par_ligue = c.fetchall()

    # Derniers matchs vérifiés (terminés + seuil + date si fourni)
    date_sql_derniers = f"AND date = '{date_filtre}'" if date_filtre else ""
    c.execute(f"""
        SELECT * FROM predictions
        WHERE statut = 'termine' {seuil_sql} {date_sql_derniers}
        ORDER BY date DESC, fixture_id DESC
        LIMIT 100
    """)
    derniers = c.fetchall()

    # En attente (sans seuil + date si fourni)
    date_sql_attente = f"AND date = '{date_filtre}'" if date_filtre else ""
    c.execute(f"""
        SELECT * FROM predictions
        WHERE statut = 'en_attente' {date_sql_attente}
        ORDER BY date DESC
        LIMIT 100
    """)
    en_attente = c.fetchall()

    # Par jour — 30 derniers jours, terminés + seuil
    c.execute(f"""
        SELECT date,
               COUNT(*) as total,
               SUM(CASE WHEN prediction_correcte = 1 THEN 1 ELSE 0 END) as correct
        FROM predictions
        WHERE statut = 'termine' {seuil_sql}
        GROUP BY date
        ORDER BY date DESC
        LIMIT 30
    """)
    par_jour_rows = c.fetchall()
    par_jour = []
    for r in par_jour_rows:
        total_j = r["total"] or 0
        correct_j = r["correct"] or 0
        par_jour.append({
            "date": r["date"],
            "total": total_j,
            "correct": correct_j,
            "precision": round(correct_j / total_j * 100) if total_j > 0 else 0,
        })
    par_jour.reverse()

    conn.close()

    total_s = stats_row["total"] or 0
    correct_s = stats_row["correct"] or 0
    precision_globale = round(correct_s / total_s * 100) if total_s > 0 else 0

    return jsonify({
        "stats": {
            "total": total_s,
            "correct": correct_s,
            "precision": precision_globale,
            "en_attente": attente_row["n"] or 0,
        },
        "par_ligue": [
            {
                "ligue": r["ligue"],
                "ligue_id": r["ligue_id"],
                "total": r["total"],
                "correct": r["correct"] or 0,
                "precision": round((r["correct"] or 0) / r["total"] * 100),
            }
            for r in par_ligue
        ],
        "derniers": [dict(r) for r in derniers],
        "en_attente": [dict(r) for r in en_attente],
        "par_jour": par_jour,
        "date": date_filtre or "",
        "seuil": seuil,
    })

@app.route("/api/scheduler-status")
def scheduler_status():
    """Expose les timestamps des dernières exécutions automatiques"""
    def fmt_time(dt):
        if dt is None:
            return None
        return dt.strftime("%H:%M")

    def fmt_ago(dt):
        if dt is None:
            return None
        diff = int((datetime.now() - dt).total_seconds() / 60)
        if diff < 1:
            return "à l'instant"
        if diff == 1:
            return "il y a 1 min"
        return f"il y a {diff} min"

    return jsonify({
        "last_save_label": fmt_time(_last_save_time),
        "last_verify_label": fmt_ago(_last_verify_time),
        "last_verify_ts": _last_verify_time.timestamp() if _last_verify_time else 0,
    })


@app.route("/api/predictions-buteurs")
def api_predictions_buteurs():
    date_param = request.args.get("date", date.today().strftime("%Y-%m-%d")).strip()
    try:
        datetime.strptime(date_param, "%Y-%m-%d")
    except (ValueError, TypeError):
        date_param = date.today().strftime("%Y-%m-%d")

    conn = get_pg()
    c = conn.cursor()
    ph = _ph(conn)

    c.execute(f"""
        SELECT * FROM predictions_buteurs
        WHERE date = {ph}
        ORDER BY probabilite DESC
    """, (date_param,))
    rows = c.fetchall()

    # Stats globales buteurs
    c.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN statut = 'termine' AND a_marque = 1 THEN 1 ELSE 0 END) as marque,
            SUM(CASE WHEN statut = 'termine' THEN 1 ELSE 0 END) as termine,
            SUM(CASE WHEN probabilite >= 20 AND statut = 'termine' AND a_marque = 1 THEN 1 ELSE 0 END) as ok_20,
            SUM(CASE WHEN probabilite >= 20 AND statut = 'termine' THEN 1 ELSE 0 END) as tot_20,
            SUM(CASE WHEN probabilite >= 30 AND statut = 'termine' AND a_marque = 1 THEN 1 ELSE 0 END) as ok_30,
            SUM(CASE WHEN probabilite >= 30 AND statut = 'termine' THEN 1 ELSE 0 END) as tot_30,
            SUM(CASE WHEN probabilite >= 40 AND statut = 'termine' AND a_marque = 1 THEN 1 ELSE 0 END) as ok_40,
            SUM(CASE WHEN probabilite >= 40 AND statut = 'termine' THEN 1 ELSE 0 END) as tot_40,
            SUM(CASE WHEN probabilite >= 65 AND statut = 'termine' AND a_marque = 1 THEN 1 ELSE 0 END) as ok_65,
            SUM(CASE WHEN probabilite >= 65 AND statut = 'termine' THEN 1 ELSE 0 END) as tot_65
        FROM predictions_buteurs
    """)
    gs = c.fetchone()
    conn.close()

    def pct(ok, tot):
        return round(ok / tot * 100) if tot and tot > 0 else None

    predictions = [dict(r) for r in rows]

    return jsonify({
        "predictions": predictions,
        "date": date_param,
        "stats": {
            "total": gs["total"] or 0,
            "termine": gs["termine"] or 0,
            "marque": gs["marque"] or 0,
            "precision_20": pct(gs["ok_20"] or 0, gs["tot_20"] or 0),
            "precision_30": pct(gs["ok_30"] or 0, gs["tot_30"] or 0),
            "precision_40": pct(gs["ok_40"] or 0, gs["tot_40"] or 0),
            "precision_65": pct(gs["ok_65"] or 0, gs["tot_65"] or 0),
            "tot_20": gs["tot_20"] or 0,
            "tot_30": gs["tot_30"] or 0,
            "tot_40": gs["tot_40"] or 0,
            "tot_65": gs["tot_65"] or 0,
        }
    })


@app.route("/api/verifier-buteurs")
def api_verifier_buteurs():
    import requests as req
    date_param = request.args.get("date", date.today().strftime("%Y-%m-%d")).strip()
    try:
        datetime.strptime(date_param, "%Y-%m-%d")
    except (ValueError, TypeError):
        date_param = date.today().strftime("%Y-%m-%d")

    conn = get_pg()
    c = conn.cursor()
    ph = _ph(conn)

    # Récupérer les fixture_ids distincts en attente pour cette date
    try:
        c.execute(f"""
            SELECT DISTINCT fixture_id FROM predictions_buteurs
            WHERE date = {ph} AND statut = 'en_attente'
        """, (date_param,))
        fixture_ids = [r["fixture_id"] for r in c.fetchall()]
    except Exception:
        conn.close()
        return jsonify({"verifie": 0, "marque": 0, "date": date_param})

    API_KEY = API_SPORTS_KEY
    api_headers = {"x-apisports-key": API_KEY}
    total_verifie = 0
    total_marque = 0

    # Limiter à 15 fixtures par appel pour éviter le rate limit
    fixture_ids = fixture_ids[:15]

    for fid in fixture_ids:
        try:
            resp = req.get(
                "https://v3.football.api-sports.io/fixtures/players",
                headers=api_headers,
                params={"fixture": fid},
                timeout=10,
            )
            fdata = resp.json()
        except Exception:
            continue

        responses = fdata.get("response", [])
        if not responses:
            continue

        # Vérifier si le match est FT
        fixture_info = responses[0].get("team", {}) if responses else {}
        # Statut du match dans le premier team block
        statut_match = None
        try:
            statut_match = responses[0].get("players", [{}])[0].get("statistics", [{}])[0]
        except Exception:
            pass

        # Récupérer le statut via l'endpoint fixtures
        try:
            resp2 = req.get(
                "https://v3.football.api-sports.io/fixtures",
                headers=api_headers,
                params={"id": fid},
                timeout=8,
            )
            fd2 = resp2.json()
            fix_statut = fd2.get("response", [{}])[0].get("fixture", {}).get("status", {}).get("short", "")
        except Exception:
            fix_statut = ""

        if fix_statut != "FT":
            continue

        # Construire un dict joueur_id -> buts
        buts_par_joueur = {}
        for team_block in responses:
            for joueur_block in team_block.get("players", []):
                jid = joueur_block.get("player", {}).get("id")
                stats_j = joueur_block.get("statistics", [{}])[0]
                goals = stats_j.get("goals", {}).get("total") or 0
                if jid:
                    buts_par_joueur[jid] = goals

        # Mettre à jour nos prédictions pour ce fixture
        c.execute(f"""
            SELECT id, joueur_id FROM predictions_buteurs
            WHERE fixture_id = {ph} AND statut = 'en_attente'
        """, (fid,))
        preds = c.fetchall()
        for pred in preds:
            jid = pred["joueur_id"]
            buts_reels = buts_par_joueur.get(jid, 0)
            a_marque = 1 if buts_reels > 0 else 0
            c.execute(f"""
                UPDATE predictions_buteurs SET
                    a_marque = {ph}, buts_reels = {ph}, statut = 'termine'
                WHERE id = {ph}
            """, (a_marque, buts_reels, pred["id"]))
            total_verifie += 1
            total_marque += a_marque

    conn.commit()
    conn.close()
    return jsonify({"verifie": total_verifie, "marque": total_marque, "date": date_param})


# ── APScheduler : génération automatique des paris à midi (Europe/Paris) ──────
def _job_generer_paris():
    try:
        from generateur_paris import generer_paris
        generer_paris()
    except Exception as e:
        print(f"[scheduler] erreur generer_paris: {e}")


def _job_sauvegarder_predictions_auto():
    global _last_save_time
    try:
        with app.test_request_context('/api/sauvegarder-predictions'):
            sauvegarder_predictions()
        _last_save_time = datetime.now()
        print("[scheduler] sauvegarde predictions auto OK")
    except Exception as e:
        print(f"[scheduler] erreur sauvegarde: {e}")


def _job_verifier_resultats_auto():
    global _last_verify_time
    today = date.today().strftime("%Y-%m-%d")
    try:
        conn = get_pg()
        c = conn.cursor()
        ph = _ph(conn)
        c.execute(f"SELECT COUNT(*) as n FROM predictions WHERE date = {ph} AND statut = 'en_attente'", (today,))
        row = c.fetchone()
        conn.close()
        if not row or (row["n"] or 0) == 0:
            return
    except Exception:
        pass
    try:
        with app.test_request_context(f'/api/verifier-resultats?date={today}'):
            verifier_resultats()
        _last_verify_time = datetime.now()
        print("[scheduler] verification resultats auto OK")
    except Exception as e:
        print(f"[scheduler] erreur verification: {e}")


init_pg_tables()

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    _scheduler = BackgroundScheduler(timezone="Europe/Paris")
    _scheduler.add_job(
        _job_generer_paris,
        "cron",
        hour=12, minute=0,
        id="generer_paris_midi",
        replace_existing=True,
    )
    _scheduler.add_job(
        _job_sauvegarder_predictions_auto,
        "cron",
        hour=0, minute=0,
        id="sauvegarder_minuit",
        replace_existing=True,
    )
    _scheduler.add_job(
        _job_sauvegarder_predictions_auto,
        "cron",
        hour=12, minute=0,
        id="sauvegarder_midi",
        replace_existing=True,
    )
    _scheduler.add_job(
        _job_verifier_resultats_auto,
        "interval",
        minutes=3,
        id="verifier_3min",
        replace_existing=True,
    )
    _scheduler.start()
    print("[scheduler] demarre - paris@12h | sauvegarde@00h+12h | verification toutes les 3min")
except Exception as _sched_err:
    print(f"[scheduler] non demarre: {_sched_err}")


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))