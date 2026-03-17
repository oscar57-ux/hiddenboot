"""
Module partagé — connexion DB et initialisation du schéma.
PostgreSQL si DATABASE_URL définie (production Railway/Render),
SQLite botfoot.db sinon (développement local).
"""
import os
import sqlite3


def get_conn():
    """Retourne une connexion PostgreSQL ou SQLite selon l'environnement.
    Le curseur PG accepte '?' comme placeholder (auto-converti en '%s').
    """
    try:
        import psycopg2, psycopg2.extras
        db_url = os.environ.get("DATABASE_PUBLIC_URL") or os.environ.get("DATABASE_URL", "")
        if db_url:
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
    except Exception:
        pass
    conn = sqlite3.connect("botfoot.db")
    conn.row_factory = sqlite3.Row
    return conn


def _is_pg(conn):
    """Vrai si la connexion est PostgreSQL (psycopg2)."""
    try:
        import psycopg2
        return isinstance(conn, psycopg2.extensions.connection)
    except Exception:
        return False


def _ph(conn):
    """Retourne le placeholder SQL selon le type de connexion."""
    return "%s" if _is_pg(conn) else "?"


def init_all_tables(conn):
    """
    Crée toutes les tables bootstrap si elles n'existent pas encore.
    Compatible PostgreSQL et SQLite.
    Tables : api_ligues, api_equipes, api_joueurs, classements, joueurs_forme.
    """
    pg = _is_pg(conn)
    c  = conn.cursor()
    real = "FLOAT" if pg else "REAL"

    # ── api_ligues ──────────────────────────────────────────────────────────
    c.execute("""CREATE TABLE IF NOT EXISTS api_ligues (
        id      INTEGER PRIMARY KEY,
        nom     TEXT,
        pays    TEXT,
        saison  INTEGER
    )""")

    # ── api_equipes ─────────────────────────────────────────────────────────
    c.execute("""CREATE TABLE IF NOT EXISTS api_equipes (
        id       INTEGER PRIMARY KEY,
        nom      TEXT,
        ligue_id INTEGER,
        pays     TEXT
    )""")

    # ── api_joueurs ─────────────────────────────────────────────────────────
    c.execute(f"""CREATE TABLE IF NOT EXISTS api_joueurs (
        id          INTEGER PRIMARY KEY,
        nom         TEXT,
        age         INTEGER,
        nationalite TEXT,
        poste       TEXT,
        equipe_id   INTEGER,
        ligue_id    INTEGER,
        matchs      INTEGER,
        buts        INTEGER,
        passes      INTEGER,
        note        {real},
        minutes     INTEGER,
        ratio       {real},
        score       {real},
        saison      INTEGER,
        date_maj    TEXT
    )""")

    # ── classements ──────────────────────────────────────────────────────────
    if pg:
        c.execute("""CREATE TABLE IF NOT EXISTS classements (
            id           SERIAL PRIMARY KEY,
            equipe_id    INTEGER,
            ligue_id     INTEGER,
            rang         INTEGER,
            points       INTEGER,
            victoires    INTEGER,
            nuls         INTEGER,
            defaites     INTEGER,
            buts_pour    INTEGER,
            buts_contre  INTEGER,
            diff_buts    INTEGER,
            forme        TEXT,
            date_maj     TEXT,
            buts_dom     INTEGER DEFAULT 0,
            buts_enc_dom INTEGER DEFAULT 0,
            matchs_dom   INTEGER DEFAULT 0,
            buts_ext     INTEGER DEFAULT 0,
            buts_enc_ext INTEGER DEFAULT 0,
            matchs_ext   INTEGER DEFAULT 0,
            forme_dom    TEXT DEFAULT '',
            forme_ext    TEXT DEFAULT ''
        )""")
        for col, typ in [
            ("buts_dom", "INTEGER DEFAULT 0"), ("buts_enc_dom", "INTEGER DEFAULT 0"),
            ("matchs_dom", "INTEGER DEFAULT 0"), ("buts_ext", "INTEGER DEFAULT 0"),
            ("buts_enc_ext", "INTEGER DEFAULT 0"), ("matchs_ext", "INTEGER DEFAULT 0"),
            ("forme_dom", "TEXT DEFAULT ''"), ("forme_ext", "TEXT DEFAULT ''"),
        ]:
            try:
                c.execute(f"ALTER TABLE classements ADD COLUMN IF NOT EXISTS {col} {typ}")
            except Exception:
                pass
    else:
        c.execute("""CREATE TABLE IF NOT EXISTS classements (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            equipe_id    INTEGER,
            ligue_id     INTEGER,
            rang         INTEGER,
            points       INTEGER,
            victoires    INTEGER,
            nuls         INTEGER,
            defaites     INTEGER,
            buts_pour    INTEGER,
            buts_contre  INTEGER,
            diff_buts    INTEGER,
            forme        TEXT,
            date_maj     TEXT,
            buts_dom     INTEGER DEFAULT 0,
            buts_enc_dom INTEGER DEFAULT 0,
            matchs_dom   INTEGER DEFAULT 0,
            buts_ext     INTEGER DEFAULT 0,
            buts_enc_ext INTEGER DEFAULT 0,
            matchs_ext   INTEGER DEFAULT 0,
            forme_dom    TEXT DEFAULT '',
            forme_ext    TEXT DEFAULT ''
        )""")
        for col, typ in [
            ("buts_dom", "INTEGER DEFAULT 0"), ("buts_enc_dom", "INTEGER DEFAULT 0"),
            ("matchs_dom", "INTEGER DEFAULT 0"), ("buts_ext", "INTEGER DEFAULT 0"),
            ("buts_enc_ext", "INTEGER DEFAULT 0"), ("matchs_ext", "INTEGER DEFAULT 0"),
            ("forme_dom", "TEXT DEFAULT ''"), ("forme_ext", "TEXT DEFAULT ''"),
        ]:
            try:
                c.execute(f"ALTER TABLE classements ADD COLUMN {col} {typ}")
            except Exception:
                pass

    # ── joueurs_forme ────────────────────────────────────────────────────────
    if pg:
        c.execute(f"""CREATE TABLE IF NOT EXISTS joueurs_forme (
            id        SERIAL PRIMARY KEY,
            joueur_id INTEGER,
            fixture_id INTEGER,
            equipe_id  INTEGER,
            ligue_id   INTEGER,
            date       TEXT,
            buts       INTEGER DEFAULT 0,
            passes     INTEGER DEFAULT 0,
            note       {real} DEFAULT 0,
            minutes    INTEGER DEFAULT 0,
            titulaire  INTEGER DEFAULT 0,
            UNIQUE(joueur_id, fixture_id),
            UNIQUE(joueur_id, date)
        )""")
    else:
        c.execute(f"""CREATE TABLE IF NOT EXISTS joueurs_forme (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            joueur_id  INTEGER,
            fixture_id INTEGER,
            equipe_id  INTEGER,
            ligue_id   INTEGER,
            date       TEXT,
            buts       INTEGER DEFAULT 0,
            passes     INTEGER DEFAULT 0,
            note       {real} DEFAULT 0,
            minutes    INTEGER DEFAULT 0,
            titulaire  INTEGER DEFAULT 0,
            UNIQUE(joueur_id, fixture_id),
            UNIQUE(joueur_id, date)
        )""")

    # ── Index performances ───────────────────────────────────────────────────
    for ddl in [
        "CREATE INDEX IF NOT EXISTS idx_joueurs_forme_joueur_id ON joueurs_forme(joueur_id)",
        "CREATE INDEX IF NOT EXISTS idx_joueurs_forme_date      ON joueurs_forme(date DESC)",
        "CREATE INDEX IF NOT EXISTS idx_joueurs_forme_buts      ON joueurs_forme(buts)",
        "CREATE INDEX IF NOT EXISTS idx_api_joueurs_equipe_id   ON api_joueurs(equipe_id)",
        "CREATE INDEX IF NOT EXISTS idx_api_joueurs_buts        ON api_joueurs(buts DESC)",
    ]:
        try:
            c.execute(ddl)
        except Exception:
            pass

    try:
        conn.commit()
    except Exception:
        pass
