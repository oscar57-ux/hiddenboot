import sqlite3

def creer_bdd():
    conn = sqlite3.connect("botfoot.db")
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS ligues (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nom TEXT,
        pays TEXT,
        url TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS equipes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nom TEXT,
        url_id TEXT,
        ligue_id INTEGER,
        FOREIGN KEY (ligue_id) REFERENCES ligues(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS resultats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        equipe_id INTEGER,
        date TEXT,
        adversaire TEXT,
        buts_marques INTEGER,
        buts_encaisses INTEGER,
        resultat TEXT,
        FOREIGN KEY (equipe_id) REFERENCES equipes(id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS scores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        equipe_id INTEGER,
        score_total INTEGER,
        date_calcul TEXT,
        FOREIGN KEY (equipe_id) REFERENCES equipes(id)
    )''')

    conn.commit()
    conn.close()
    print("BDD créée avec succès !")

def inserer_ligue(nom, pays, url):
    conn = sqlite3.connect("botfoot.db")
    c = conn.cursor()
    c.execute("INSERT INTO ligues (nom, pays, url) VALUES (?, ?, ?)", (nom, pays, url))
    conn.commit()
    ligue_id = c.lastrowid
    conn.close()
    return ligue_id

def inserer_equipe(nom, url_id, ligue_id):
    conn = sqlite3.connect("botfoot.db")
    c = conn.cursor()
    c.execute("INSERT INTO equipes (nom, url_id, ligue_id) VALUES (?, ?, ?)", (nom, url_id, ligue_id))
    conn.commit()
    conn.close()

def inserer_score(equipe_id, score_total, date_calcul):
    conn = sqlite3.connect("botfoot.db")
    c = conn.cursor()
    c.execute("INSERT INTO scores (equipe_id, score_total, date_calcul) VALUES (?, ?, ?)", 
              (equipe_id, score_total, date_calcul))
    conn.commit()
    conn.close()

if __name__ == "__main__":
    creer_bdd()