from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import sqlite3
import time
from datetime import datetime

options = webdriver.ChromeOptions()
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def creer_table_buteurs():
    conn = sqlite3.connect("botfoot.db")
    c = conn.cursor()
    # On supprime l'ancienne table et on recrée
    c.execute("DROP TABLE IF EXISTS buteurs")
    c.execute('''CREATE TABLE IF NOT EXISTS buteurs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nom TEXT,
        equipe TEXT,
        ligue TEXT,
        matchs INTEGER,
        buts INTEGER,
        passes INTEGER,
        note REAL,
        ratio REAL,
        score_buteur REAL,
        date_calcul TEXT
    )''')
    conn.commit()
    conn.close()

def get_joueurs_equipe(url_equipe):
    joueurs = {}
    driver.get(url_equipe)
    time.sleep(4)
    liens = driver.find_elements(By.TAG_NAME, "a")
    for lien in liens:
        href = lien.get_attribute("href")
        texte = lien.text.strip()
        if href and "/joueur/" in href and texte:
            joueurs[texte] = href
    return joueurs

def scraper_stats_joueur(nom, url, equipe, ligue):
    try:
        driver.get(url)
        time.sleep(3)

        body = driver.find_element(By.TAG_NAME, "body").text
        lignes = body.split("\n")

        for i, ligne in enumerate(lignes):
            if "2025/2026" in ligne:
                try:
                    note = float(lignes[i+3])
                    matchs = int(lignes[i+4])
                    buts = int(lignes[i+5])
                    passes = int(lignes[i+6])
                    ratio = round(buts / matchs, 2) if matchs > 0 else 0
                    score = round((buts * 3) + (ratio * 10) + note, 2)

                    # Stocker en BDD
                    conn = sqlite3.connect("botfoot.db")
                    c = conn.cursor()
                    c.execute('''INSERT INTO buteurs 
                        (nom, equipe, ligue, matchs, buts, passes, note, ratio, score_buteur, date_calcul)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (nom, equipe, ligue, matchs, buts, passes, note, ratio, score, 
                         datetime.now().strftime("%Y-%m-%d %H:%M")))
                    conn.commit()
                    conn.close()

                    if buts > 0:
                        print(f"⚽ {nom} ({equipe}) → {buts} buts | Note: {note} | Score: {score}")
                    break
                except:
                    pass
    except:
        pass

# Init
creer_table_buteurs()

# Test sur Nancy complète
try:
    print("Récupération des joueurs de Nancy...")
    joueurs = get_joueurs_equipe("https://www.flashscore.fr/equipe/nancy/dnIrsS0A/effectif/")
    print(f"{len(joueurs)} joueurs trouvés\n")

    for nom, url in joueurs.items():
        scraper_stats_joueur(nom, url, "Nancy", "Ligue 2")

    # Afficher le classement des buteurs de Nancy
    conn = sqlite3.connect("botfoot.db")
    c = conn.cursor()
    c.execute("SELECT nom, buts, note, score_buteur FROM buteurs WHERE equipe='Nancy' ORDER BY score_buteur DESC")
    resultats = c.fetchall()
    conn.close()

    print("\n🏆 CLASSEMENT BUTEURS NANCY")
    print("="*40)
    for i, (nom, buts, note, score) in enumerate(resultats, 1):
        print(f"{i}. {nom} → {buts} buts | Note: {note} | Score: {score}")

finally:
    driver.quit()