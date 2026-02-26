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
options.add_argument("--headless")  # 👈 Pas de fenêtre = plus rapide
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")
options.add_argument("--disable-images")  # 👈 Pas d'images = plus rapide
options.add_argument("--blink-settings=imagesEnabled=false")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def creer_table_buteurs():
    conn = sqlite3.connect("botfoot.db")
    c = conn.cursor()
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

def get_toutes_equipes():
    conn = sqlite3.connect("botfoot.db")
    c = conn.cursor()
    c.execute("SELECT e.nom, e.url_id, l.nom FROM equipes e JOIN ligues l ON e.ligue_id = l.id")
    equipes = c.fetchall()
    conn.close()
    return equipes

def get_joueurs_equipe(url_id):
    joueurs = {}
    driver.get(f"https://www.flashscore.fr/equipe/{url_id}/effectif/")
    time.sleep(2)  # 👈 Réduit de 4 à 2
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
        time.sleep(2)  # 👈 Réduit de 4 à 2

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

                    conn = sqlite3.connect("botfoot.db")
                    c = conn.cursor()
                    c.execute('''INSERT INTO buteurs 
                        (nom, equipe, ligue, matchs, buts, passes, note, ratio, score_buteur, date_calcul)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                        (nom, equipe, ligue, matchs, buts, passes, note, ratio, score,
                         datetime.now().strftime("%Y-%m-%d %H:%M")))
                    conn.commit()
                    conn.close()
                    break
                except:
                    pass
    except:
        pass

# Init
creer_table_buteurs()

try:
    equipes = get_toutes_equipes()
    total = len(equipes)
    
    for idx, (nom_equipe, url_id, ligue) in enumerate(equipes, 1):
        print(f"[{idx}/{total}] {ligue} - {nom_equipe}...")
        joueurs = get_joueurs_equipe(url_id)
        for nom_joueur, url_joueur in joueurs.items():
            scraper_stats_joueur(nom_joueur, url_joueur, nom_equipe, ligue)

finally:
    driver.quit()

# Affichage top 30 buteurs
conn = sqlite3.connect("botfoot.db")
c = conn.cursor()
c.execute("""
    SELECT nom, equipe, ligue, buts, note, score_buteur 
    FROM buteurs 
    WHERE buts > 0
    ORDER BY score_buteur DESC 
    LIMIT 30
""")
resultats = c.fetchall()
conn.close()

print("\n🏆 TOP 30 BUTEURS TOUTES LIGUES")
print("="*50)
for i, (nom, equipe, ligue, buts, note, score) in enumerate(resultats, 1):
    print(f"{i}. {nom} ({equipe} - {ligue}) → {buts} buts | Note: {note} | Score: {score}")
    