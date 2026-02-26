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

def get_equipes():
    conn = sqlite3.connect("botfoot.db")
    c = conn.cursor()
    c.execute("""
        SELECT e.id, e.nom, e.url_id, l.nom 
        FROM equipes e 
        JOIN ligues l ON e.ligue_id = l.id
    """)
    equipes = c.fetchall()
    conn.close()
    return equipes

def calculer_et_stocker(equipe_id, nom, url_id, ligue):
    score_total = 0
    try:
        driver.get(f"https://www.flashscore.fr/equipe/{url_id}/resultats/")
        time.sleep(4)

        elements = driver.find_elements(By.CLASS_NAME, "event__match")
        resultats = []

        for el in elements:
            try:
                texte = el.text.strip().split("\n")
                texte = [t for t in texte if t not in ["Après TAB", "Après Prolongations", "PREVIEW", "-"]]

                if len(texte) >= 5:
                    domicile = texte[1]
                    exterieur = texte[2]
                    but_dom = int(texte[3]) if texte[3].isdigit() else 0
                    but_ext = int(texte[4]) if texte[4].isdigit() else 0
                    resultat = texte[-1]

                    if nom.lower() in domicile.lower():
                        buts_marques = but_dom
                        buts_encaisses = but_ext
                    else:
                        buts_marques = but_ext
                        buts_encaisses = but_dom

                    resultats.append({
                        "resultat": resultat,
                        "buts_marques": buts_marques,
                        "buts_encaisses": buts_encaisses
                    })
            except:
                pass

        for i, r in enumerate(resultats):
            multiplicateur = 2 if i < 5 else 1
            points = 0
            if r["resultat"] == "V":
                points += 3
            elif r["resultat"] == "N":
                points += 1
            elif r["resultat"] == "D":
                points -= 3
            points += r["buts_marques"]
            points -= r["buts_encaisses"]
            score_total += points * multiplicateur

        # Stocker en BDD
        conn = sqlite3.connect("botfoot.db")
        c = conn.cursor()
        c.execute("INSERT INTO scores (equipe_id, score_total, date_calcul) VALUES (?, ?, ?)",
                  (equipe_id, score_total, datetime.now().strftime("%Y-%m-%d %H:%M")))
        conn.commit()
        conn.close()

    except Exception as e:
        print(f"Erreur {nom}: {e}")

    return score_total

# Lancement
equipes = get_equipes()
scores = {}

try:
    for equipe_id, nom, url_id, ligue in equipes:
        print(f"Analyse {ligue} - {nom}...")
        score = calculer_et_stocker(equipe_id, nom, url_id, ligue)
        scores[(ligue, nom)] = score

finally:
    driver.quit()

# Affichage par ligue
ligues = {}
for (ligue, nom), score in scores.items():
    if ligue not in ligues:
        ligues[ligue] = []
    ligues[ligue].append((nom, score))

for ligue, equipes_scores in ligues.items():
    classement = sorted(equipes_scores, key=lambda x: x[1], reverse=True)
    print(f"\n🏆 {ligue}")
    print("="*40)
    for i, (nom, score) in enumerate(classement, 1):
        print(f"{i}. {nom} → {score} pts")
        