from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time

options = webdriver.ChromeOptions()
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

ligues = {
    "Premier League": "https://www.flashscore.fr/football/angleterre/premier-league/classement/",
    "Liga": "https://www.flashscore.fr/football/espagne/laliga/classement/",
    "Serie A": "https://www.flashscore.fr/football/italie/serie-a/classement/",
    "Bundesliga": "https://www.flashscore.fr/football/allemagne/bundesliga/classement/",
    "Ligue 1": "https://www.flashscore.fr/football/france/ligue-1/classement/",
}

toutes_equipes = {}

try:
    for nom_ligue, url in ligues.items():
        print(f"\nRécupération de {nom_ligue}...")
        driver.get(url)
        time.sleep(4)

        liens = driver.find_elements(By.TAG_NAME, "a")
        equipes_ligue = {}

        for lien in liens:
            href = lien.get_attribute("href")
            texte = lien.text.strip()
            if href and "/equipe/" in href and texte:
                url_id = "/".join(href.rstrip("/").split("/")[-2:])
                equipes_ligue[texte] = url_id

        toutes_equipes[nom_ligue] = equipes_ligue
        print(f"{len(equipes_ligue)} équipes trouvées")
        for nom, uid in equipes_ligue.items():
            print(f"  {nom} → {uid}")

finally:
    driver.quit()