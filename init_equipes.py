import sqlite3
from database import inserer_ligue, inserer_equipe

ligues_equipes = {
    "Premier League": {
        "pays": "Angleterre",
        "url": "https://www.flashscore.fr/football/angleterre/premier-league/classement/",
        "equipes": {
            "Arsenal": "arsenal/hA1Zm19f",
            "Manchester City": "manchester-city/Wtn9Stg0",
            "Aston Villa": "aston-villa/W00wmLO0",
            "Manchester Utd": "manchester-utd/ppjDR086",
            "Chelsea": "chelsea/4fGZN2oK",
            "Liverpool": "liverpool/lId4TMwf",
            "Brentford": "brentford/xYe7DwID",
            "Bournemouth": "bournemouth/OtpNdwrc",
            "Everton": "everton/KluSTr9s",
            "Fulham": "fulham/69ZiU2Om",
            "Newcastle": "newcastle/p6ahwuwJ",
            "Sunderland": "sunderland/WSzc94ws",
            "Crystal Palace": "crystal-palace/AovF1Mia",
            "Brighton": "brighton/2XrRecc3",
            "Leeds": "leeds/tUxUbLR2",
            "Tottenham": "tottenham/UDg08Ohm",
            "Nottingham": "nottingham/UsushcZr",
            "West Ham": "west-ham/Cxq57r8g",
            "Burnley": "burnley/z3dmTMMO",
            "Wolves": "wolves/j3Azpf5d"
        }
    },
    "Liga": {
        "pays": "Espagne",
        "url": "https://www.flashscore.fr/football/espagne/laliga/classement/",
        "equipes": {
            "Barcelone": "barcelone/SKbpVP5K",
            "Real Madrid": "real-madrid/W8mj7MDD",
            "Villarreal": "villarreal/lUatW5jE",
            "Atl. Madrid": "atl-madrid/jaarqpLQ",
            "Betis": "betis/vJbTeCGP",
            "Celta Vigo": "celta-vigo/8pvUZFhf",
            "Espanyol": "espanyol/QFfPdh1J",
            "Ath. Bilbao": "ath-bilbao/IP5zl0cJ",
            "Osasuna": "osasuna/ETdxjU8a",
            "Real Sociedad": "real-sociedad/jNvak2f3",
            "Gérone": "girona/nNNpcUSL",
            "FC Séville": "sevilla/h8oAv4Ts",
            "Getafe": "getafe/dboeiWOt",
            "Alaves": "alaves/hxt57t2q",
            "Vallecano": "vallecano/8bcjFy6O",
            "Valence": "valencia/CQeaytrD",
            "Elche": "elche/4jl02tPF",
            "Majorque": "mallorca/4jDQxrbf",
            "Levante": "levante/G8FL0ShI",
            "Oviedo": "oviedo/SzYzw34K"
        }
    },
    "Serie A": {
        "pays": "Italie",
        "url": "https://www.flashscore.fr/football/italie/serie-a/classement/",
        "equipes": {
            "Inter": "inter/Iw7eKK25",
            "AC Milan": "ac-milan/8Sa8HInO",
            "Naples": "napoli/69Dxbc61",
            "AS Rome": "as-roma/zVqqL0ma",
            "Juventus": "juventus/C06aJvIB",
            "Como": "como/ttyLthOA",
            "Atalanta": "atalanta/8C9JjMXu",
            "Bologne": "bologna/0M9xNN8N",
            "Sassuolo": "sassuolo/QDdvI0zl",
            "Lazio": "lazio/URcSl02h",
            "Udinese": "udinese/rXw8YKDE",
            "Parme": "parma/6DxlaxHN",
            "Cagliari": "cagliari/SCGVmKHb",
            "Genoa": "genoa/d0PJxeie",
            "Torino": "torino/MZFZnvX4",
            "Fiorentina": "fiorentina/Q3A3IbXH",
            "Cremonese": "cremonese/KUzfp5N3",
            "Lecce": "lecce/G8lYsMgU",
            "Pisa": "pisa/roasMsOT",
            "Hellas Vérone": "verona/rJVAIaHo"
        }
    },
    "Bundesliga": {
        "pays": "Allemagne",
        "url": "https://www.flashscore.fr/football/allemagne/bundesliga/classement/",
        "equipes": {
            "Bayern Munich": "bayern/nVp0wiqd",
            "Dortmund": "dortmund/nP1i5US1",
            "Hoffenheim": "hoffenheim/hQAtP9Sl",
            "Stuttgart": "stuttgart/nJQmYp1B",
            "RB Leipzig": "rb-leipzig/KbS1suSm",
            "Bayer Leverkusen": "leverkusen/4jcj2zMd",
            "Fribourg": "freiburg/fiEQZ7C7",
            "Eintracht Francfort": "frankfurt/8vndvXTk",
            "Union Berlin": "union-berlin/pzHW4oaE",
            "Augsburg": "augsburg/fTVNku3I",
            "Hambourg SV": "hamburger/v9k3aY5F",
            "FC Cologne": "koln/WG9pOTse",
            "Mayence": "mainz/EuakNmc1",
            "B. Monchengladbach": "b-monchengladbach/88HSzjDr",
            "Wolfsburg": "wolfsburg/nwkTahLL",
            "St. Pauli": "st-pauli/ILyJuN3g",
            "Werder Brême": "bremen/Ig1f1fy3",
            "Heidenheim": "heidenheim/KWixEVWi"
        }
    },
    "Ligue 1": {
        "pays": "France",
        "url": "https://www.flashscore.fr/football/france/ligue-1/classement/",
        "equipes": {
            "PSG": "psg/CjhkPw0k",
            "Lens": "lens/IBmris38",
            "Lyon": "lyon/2akflumR",
            "Marseille": "marseille/SblU3Hee",
            "Lille": "lille/pfDZL71o",
            "Rennes": "rennes/d2nnj1IE",
            "Strasbourg": "strasbourg/nP6UzIU1",
            "Monaco": "monaco/2PIvr8o4",
            "Lorient": "lorient/jgNAYRGi",
            "Toulouse": "toulouse/MLmY2yB1",
            "Brest": "brest/Cr4VGaUl",
            "Angers": "angers/SAlF91iL",
            "Le Havre": "le-havre/CIEe04GT",
            "Nice": "nice/YagoQJpq",
            "Paris FC": "paris-fc/0OEHEprs",
            "Auxerre": "auxerre/MTLr36WA",
            "Nantes": "nantes/veuetnGG",
            "Metz": "metz/4v0yqlWc"
        }
    },
    "Ligue 2": {
        "pays": "France",
        "url": "https://www.flashscore.fr/football/france/ligue-2/classement/",
        "equipes": {
            "Troyes": "troyes/fsfAHubD",
            "Saint-Étienne": "st-etienne/YL2QybFe",
            "Reims": "reims/tItR6sEf",
            "Le Mans": "le-mans/ER0NeOOp",
            "Red Star": "red-star/2kM6ufaS",
            "Dunkerque": "dunkerque/tAAobNse",
            "Annecy": "annecy/25dB6Eto",
            "Rodez": "rodez/0pOdQOCg",
            "Montpellier": "montpellier/Eyf00syR",
            "Guingamp": "guingamp/QDfKl36E",
            "Pau FC": "pau/6NOhR4cm",
            "Grenoble": "grenoble/UgPj1p1N",
            "Nancy": "nancy/dnIrsS0A",
            "Boulogne": "boulogne/IL4RHuFr",
            "Clermont": "clermont/IRuMRua1",
            "Amiens": "amiens-sc/lKkBAsxF",
            "Bastia": "bastia/AZwK8L6R",
            "Laval": "laval/E1wDrLsJ"
        }
    }
}

for nom_ligue, data in ligues_equipes.items():
    ligue_id = inserer_ligue(nom_ligue, data["pays"], data["url"])
    for nom_equipe, url_id in data["equipes"].items():
        inserer_equipe(nom_equipe, url_id, ligue_id)
    print(f"✅ {nom_ligue} → {len(data['equipes'])} équipes insérées")

print("\n🏆 BDD initialisée avec succès !")