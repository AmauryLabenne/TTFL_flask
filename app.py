from flask import Flask, render_template
import pandas as pd
import os
from datetime import datetime, timedelta
from unidecode import unidecode

# Import de tes fonctions existantes
from core.get_calendar import fetch_nba_games, create_games_dataframe, process_match_day
# from core.get_db import process_match_day
from core.injuries_scrapper import get_nba_injuries_cbs

app = Flask(__name__)

@app.route("/")
def home():
    return '<a href="/tableau">Voir le tableau TTFL</a>'

@app.route("/tableau")
def tableau():
    # --- 1. Chargement des données sources ---
    df_upcoming_matches = create_games_dataframe(fetch_nba_games())
    df_logs_last_year = pd.read_csv("data/player_game_logs_2024-25.csv")
    df_logs_now = pd.read_csv("data/player_game_logs_2024-25.csv")  # ou autre fichier

    # --- 2. Date du jour ---
    sel_date = datetime.today().strftime("%Y-%m-%d")
    day_to_check = datetime.strptime(sel_date, "%Y-%m-%d")

    # --- 3. Calcul du tableau principal ---
    tab_day = process_match_day(
        day_to_check,
        df_logs_last_year,
        df_logs_now,
        df_upcoming_matches,
        save_csv=False
    )
    # print(tab_day)
    tab_day["Player"] = tab_day["Player"].apply(unidecode)

    # --- 4. Gestion des blessures ---
    df_injuries = get_nba_injuries_cbs()
    tab_final = pd.merge(
        tab_day,
        df_injuries[["Player", "Status"]],
        how="left",
        on="Player"
    )
    tab_final["Status"] = tab_final["Status"].fillna("OK")

    # --- 5. Sélection des colonnes ---
    columns = [
        "Player", "Team", "Status",
        "Score TTFL moyen saison",
        "Score TTFL 10 derniers jours",
        "Points moyen",
        "Assists moyen",
        "Rebonds moyen"
    ]
    # print("AAAAAAAAAAAAAAAAAAAAAAAAAAA")
    # print(tab_final)
    columns = tab_final.columns
    df_display = tab_final[columns]
    
    df_display= df_display.sort_values(by="Saison", ascending=False)

    # --- 6. Conversion pour affichage ---
    data = df_display.to_dict(orient="records")

    return render_template("tableau.html", columns=columns, data=data)
    

if __name__ == "__main__":
    app.run(debug=True)
