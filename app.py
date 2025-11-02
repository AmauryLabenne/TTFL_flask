from flask import Flask, render_template, request, jsonify
import pandas as pd
import os
from datetime import datetime, timedelta
from unidecode import unidecode


# Import de tes fonctions existantes
from core.get_calendar import fetch_nba_games, create_games_dataframe, process_match_day, get_detailed_stats_player
from core.get_db import process_player_logs
from core.injuries_scrapper import get_nba_injuries_cbs

app = Flask(__name__)

@app.route("/")
def home():
    return render_template('index.html', datetime=datetime)

@app.route("/tableau")
def tableau():
    print(">>> PARAMETRE RECU :", request.args)
    # --- 1. Chargement des données sources ---
    df_upcoming_matches = create_games_dataframe(fetch_nba_games())
    # df_logs_last_year = pd.read_csv("data/player_game_logs_2024-25.csv")
    df_logs_last_year = pd.DataFrame()
    # df_logs_now = pd.read_csv("data/player_game_logs_2025-26.csv")  # ou autre fichier
    df_logs_now = process_player_logs(season="2025-26")

    # --- 2. Récupération de la date depuis le paramètre GET ---
    # --- 2. Récupération de la date depuis le formulaire ---
    sel_date = request.args.get("date")
    print(sel_date)

    if sel_date:
        try:
            day_to_check = datetime.strptime(sel_date, "%Y-%m-%d")
            sel_date = day_to_check.strftime("%Y-%m-%d")
        except ValueError:
            day_to_check = datetime.today()
            sel_date = day_to_check.strftime("%Y-%m-%d")
    else:
        day_to_check = datetime.today()
        sel_date = day_to_check.strftime("%Y-%m-%d")

    # --- 3. Calcul du tableau principal ---
    tab_day = process_match_day(
        day_to_check,
        df_logs_last_year,
        df_logs_now,
        df_upcoming_matches,
        save_csv=False
    )
    # print(tab_day)


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
    print("AAAAAAAAAAAAAAAAAAAAAAAAAAA")
    print(tab_final)
    columns = tab_final.columns
    df_display = tab_final[columns]

    df_display= df_display.sort_values(by="Saison", ascending=False)

    def format_player_name(name):
        # Coupe au premier espace et met un retour à la ligne après
        parts = name.split(' ', 1)
        if len(parts) > 1:
            return parts[0] + "<br>" + parts[1]
        return name

    def format_bool(b):
        return "✔️" if b else ""
    
    # df_display["Player"] = df_display["Player"].apply(format_player_name)
    df_display["B2B"] = df_display["B2B"].apply(format_bool)

    # Deal with nan and round on numeric
    num_cols = df_display.select_dtypes(include='number').columns
    print(num_cols)
    print(df_display.head())
    df_display[num_cols] = df_display[num_cols].fillna(0)
    df_display[num_cols] = df_display[num_cols].round(0).astype(int)

    # Trunk Opponent : Remplacer "@ XXX" → "@XXX" et "VS XXX" → "XXX"
    df_display['Adversaire'] = df_display['Adversaire'].str.replace(r'@[\s]+', '@', regex=True)
    df_display['Adversaire'] = df_display['Adversaire'].str.replace(r'\bVS\s+', '', regex=True)



    # --- 6. Conversion pour affichage ---
    data = df_display.to_dict(orient="records")

    return render_template("tableau.html", columns=columns, data=data, sel_date=sel_date)

@app.route("/joueurs", methods=["GET", "POST"])
def joueurs():
    player_data = None
    player_name = ""

    # Charger les données (même source que ton tableau)
    df_logs_now = process_player_logs(season="2025-26")

    df_out = None
    if request.method == "POST":
        player_name = request.form.get("player_name", "").strip()
        print(player_name)
        
        if player_name != "":
            # Filtrer les joueurs correspondants (insensible à la casse)
            # Filtrer les joueurs correspondants (insensible à la casse)
            print("AAAAAAAAAAAAAAAA")
            print(df_logs_now['PLAYER_NAME'].str.contains(player_name, case=False))
            player_info = df_logs_now[df_logs_now['PLAYER_NAME'].str.contains(player_name, case=False)]
            print(player_info.head())
            print(player_info.columns)
            if not player_info.empty:
               out_cols = ['PLAYER_NAME', 'GAME_DATE', 'MATCHUP', "score_ttfl", 'WL', 'MIN']
               df_out = player_info[out_cols]
               df_out = df_out.sort_values(by="GAME_DATE")
               # stats_df = get_detailed_stats_player(player_info)
                
    return render_template("joueurs.html", player_data=df_out, player_name=player_name)

@app.route("/api/suggest_players")
def suggest_players():
    query = request.args.get("q", "").strip().lower()
    if not query:
        return jsonify([])

    df_logs_now = pd.read_csv("data/player_game_logs_2024-25.csv")
    players = df_logs_now["PLAYER_NAME"].dropna().unique()

    matches = [p for p in players if query in p.lower()][:10]  # max 10 résultats
    return jsonify(matches)

@app.route("/joueur/<player_name>")
def joueur(player_name):
    player_name = player_name.strip()
    print("BBBBBBBBBBBBBBBB")
    print(player_name)
    
    # Charger les données comme dans /joueurs
    df_logs_now = process_player_logs(season="2025-26")
    
    # Filtrer le joueur
    player_info = df_logs_now[df_logs_now['PLAYER_NAME'].str.contains(player_name, case=False)]
    
    if player_info.empty:
        return render_template("joueur.html", player_name=player_name, player_data=None)
    
    # Colonnes à afficher
    out_cols = ['PLAYER_NAME', 'GAME_DATE', 'MATCHUP', "score_ttfl", 'WL', 'MIN']
    df_out = player_info[out_cols].sort_values(by="GAME_DATE")
    df_out['GAME_DATE'] = pd.to_datetime(df_out['GAME_DATE']).dt.strftime("%Y-%m-%d")


    data = df_out.to_dict(orient="records")
    
    return render_template("joueur.html", player_name=player_name, player_data=data)
       

if __name__ == "__main__":
    app.run(debug=True)
