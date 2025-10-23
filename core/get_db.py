import pandas as pd
import time
from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import commonteamroster
from nba_api.stats.endpoints import PlayerGameLogs

# Récupérer les équipes NBA
def fetch_nba_teams():
    return teams.get_teams()

# Récupérer les joueurs d'une équipe donnée
def fetch_team_roster(team_id):
    roster = commonteamroster.CommonTeamRoster(team_id=team_id).get_dict()
    return roster['resultSets'][0]['rowSet']  # Liste des joueurs

# Extraire les informations d'un joueur
def extract_player_info(player, team_name):
    return {
        "PLAYER_ID": player[14],
        "Player": player[3],  # Nom du joueur
        "Team": team_name,    # Nom de l'équipe
        "Position": player[7] # Position du joueur
    }

# Récupérer les informations de tous les joueurs de la NBA
def fetch_all_players_data():
    nba_teams = fetch_nba_teams()
    all_players_data = []

    for team in nba_teams:
        #time.sleep(1)
        team_id = team['id']
        team_name = team['full_name']
        players = fetch_team_roster(team_id)

        # Extraire les informations des joueurs
        for player in players:
            player_info = extract_player_info(player, team_name)
            all_players_data.append(player_info)
    
    return pd.DataFrame(all_players_data)

# Sauvegarder les données dans un fichier CSV
def save_players_data(df, filename):
    df.to_csv(filename, index=False)

# Pipeline complet
def process_nba_players():
    df_players = fetch_all_players_data()
    print(df_players.head(10))
    save_players_data(df_players, '../data/nba_players_by_team.csv')


##############################################################
# Fonction pour récupérer les données des matchs des joueurs
def fetch_player_game_logs(season='2024-25', season_type='Regular Season'):
    player_game_logs = PlayerGameLogs(season_nullable=season, season_type_nullable=season_type,league_id_nullable='00')
    return player_game_logs.get_data_frames()[0]

def add_players_info(df, players_info):
    res = pd.merge(df, players_info[["PLAYER_ID", "Position","Team"]], on=['PLAYER_ID', 'PLAYER_ID'], how='left')
    # Replace team name by new team name
    res['TEAM_NAME'] = res["Team"].replace({'Los Angeles Clippers': 'LA Clippers'})
    return(res)

# Calculer le score TTFL
def calculate_ttfl_score(df):
    df["score_ttfl"] = (
        df['FGM'] + df['FG3M'] + df['FTM'] + df['REB'] + df['AST'] + df['STL'] +
        df['BLK'] + df['PTS'] - df['TOV'] - (df['FGA'] - df['FGM']) -
        (df['FG3A'] - df['FG3M']) - (df['FTA'] - df['FTM'])
    )
    return df

# Extraire l'équipe adverse
def extract_away_team(matchup):
    return matchup.split()[2] 

# Déterminer le lieu du match (domicile/extérieur)
def determine_location(matchup):
    return 'Away' if '@' in matchup else 'Home'

# Marquer les matchs joués en back-to-back
def is_back_to_back(row, df):
    player = row['PLAYER_NAME']
    current_date = row['GAME_DATE']
    previous_match = df[(df['PLAYER_NAME'] == player) & (df['GAME_DATE'] == current_date - pd.Timedelta(days=1))]
    next_match = df[(df['PLAYER_NAME'] == player) & (df['GAME_DATE'] == current_date + pd.Timedelta(days=1))]
    return not previous_match.empty or not next_match.empty

def is_back_to_back_2(df):
    # Sort dataframe by player and game date
    df = df.sort_values(by=['PLAYER_NAME', 'GAME_DATE'])

    # Create shifted columns for previous and next game dates for each player
    df['PREV_GAME_DATE'] = df.groupby('PLAYER_NAME')['GAME_DATE'].shift(1)
    df['NEXT_GAME_DATE'] = df.groupby('PLAYER_NAME')['GAME_DATE'].shift(-1)

    # Calculate if the current game is back-to-back with the previous or next game
    df['B2B'] = (
            (df['GAME_DATE'] - df['PREV_GAME_DATE'] == pd.Timedelta(days=1)) |
            (df['NEXT_GAME_DATE'] - df['GAME_DATE'] == pd.Timedelta(days=1))
    )

    return df

# Ajouter des colonnes dérivées
def enrich_game_logs(df):
    df['Opponent'] = df['MATCHUP'].apply(extract_away_team)
    df['Location'] = df['MATCHUP'].apply(determine_location)
    df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'])
    df = df.sort_values(by=['PLAYER_NAME', 'GAME_DATE'])
    # df['Back to Back'] = df.apply(is_back_to_back, axis=1, args=(df,))
    df = is_back_to_back_2(df)
    return df

# Calcul des moyennes des derniers matchs
def calculate_moving_averages(df):
    df['Last_5_Avg_PTS'] = df.groupby('PLAYER_ID')['score_ttfl'].transform(lambda x: x.rolling(window=5, min_periods=1).mean())
    return df

# Calcul des moyennes par adversaire, position, etc.
# Question : is it pertinent to create all indicators here knowing that we can calculate them in process_match_day()
def calculate_grouped_averages(df):
    # Moyenne des 3 derniers matchs contre un adversaire
    last_3_vs_opponent = df.groupby(['PLAYER_ID', 'Opponent']).tail(3).groupby(['PLAYER_ID', 'Opponent']).agg({'PTS': 'mean'}).reset_index()
    last_3_vs_opponent.rename(columns={'PTS': 'Last_3_Avg_PTS_vs_team'}, inplace=True)

    # Calculer la différence (écart) entre la moyenne des points par poste et celle par adversaire
    impact_by_position = df[['Position','Opponent','score_ttfl']].copy()
    impact_by_position['Avg_PTS_by_Position'] = impact_by_position.groupby(['Position', 'Opponent'])['score_ttfl'].transform('mean')
    impact_by_position['avg_pts_by_opponent'] = impact_by_position.groupby(['Opponent'])['score_ttfl'].transform('mean')
    impact_by_position['impact_position_vs_opponent'] = impact_by_position['Avg_PTS_by_Position'] - impact_by_position['avg_pts_by_opponent'] 
    impact_by_position = impact_by_position[['Position', 'Opponent', 'impact_position_vs_opponent']].drop_duplicates()
    #Impact de la locatisation
    home_away_avg = df.groupby(['PLAYER_ID', 'Location']).agg({'score_ttfl': 'mean'}).reset_index()
    player_avg = df.groupby(['PLAYER_ID'])['score_ttfl'].mean().reset_index()
    player_avg.rename(columns={'score_ttfl': 'Avg_PTS_Overall'}, inplace=True)
    home_away_avg = home_away_avg.merge(player_avg, on='PLAYER_ID', how='left')
    home_away_avg['Avg_PTS_Home_Away'] = home_away_avg['score_ttfl'] - home_away_avg['Avg_PTS_Overall']
    home_away_avg = home_away_avg[['PLAYER_ID', 'Location', 'Avg_PTS_Home_Away']]

    # Moyenne en back-to-back
    back_to_back_avg = df.groupby(['PLAYER_ID', 'B2B']).agg({'score_ttfl': 'mean'}).reset_index() 
    back_to_back_avg.rename(columns={'score_ttfl': 'Avg_PTS_B2B'}, inplace=True)

    return last_3_vs_opponent, impact_by_position, home_away_avg, back_to_back_avg

# Fusionner les données dérivées avec le DataFrame original
def merge_data(df, last_3_vs_opponent, impact_by_position, home_away_avg, back_to_back_avg):
    final_df = pd.merge(df,impact_by_position, on=['Position', 'Opponent'], how='outer')
    final_df = pd.merge(final_df, home_away_avg, on=['PLAYER_ID', 'Location'], how='outer')
    final_df = pd.merge(final_df, back_to_back_avg, on=['PLAYER_ID', 'B2B'], how='outer')
    final_df = pd.merge(final_df, last_3_vs_opponent, on=['PLAYER_ID', 'Opponent'], how='outer')
    return final_df

# Sauvegarder les résultats
def save_to_csv(df, filename):
    df.to_csv(filename, index=False)

# Pipeline complet
def process_player_logs(season='2024-25', season_type='Regular Season', save_csv=False):
    print("Fetch player game logs")
    df = fetch_player_game_logs(season=season, season_type=season_type)

    nba_teams = [
        'ATL', 'BOS', 'BKN', 'CHA', 'CHI', 'CLE', 'DAL', 'DEN', 'DET', 
        'GSW', 'HOU', 'IND', 'LAL', 'LAC', 'MEM', 'MIA', 'MIL', 'MIN', 
        'NOP', 'NYK', 'OKC', 'ORL', 'PHI', 'PHX', 'POR', 'SAC', 'SAS', 
        'TOR', 'UTA', 'WAS'
    ]
    df = df[df['TEAM_ABBREVIATION'].isin(nba_teams)]
    

    print("Add player info")
    players_info = pd.read_csv("./data/nba_players_by_team.csv")
    df = add_players_info(df, players_info)

    print("Calculate TTFL score")
    df = calculate_ttfl_score(df)

    print("Enrich game logs (opponent, Date, B2B,...")
    df = enrich_game_logs(df)

    print("Calculate moving average")
    df = calculate_moving_averages(df)

    #save_to_csv(df, 'data/player_game_logs_2022_23_pred_temp.csv')

    print("Computed average by group (opponent, position, home_away, B2B")
    last_3_vs_opponent, impact_by_position, home_away_avg, back_to_back_avg = calculate_grouped_averages(df)
    final_df = merge_data(df, last_3_vs_opponent, impact_by_position, home_away_avg, back_to_back_avg)

    if (save_csv):
        file_name = f"../data/player_game_logs_{season}.csv"
        save_to_csv(final_df, file_name)
    return(final_df)

# Exécuter le pipeline
if __name__ == "__main__":

    process_nba_players()

    process_player_logs(season="2024-25",save_csv=True)