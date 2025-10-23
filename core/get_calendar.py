import http.client
import json  
import pandas as pd  
from datetime import datetime, timedelta
from pytz import UTC
from core.get_db import *

# Connexion to NBA API (RapidAPI)
def fetch_nba_games():
    conn = http.client.HTTPSConnection("api-nba-v1.p.rapidapi.com")
    headers = {
        'x-rapidapi-key': "79b88cc3c5msh1d30692405caa0cp191069jsn86b832042652",
        'x-rapidapi-host': "api-nba-v1.p.rapidapi.com"
    }
    conn.request("GET", "/games?season=2025", headers=headers)
    res = conn.getresponse()
    data = res.read()
    conn.close()
    return json.loads(data.decode("utf-8"))['response']

# Retrieving matches and creating the DataFrame
def create_games_dataframe(games):
    game_data = [{
        'date': game['date']['start'],
        'home_team': game['teams']['home']['name'],
        'away_team': game['teams']['visitors']['name'],
        'league': game['league']
    } for game in games]

    df = pd.DataFrame(game_data)
    df['date'] = pd.to_datetime(df['date']).dt.tz_convert(None) - timedelta(hours=4)
    return df[df['date'] > datetime(2025, 10, 20)]

# Mapping of NBA teams
team_mapping = {
    'Atlanta Hawks': 'ATL', 'Boston Celtics': 'BOS', 'Brooklyn Nets': 'BKN', 
    'Charlotte Hornets': 'CHA', 'Chicago Bulls': 'CHI', 'Cleveland Cavaliers': 'CLE', 
    'Dallas Mavericks': 'DAL', 'Denver Nuggets': 'DEN', 'Detroit Pistons': 'DET', 
    'Golden State Warriors': 'GSW', 'Houston Rockets': 'HOU', 'Indiana Pacers': 'IND', 
    'Los Angeles Lakers': 'LAL','LA Clippers': 'LAC', 'Memphis Grizzlies': 'MEM', 'Miami Heat': 'MIA', 
    'Milwaukee Bucks': 'MIL', 'Minnesota Timberwolves': 'MIN', 'New Orleans Pelicans': 'NOP', 
    'New York Knicks': 'NYK', 'Oklahoma City Thunder': 'OKC', 'Orlando Magic': 'ORL', 
    'Philadelphia 76ers': 'PHI', 'Phoenix Suns': 'PHX', 'Portland Trail Blazers': 'POR', 
    'Sacramento Kings': 'SAC', 'San Antonio Spurs': 'SAS', 'Toronto Raptors': 'TOR', 
    'Utah Jazz': 'UTA', 'Washington Wizards': 'WAS'
}

# Function for processing the day's matches
def process_match_day(day_to_check, df_logs_last_year, df_logs_now, df_upcoming_matches, save_csv=False):

    day_before = day_to_check - timedelta(days=1)
    # Filter day's and yesterday's matches
    df_today_matches = df_upcoming_matches[df_upcoming_matches['date'].dt.normalize() == day_to_check]
    df_yesterday_matches = df_upcoming_matches[df_upcoming_matches['date'].dt.normalize() == day_before]

    teams_playing_today = df_today_matches['home_team'].tolist() + df_today_matches['away_team'].tolist()
    teams_playing_yesterday = df_yesterday_matches['home_team'].tolist() + df_yesterday_matches['away_team'].tolist()
    
    # Filter on players for tonight
    df_today_players_last_year = df_logs_last_year[df_logs_last_year['TEAM_NAME'].isin(teams_playing_today)].copy()
    df_today_players_last_year['Season'] = 'Last'
    df_today_players = df_logs_now[df_logs_now['TEAM_NAME'].isin(teams_playing_today)].copy()
    df_today_players['Season'] = 'Current'
    #df_today_players = pd.concat([df_today_players_last_year, df_today_players_now])



    # Wether the player has played yesterday or not
    df_today_players.loc[:, 'is_backtoback'] = df_today_players['TEAM_NAME'].isin(teams_playing_yesterday)

    # Get games with opponent
    df_today_matches_expanded = pd.concat([
        df_today_matches[['home_team', 'away_team']].assign(TEAM_NAME=df_today_matches['home_team'], opponent=df_today_matches['away_team'], is_home="Home"),
        df_today_matches[['home_team', 'away_team']].assign(TEAM_NAME=df_today_matches['away_team'], opponent=df_today_matches['home_team'], is_home="Away")
    ])

    # Merge players and opponents
    df_today_players = df_today_players.merge(df_today_matches_expanded[['TEAM_NAME', 'opponent', 'is_home']], on='TEAM_NAME', how='left')
    df_today_players = df_today_players[df_today_players['opponent'].isin(teams_playing_today)]
    df_today_players['opponent'] = df_today_players['opponent'].replace(team_mapping)
    df_today_players['TEAM_NAME'] = df_today_players['TEAM_NAME'].replace(team_mapping)

# Get stats vs opponent on past AND current season
    previous_games=df_today_players[['PLAYER_NAME','score_ttfl','opponent','Opponent',"is_home","Location"]].copy()
    
    # Calculating avg home/away
    previous_games_location = previous_games[previous_games['is_home']==previous_games['Location']].copy()
    previous_games_location['impact_location'] = previous_games_location.groupby(['PLAYER_NAME'])['score_ttfl'].transform('mean')
    
    # Calculating avg vs opponent
    previous_games=previous_games[previous_games['opponent'] == previous_games['Opponent']].copy()
    previous_games['score_ttfl_vs_opponent'] = previous_games.groupby(['PLAYER_NAME', 'opponent'])['score_ttfl'].transform('mean')
    previous_games['games_played_against_opponent'] = previous_games.groupby(['PLAYER_NAME', 'opponent'])['score_ttfl'].transform('count')


    # Get mean score only on current season
    current_mean = df_today_players[['PLAYER_NAME', "score_ttfl"]].copy()
    current_mean['mean_score_ttfl'] = current_mean.groupby(['PLAYER_NAME'])['score_ttfl'].transform('mean')
    df_today_players = pd.merge(df_today_players, current_mean[['PLAYER_NAME', "mean_score_ttfl"]].drop_duplicates(), how='left', on="PLAYER_NAME")


    # Mean over 30 last days

    thirty_days_ago = day_to_check - timedelta(days=30)
    df_logs_now['GAME_DATE'] = pd.to_datetime(df_logs_now['GAME_DATE'])
    df_last_30_days = df_logs_now[df_logs_now['GAME_DATE'] >= thirty_days_ago]
    mean_30_days = df_last_30_days.groupby('PLAYER_NAME')['score_ttfl'].mean().reset_index()
    mean_30_days.rename(columns={'score_ttfl': 'mean_30_days'}, inplace=True)

    # Merge with df_today_players
    df_today_players = df_today_players.merge(mean_30_days, on='PLAYER_NAME', how='left')
    # Merge with tables of indicators
    df_today_players = pd.merge(df_today_players,previous_games[['PLAYER_NAME','score_ttfl_vs_opponent','games_played_against_opponent']].drop_duplicates(),how="left",on="PLAYER_NAME")
    df_today_players = pd.merge(df_today_players,previous_games_location[['PLAYER_NAME','impact_location']].drop_duplicates(),how="left",on="PLAYER_NAME")

    # Get home away
    df_today_players['impact_location'] = df_today_players['impact_location'] - df_today_players['mean_score_ttfl']
    df_today_players = df_today_players.sort_values(by='PLAYER_NAME', ascending=True)

    # Obtain a table of impact pos vs opp
    df_pos_vs_opp = df_logs_now[['Position','Opponent','impact_position_vs_opponent']].drop_duplicates().copy()
    
    # Filter on current location to do not get two times 'impact_location'
    df_today_players = df_today_players[df_today_players['is_home']==df_today_players['Location']].copy()


    # Get final table
    df_today_summary = df_today_players[['PLAYER_NAME', 'TEAM_NAME', 'opponent', 'Location', 'Position', 'is_backtoback',
                                     'mean_score_ttfl', 'mean_30_days', 'score_ttfl_vs_opponent', 'games_played_against_opponent',
                                     'impact_location']].drop_duplicates()
    # print(df_today_summary)

    # Merge with the table of pos vs opp 
    df_today_summary = pd.merge(df_today_summary,df_pos_vs_opp[['Position','Opponent','impact_position_vs_opponent']],left_on=['Position', 'opponent'],right_on=['Position', 'Opponent'],how='left')

    # Recode oppponent with Home away (VS @)
    df_today_summary = df_today_summary.sort_values(by='mean_30_days', ascending=False)
    df_today_summary['opponent'] = df_today_summary.apply(lambda x: f" VS {x['opponent']}" if x['Location'] == 'Home' else f"@ {x['opponent']}", axis=1)
    df_today_summary.pop('Location')
    df_today_summary.pop('Opponent')

    df_today_summary = df_today_summary.rename(columns={
        'PLAYER_NAME': 'Player',
        'TEAM_NAME': 'Team',
        'opponent': 'Adversaire',
        'impact_location': 'Impact H/@',
        'mean_score_ttfl': "Saison",
        'mean_30_days': 'Moy 30j',
        'score_ttfl_vs_opponent': "Hist VS adv",
        'games_played_against_opponent': "Games VS adv", # pas forcément pertinent juste remontrer les n derniers matchs
        #'Location': 'Domicile/Extérieur',
        'impact_position_vs_opponent': "Impact pos VS adv",
        'is_backtoback':'B2B'
    })
    df_today_summary = df_today_summary.round(1)

    if (save_csv==True):
        output_filename = f'data/summary_{day_to_check.date()}.csv'
        df_today_summary.to_csv(output_filename, index=False)
        print(f'Saved summary for {day_to_check.date()} to {output_filename}')
    return(df_today_summary)

def calculate_mean_for_period(player_info, stat_column, start_date):
    period_data = player_info[player_info['GAME_DATE'] >= start_date]
    return period_data[stat_column].mean()

def get_detailed_stats_player(player_info) :
    # Get average stats
    avg_points = player_info['PTS'].mean()
    avg_assists = player_info['AST'].mean()
    avg_rebounds = player_info['REB'].mean()
    avg_blocks = player_info['BLK'].mean()
    avg_steals = player_info['STL'].mean()
    avg_turnovers = player_info['TOV'].mean()
    avg_FG = player_info['FGM'].mean()
    avg_3P = player_info['FG3M'].mean()
    avg_FT = player_info['FTM'].mean()
    avg_FGmiss = player_info['FGA'].mean() - avg_FG
    avg_3Pmiss = player_info['FG3A'].mean() - avg_3P
    avg_FTmiss = player_info['FTA'].mean() - avg_FT
    avg_score_ttfl = player_info['score_ttfl'].mean()
    avg_time = player_info['MIN'].mean()
    team_name = player_info['TEAM_NAME'].values[0]

    # Get timed stats
    current_date = pd.to_datetime("today")
    date_10_days_ago = current_date - timedelta(days=10)
    date_30_days_ago = current_date - timedelta(days=30)
    mean_10_days = calculate_mean_for_period(player_info, 'score_ttfl', date_10_days_ago)
    mean_30_days = calculate_mean_for_period(player_info, 'score_ttfl', date_30_days_ago)

    # Create final df
    stats_df = pd.DataFrame({
        'Nom d\'équipe': [team_name],
        'Minutes moyennes': [avg_time],
        'Score TTFL moyen saison': [avg_score_ttfl],
        'Score TTFL 10 derniers jours': [mean_10_days],
        'Score TTFL 30 derniers jours': [mean_30_days],
        'Points moyen': [avg_points],
        'Assists moyen': [avg_assists],
        'Rebonds moyen': [avg_rebounds],
        'Blocks moyen': [avg_blocks],
        'Steals moyen': [avg_steals],
        'FG réussis moyen': [avg_FG],
        'FG ratés moyen': [avg_FGmiss],
        '3P réussis moyen': [avg_3P],
        '3P points ratés moyen': [avg_3Pmiss],
        'FT réussis moyen': [avg_FT],
        'FT ratés moyen': [avg_FTmiss],
        'Turnovers moyen': [avg_turnovers]
    }).round(1)
    return (stats_df)



# Fonction principale pour orchestrer le processus
def process_n_days(n):
    games = fetch_nba_games()
    df_upcoming_matches = create_games_dataframe(games)
    df_logs_last_year = pd.read_csv('../data/player_game_logs_2024-25.csv', sep=",")
    df_logs_now = process_player_logs(season="2025-26")

    # Boucle sur les n jours à venir
    days_to_check = pd.date_range(start=pd.Timestamp('today').normalize(), periods=n)
    for day_to_check in days_to_check:
        process_match_day(day_to_check, df_logs_last_year, df_logs_now=df_logs_now,
                          df_upcoming_matches = df_upcoming_matches, save_csv=False)


# Exécuter le pipeline
# if __name__ == "__main__":
#     process_n_days(n=30)
