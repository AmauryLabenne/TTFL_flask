import requests
from bs4 import BeautifulSoup
import pandas as pd


def get_nba_injuries_cbs(save_csv = False):
    # Get url and html
    # url = "https://www.espn.com/nba/injuries"
    url = "https://www.cbssports.com/nba/injuries/"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Get injuries infos by teams
    team_sections = soup.find_all('div', 'TableBase')

    # Loop to store all infos
    teams = []
    players = []
    positions = []
    status = []
    for table in team_sections:
        team_name = table.find('h4').text.strip()
        rows = table.find_all('tr', class_='TableBase-bodyTr')
        for row in rows:
            player_data = row.find_all('td')
            if len(player_data) == 5:  # Need 5 columns (player, position, updated, injury, status)
                # player_name = player_data[0].text.strip()
                player_name = player_data[0].select_one('.CellPlayerName--long a').text
                position = player_data[1].text.strip()
                # updated = player_data[2].text.strip()
                # injury = player_data[3].text.strip()
                statu = player_data[4].text.strip()

                # Append to final lists
                teams.append(team_name)
                players.append(player_name)
                positions.append(position)
                status.append(statu)

    # Convert to df
    df_injuries = pd.DataFrame({
        'Team': teams,
        'Player': players,
        'Position': positions,
        'Status': status
    })

    # Convert Status
    df_injuries['Status'] = df_injuries['Status'].replace({
        r'(?i)game time decision': 'GTD',  # Matches "Game Time Decision" (case insensitive)
        r'(?i)out for the season': 'Out Season',  # Matches "Out for the season" (case insensitive)
        r'(?i)expected to be out until at least (.*)': r'Out till \1'  # Formats as "Out till [Date]"
    }, regex=True)

    if (save_csv):
        df_injuries.to_csv('data/nba_injuries.csv', index=False)

    return (df_injuries)