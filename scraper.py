import requests
from bs4 import BeautifulSoup
import pandas as pd

# URL for Manchester United page
comp = "c9"
team_url = "https://fbref.com/en/squads/19538871/Manchester-United-Stats"

# Request and parse the team page
response = requests.get(team_url)
soup = BeautifulSoup(response.content, "html.parser")

# Extract table data (e.g., Squad stats)
squad_list_table = soup.find('table', {'class': 'stats_table sortable min_width'})
player_links = squad_list_table.find_all('a', href=True)

# Filtering and formatting player links
squad_player_links = [
    link.get('href') for link in player_links 
    if link.get('href').count('/') == 4 and link.get('href').startswith("/en/players/")
]
formatted_urls = [
    f"https://fbref.com/en/players/{link.split('/')[3]}/matchlogs/2023-2024/{comp}/{'-'.join(link.split('/')[-1].split('-')[:-1])}-Match-Logs"
    for link in squad_player_links
]


import re

player_data = []
keeper_data = []

for url in formatted_urls:
    response_data = session.get(url, headers=headers)
    soup_data = BeautifulSoup(response_data.text, "html.parser")

    # Extracting the player's name from the title and cleaning it
    title_data = soup_data.title.string
    pattern = r"2023-2024 (Premier League|Serie A|La Liga|Ligue 1|Bundesliga) Match Logs( \(Goalkeeping\))? \| FBref\.com"
    player_name = re.sub(pattern, "", title_data).strip()

    # Identifying and processing the data rows
    date_header = soup_data.find("th", text="Date")
    if date_header is None:
        continue  # Skip if the player has no match data
    header_rows = date_header.find_parent("tr")
    data_rows = header_rows.find_all_next("tr")

    for row in data_rows[:-1]:  # Exclude total statistics
        if row.get('class') is None:  # Skip rows for matches not played
            cells = row.find_all(['th', 'td'])
            row_data = [cell.get_text(strip=True) for cell in cells]
            row_data.insert(0, player_name)

            if "Goalkeeping" in title_data:
                keeper_data.append(row_data)
            else:
                player_data.append(row_data)

# Creating DataFrames
df_player = pd.DataFrame(player_data, columns=headers_player)
df_keeper = pd.DataFrame(keeper_data, columns=headers_keeper)

# Saving to CSV files
df_player.to_csv("Liverpoolplayer.csv", sep=';', encoding='utf-8', index=False)
df_keeper.to_csv("Liverpoolkeeper.csv", sep=';', encoding='utf-8', index=False)