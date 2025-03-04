import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import ace_tools_open as tools

def scrape_transfermarkt(max_age=None, positions=None):
    url = "https://www.transfermarkt.com/spieler-statistik/wertvollstespieler/marktwertetop/plus/0/galerie/0"
    headers = {"User-Agent": "Mozilla/5.0"}  # Avoid getting blocked
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print("Failed to retrieve data")
        return None
    
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", class_="items")
    
    players = []
    for row in table.find_all("tr", class_=["odd", "even"]):
        cols = row.find_all("td")

        
        name = cols[1].find_all("tr")[0].text.strip()
        position = cols[1].find_all("tr")[1].text.strip()
        print(cols[7].find('a').find_all('href'))
        club = cols[7].find_all("a")[0].text.strip()
        age = int(cols[5].text.strip())
        value = cols[8].text.strip()
        
        players.append({
            "Name": name,
            "Position": position,
            "Club": club,
            "Age": age,
            "Value": value
        })
    df = pd.DataFrame(players)
    print(df)
    
    if max_age:
        df = df[df["Age"] <= max_age]
    
    if positions:
        df = df[df["Position"].isin(positions)]
    
    return df

def search_fbref_player(player_name):
    search_url = f"https://fbref.com/en/search/search.fcgi?search={player_name.replace(' ', '+')}"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(search_url, headers=headers)
    
    if response.status_code != 200:
        return None
    
    soup = BeautifulSoup(response.text, "html.parser")
    player_links = soup.find_all("a", href=re.compile("^/en/players/"))
    
    if not player_links:
        return None
    
    return "https://fbref.com" + player_links[0]["href"]

def scrape_fbref_scouting_report(player_url):
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(player_url, headers=headers)
    
    if response.status_code != 200:
        return None
    
    soup = BeautifulSoup(response.text, "html.parser")
    stats_table = soup.find("table", id="scout_summary")
    
    if not stats_table:
        return None
    
    stats = {}
    for row in stats_table.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) >= 2:
            metric = cols[0].text.strip()
            percentile = cols[1].text.strip()
            stats[metric] = percentile
    
    return stats

def get_players_with_scouting_report(max_age=None, positions=None):
    players_df = scrape_transfermarkt(max_age, positions)
    
    if players_df is None:
        return None
    
    players_data = []
    for _, player in players_df.iterrows():
        player_name = player["Name"]
        fbref_url = search_fbref_player(player_name)
        
        if fbref_url:
            scouting_report = scrape_fbref_scouting_report(fbref_url)
            if scouting_report:
                player_data = player.to_dict()
                player_data.update(scouting_report)
                players_data.append(player_data)
        
        time.sleep(2)  # Avoid overwhelming the server
    
    if not players_data:
        print("No players found with a scouting report.")
        return None
    
    df = pd.DataFrame(players_data)
    return df

def categorize_value_thirds(df):
    df = df.sort_values(by="Value", ascending=False).reset_index(drop=True)
    third_size = len(df) // 3
    df["Value Category"] = ["High"] * third_size + ["Mid"] * third_size + ["Low"] * (len(df) - 2 * third_size)
    return df

def evaluate_player(player, criteria):
    weighted_sum = 0
    total_weight = sum(criteria.values())
    for stat, weight in criteria.items():
        if stat in player and player[stat].replace("%", "").isdigit():
            weighted_sum += (int(player[stat].replace("%", "")) * weight)
    return weighted_sum / total_weight if total_weight > 0 else 0

def find_best_players(df, criteria):
    df = categorize_value_thirds(df)
    best_players = []
    
    for category in ["High", "Mid", "Low"]:
        subset = df[df["Value Category"] == category].copy()
        subset["Score"] = subset.apply(lambda row: evaluate_player(row, criteria), axis=1)
        best_players.extend(subset.nlargest(3, "Score").to_dict(orient="records"))
    
    return pd.DataFrame(best_players)

def get_best_players_with_criteria(max_age=None, positions=None, criteria=None):
    players_df = get_players_with_scouting_report(max_age, positions)
    if players_df is not None and criteria:
        best_players_df = find_best_players(players_df, criteria)
        tools.display_dataframe_to_user("Best Players by Criteria", best_players_df)

# Example comprehensive criteria for a Right-Back, including all statistics with different weights
right_back_criteria = {
    "Tackles": 3,
    "Interceptions": 3,
    "Dribbles Completed": 2,
    "Progressive Passes": 2,
    "Aerial Duels Won": 1,
    "Pass Completion %": 1,
    "Progressive Carries": 2,
    "Blocks": 2,
    "Shot-Creating Actions": 2,
    "Touches in Attacking Third": 1,
    "Pressures": 2
}

get_best_players_with_criteria(max_age=25, positions=["Right-Back"], criteria=right_back_criteria)
