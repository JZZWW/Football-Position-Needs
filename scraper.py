import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import sqlite3
import cloudscraper

db = 'position_needs.db'
conn = sqlite3.connect(db)
cursor = conn.cursor()

def table_drop(table, database = 'position_needs.db'):
    """
    Drops a table in a database.

    Arguments:
    table -- A table in the database
    database -- A database (default position_needs.db)
    """
    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    cursor.execute(f'DROP TABLE IF EXISTS {table}')

    conn.commit()
    conn.close()



def scrape_transfermarkt( max_age=None, positions=None, database = 'position_needs.db'):
    """
    Scrapes transfermarkt for players. Create a table 'scraped_players' in the database.

    Arguments:
    max_age -- Integer max age of the players that are to be scraped (default None)
    positions -- List of position or positions of the players that are to be scraped (default None)
    database -- A database (default position_needs.db)
    """

    table_drop('scraped_players')
    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS scraped_players (
        Name TEXT,
        Position TEXT,
        Club TEXT,
        Nationality TEXT,
        Age INTEGER,
        Value FLOAT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()

    position_mapping = {
        "Goalkeeper": 1,
        "Sweeper": 2, 
        "Centre-Back": 3,
        "Left-Back": 4,
        "Right-Back": 5,
        "Defensive Midfield": 6,
        "Central Midfield": 8,
        "Attacking Midfield": 10,
        "Right Winger": 7,
        "Left Winger": 9,
        "Centre-Forward": 11
    }

    # Determine the position ID for the URL
    position_id = "alle"  # Default (all positions)
    if positions and len(positions) == 1 and positions[0] in position_mapping:
        position_id = position_mapping[positions[0]]

    base_url = f"https://www.transfermarkt.com/spieler-statistik/wertvollstespieler/marktwertetop/plus/0/galerie/0?ausrichtung=alle&spielerposition_id={position_id}"
    headers = {"User-Agent": "Mozilla/5.0"}  
    players = []
    
    
    page = 1
    while True:
        response = requests.get(f"{base_url}&page={page}", headers=headers)
        if response.status_code != 200:
            print("Failed to retrieve data on page", page)
            break
        
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", class_="items")
        if not table:
            break  # No more pages
        
        for row in table.find_all("tr", class_=["odd", "even"]):
            cols = row.find_all("td")
            
            
            # Extract Data
            name = cols[1].find_all("tr")[0].text.strip()
            
            position = cols[1].find_all("tr")[1].text.strip()
            
            club_tag = cols[7].find("a")
            club = club_tag["title"].strip() if club_tag and "title" in club_tag.attrs else "Unknown"

            nationality_tag = cols[6].find("img")
            nationality = nationality_tag["title"].strip() if nationality_tag and "title" in nationality_tag.attrs else "Unknown"
            
            age = int(cols[5].text.strip())
            
            value_text = cols[8].text.strip()
            value = re.sub(r'[^0-9.]', '', value_text)
            
            player_data = {
                "Name": name,
                "Position": position,
                "Club": club,
                "Nationality": nationality,
                "Age": age,
                "Value": value
            }

            if player_data in players:
                break

            # Apply age and position filters
            if (max_age is None or age <= max_age) and (positions is None or position in positions):
                players.append(player_data)
            
            if len(players) >= 100:
                break 
        
        page += 1  
        if len(players) >= 100:
            break
        
        
    
    insert_query = "INSERT INTO scraped_players (Name, Position, Club, Nationality, Age, Value) VALUES (?, ?, ?, ?, ?, ?)"
    cursor.executemany(insert_query, [(p["Name"], p["Position"], p["Club"], p["Nationality"], p["Age"], p["Value"]) for p in players])
    conn.commit()
    df = pd.read_sql_query("SELECT * FROM scraped_players", conn)
    print(df)
    conn.close()

# scrape_transfermarkt(25, ["Right-Back"])

def get_transfermarkt_players(database=db):
    """
    Retrieves player names from the 'scraped_players' table in the database.
    """
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    cursor.execute("SELECT Name FROM scraped_players")
    players = [row[0] for row in cursor.fetchall()]
    conn.close()
    return players

def scrape_fbref(db = db):
    table_drop('leagues')
    table_drop('teams')
    table_drop('players')
    # Create tables
    cursor.executescript('''
        CREATE TABLE IF NOT EXISTS leagues (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            url TEXT
        );
        
        CREATE TABLE IF NOT EXISTS teams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            league_id INTEGER,
            url TEXT, 
            FOREIGN KEY (league_id) REFERENCES leagues(id)
        );
        
        CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            position TEXT,
            team_id INTEGER,
            nationality TEXT,
            age INTEGER,
            value FLOAT,
            unique_url TEXT,
            FOREIGN KEY (team_id) REFERENCES teams(id)
        );
    ''')
    conn.commit()

    # League URLs
    league_urls = {
        "Champions League": "https://fbref.com/en/comps/8/Champions-League-Stats",
        "Europa League": "https://fbref.com/en/comps/19/Europa-League-Stats",
        "Big 5 European Leagues": "https://fbref.com/en/comps/Big5/Big-5-European-Leagues-Stats",
        "Championship": "https://fbref.com/en/comps/10/Championship-Stats",
        "Major League Soccer": "https://fbref.com/en/comps/22/Major-League-Soccer-Stats",
        "Serie A": "https://fbref.com/en/comps/24/Serie-A-Stats",
        "Serie B": "https://fbref.com/en/comps/18/Serie-B-Stats",
        "Eredivisie": "https://fbref.com/en/comps/23/Eredivisie-Stats",
        "Primeira Liga": "https://fbref.com/en/comps/32/Primeira-Liga-Stats",
        "Liga MX": "https://fbref.com/en/comps/31/Liga-MX-Stats",
        "Belgian Pro League": "https://fbref.com/en/comps/37/Belgian-Pro-League-Stats",
        "Segunda Division": "https://fbref.com/en/comps/17/Segunda-Division-Stats"
    }

    # Insert leagues
    for name, url in league_urls.items():
        cursor.execute("INSERT OR IGNORE INTO leagues (name, url) VALUES (?, ?)", (name, url))
    conn.commit()
    
    for league_name, league_url in league_urls.items():
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
        scraper = cloudscraper.create_scraper()
        response = scraper.get(league_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to retrieve {league_name} data.")
            continue
        soup = BeautifulSoup(response.text, 'html.parser')

        # Get league ID
        cursor.execute("SELECT id FROM leagues WHERE name = ?", (league_name,))
        league_id = cursor.fetchone()[0]

        teams_table = soup.find("table", class_="stats_table")
        if teams_table:
            for row in teams_table.find_all("tr"):
                link = row.find("a")
                if link:
                    team_name = link.text
                    team_url = "https://fbref.com" + link.get("href")
                    cursor.execute("INSERT OR IGNORE INTO teams (name, league_id, url) VALUES (?, ?, ?)", (team_name, league_id, team_url))

    conn.commit()
    df = pd.read_sql_query("SELECT * FROM teams WHERE name = 'Liverpool'", conn)
    print(df)
    if df.empty:
        print("No teams found. Please check the website structure.")
    if df.empty:
        print("No teams found. Check the HTML structure.")
    conn.close()
scrape_fbref()
