import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import sqlite3

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

    base_url = "https://www.transfermarkt.com/spieler-statistik/wertvollstespieler/marktwertetop/plus/0/galerie/0?page={}"
    headers = {"User-Agent": "Mozilla/5.0"}  
    players = []
    
    page = 1
    while True:
        response = requests.get(base_url.format(page), headers=headers)
        if response.status_code != 200:
            print("Failed to retrieve data on page", page)
            break
        
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", class_="items")
        if not table:
            break  # No more pages
        
        for row in table.find_all("tr", class_=["odd", "even"]):
            cols = row.find_all("td")
            
            if len(cols) < 9:
                continue  # Skip rows that don't have enough columns
            
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
            
            # Apply age and position filters
            if (max_age is None or age <= max_age) and (positions is None or position in positions):
                players.append(player_data)
            
            if len(players) >= 250:
                break 
        
        if len(players) >= 250:
            break
        
        page += 1  
        time.sleep(1)  
    
    insert_query = "INSERT INTO scraped_players (Name, Position, Club, Nationality, Age, Value) VALUES (?, ?, ?, ?, ?, ?)"
    cursor.executemany(insert_query, [(p["Name"], p["Position"], p["Club"], p["Nationality"], p["Age"], p["Value"]) for p in players])

    df = pd.read_sql_query("SELECT * FROM scraped_players", conn)
    print(df)
    conn.close()

scrape_transfermarkt(25, ["Right-Back"])