import requests
import json
import pandas as pd
import time
import datetime
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

CONNECTION_ID = "LOCAL_POSTGRES"
url = 'https://understat.com/league/EPL/'

def player_retrieval(postgres_config):
    print("Fetching player data from remote website.")
    res = requests.get(url)
    print("Successfully fetched player data")
    soup = BeautifulSoup(res.content, 'html.parser')
    scripts = soup.find_all('script')
    matches = scripts[3]
    strings = matches.string
    start = strings.index("('")+2
    end = strings.index("')")
    json_data = strings[start:end]
    json_data = json_data.encode('utf8').decode('unicode_escape')

    data = json.loads(json_data)

    columns = [
        'id', 'player_name', 'games', 'time', 'goals', 'xG', 'assists', 'xA', 
        'shots', 'key_passes', 'yellow_cards', 'red_cards', 'position',
        'team_title', 'xGChain', 'xGBuildup'
    ]
    dc = {c:[] for c in columns}

    for player in data:
        for c in columns:
            dc[c].append(player[c])
    
    players = pd.DataFrame(dc)
    players['updated_at'] = datetime.datetime.now()

    db_url = f"postgresql://{postgres_config['user']}:{postgres_config['password']}@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
    # Create a SQLAlchemy engine
    engine = create_engine(db_url)

    players.to_sql(name='player', con=engine, if_exists='replace', index=False)
    print("successfully stored data into player table.")
