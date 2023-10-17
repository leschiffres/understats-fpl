import requests
import json
import pandas as pd
import time
import datetime
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import psycopg2.extras
import psycopg2
from utils.config import SLEEP_TIME_IN_SECONDS

CONNECTION_ID = "LOCAL_POSTGRES"
url = 'https://understat.com/league/EPL/'

def aggregated_player_retrieval(postgres_config):
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

    # data conversion
    players['xG'] = players['xG'].apply(lambda x: float(x))
    players['xA'] = players['xA'].apply(lambda x: float(x))
    players['xGChain'] = players['xGChain'].apply(lambda x: float(x))

    players['time'] = players['time'].apply(lambda x: int(x))
    players['games'] = players['games'].apply(lambda x: int(x))
    players['goals'] = players['goals'].apply(lambda x: int(x))
    players['assists'] = players['assists'].apply(lambda x: int(x))
    players['shots'] = players['shots'].apply(lambda x: int(x))
    players['key_passes'] = players['key_passes'].apply(lambda x: int(x))

    db_url = f"postgresql://{postgres_config['user']}:{postgres_config['password']}@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
    # Create a SQLAlchemy engine
    engine = create_engine(db_url)

    players.to_sql(name='player', con=engine, if_exists='replace', index=False)
    print("successfully stored data into player table.")

## number of matches to look the form

def player_recent_matches_retrieval(postgres_config):

    # we want detailed retrieval of data for players whose xTotal (xgoals + xassists) is at least 0.2
    # and they played at least 4 matches

    query = """select id, player_name, team_title as team from player where games > 3 and ("xG" + "xA") / games > 0.2"""
    pg_connection = psycopg2.connect(**postgres_config)


    # first we have to fetch the necessary information from the postgres universe table.
    with pg_connection.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as pg_cur:
        pg_cur.execute(query)
        df = pd.DataFrame(pg_cur.fetchall())

    pg_connection.close()

    print(f"Fetching data for {df.shape[0]} players.")

    # we keep a limited number of matches for each player
    NUMBER_OF_MATCHES=6

    player_data_all = pd.DataFrame()
    for i in range(len(df)):
        time.sleep(SLEEP_TIME_IN_SECONDS)
        player_id = df.at[i, 'id']
        player_name = df.at[i, 'player_name']
        team = df.at[i, 'team']
        print(player_id, player_name, team)
        
        base_url = f'https://understat.com/player/{player_id}'
        url = base_url
        print(url)

        res = requests.get(url)
        soup = BeautifulSoup(res.content, 'html.parser')
        scripts = soup.find_all('script')
        script_data = scripts[4]
        strings = script_data.string
        start = strings.index("('")+2
        end = strings.index("')")
        json_data = strings[start:end]
        json_data = json_data.encode('utf8').decode('unicode_escape')
        data = json.loads(json_data)

        # get data for every player for the past 5 matches
        
        columns = ['time', 'xG', 'xA', 'goals', 'assists', 'key_passes', 'shots', 'h_team', 'a_team', 'date']
        dc = {c:[] for c in columns}

        for match in data[:NUMBER_OF_MATCHES]:
            for c in columns:
                dc[c].append(match[c])
        player_data = pd.DataFrame(dc)
        player_data['player_id'] = player_id
        player_data['player_name'] = player_name
        player_data['team'] = team
        player_data['NUMBER_OF_MATCHES'] = NUMBER_OF_MATCHES
        player_data['time'] = player_data['time'].astype(float)
        player_data['xG'] = player_data['xG'].astype(float)
        player_data['xA'] = player_data['xA'].astype(float)
        player_data['goals'] = player_data['goals'].astype(float)
        player_data['assists'] = player_data['assists'].astype(float)
        player_data['key_passes'] = player_data['key_passes'].astype(float)
        player_data['shots'] = player_data['shots'].astype(float)
        player_data['team_played_against'] =  player_data.apply(lambda row: row['h_team'] if row['h_team'] != row['team'] else row['a_team'], axis=1)

        player_data_all = pd.concat([player_data_all, player_data])
    
    db_url = f"postgresql://{postgres_config['user']}:{postgres_config['password']}@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
    # Create a SQLAlchemy engine
    engine = create_engine(db_url)

    player_data_all.to_sql(name='player_recent_matches', con=engine, if_exists='replace', index=False)
    print("successfully stored data into player_recent_matches table.")
