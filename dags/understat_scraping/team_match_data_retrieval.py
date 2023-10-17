import requests
import json
import pandas as pd
import time
import datetime
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import psycopg2.extras
import psycopg2
from utils.config import SEASON, SLEEP_TIME_IN_SECONDS

CONNECTION_ID = "LOCAL_POSTGRES"
url = 'https://understat.com/league/EPL/'

def team_past_matches_retrieval(postgres_config):
    print("Fetching team past matches data from remote website.")
    res = requests.get(url)
    print("Successfully fetched team past matches data")
    soup = BeautifulSoup(res.content, 'html.parser')
    scripts = soup.find_all('script')

    matches = scripts[2]
    strings = matches.string
    start = strings.index("('")+2
    end = strings.index("')")
    json_data = strings[start:end]
    json_data = json_data.encode('utf8').decode('unicode_escape')

    data = json.loads(json_data)
    columns = ['h_a', 'xG', 'xGA', 'npxG', 'npxGA', 'scored','missed', 'xpts', 'result']

    dc = {}
    dc['team'] = []
    for c in columns:
        dc[c] = []

    team_list = []
    for team_id in data:
        team_name = data[team_id]['title']
        team_list.append(team_name)
        for match in data[team_id]['history']:
            dc['team'].append(team_name)
            for c in columns:
                dc[c].append(match[c]) 
                
    team_match_data = pd.DataFrame(dc)

    team_match_data['updated_at'] = datetime.datetime.now()

    db_url = f"postgresql://{postgres_config['user']}:{postgres_config['password']}@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
    engine = create_engine(db_url)

    team_match_data.to_sql(name='team_past_matches', con=engine, if_exists='replace', index=False)
    print("successfully stored data into team_past_matches table.")

def team_match_data_retrieval(postgres_config):
    pg_connection = psycopg2.connect(**postgres_config)
    query = 'SELECT DISTINCT team FROM team_past_matches ORDER BY team'

    # first we have to fetch the necessary information from the postgres universe table.
    with pg_connection.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as pg_cur:
        pg_cur.execute(query)
        df = pd.DataFrame(pg_cur.fetchall())

    pg_connection.close()
    
    teams = df['team'].to_list()
    matches_dc = {
        'team':[],
        'datetime':[],
        'is_result':[],
        'side':[],
        'home_team':[],
        'away_team':[],
        'goals_home':[],
        'goals_away':[],
        'xgoals_home':[],
        'xgoals_away':[],
    }
    for team in teams:
        time.sleep(SLEEP_TIME_IN_SECONDS)
        
        # replace empty space with special character %20
        team_name_url = team.replace(' ', '%20')
        base_url = f'https://understat.com/team/{team_name_url}/{24}'
        url = base_url
        print(url)

        res = requests.get(url)
        soup = BeautifulSoup(res.content, 'html.parser')
        scripts = soup.find_all('script')
        
        script_data = scripts[1]
        strings = script_data.string
        start = strings.index("('")+2
        end = strings.index("')")
        json_data = strings[start:end]
        json_data = json_data.encode('utf8').decode('unicode_escape')
        data = json.loads(json_data)

        now = datetime.datetime.now()

        for match_info in data:
            matches_dc['team'].append(team)
            matches_dc['datetime'].append(match_info['datetime'])
            matches_dc['is_result'].append(match_info['isResult'])
            matches_dc['side'].append(match_info['side'])
            matches_dc['home_team'].append(match_info['h']['title'])
            matches_dc['away_team'].append(match_info['a']['title'])
            matches_dc['goals_home'].append(match_info['goals']['h'])
            matches_dc['goals_away'].append(match_info['goals']['a'])
            matches_dc['xgoals_home'].append(match_info['xG']['h'])
            matches_dc['xgoals_away'].append(match_info['xG']['a'])
    
    team_match_data = pd.DataFrame(matches_dc)

    team_match_data['updated_at'] = datetime.datetime.now()

    db_url = f"postgresql://{postgres_config['user']}:{postgres_config['password']}@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
    engine = create_engine(db_url)

    team_match_data.to_sql(name='team_match_data', con=engine, if_exists='replace', index=False)
    print("successfully stored data into team_match_data table.")