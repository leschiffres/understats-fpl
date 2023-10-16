import pandas as pd 
from sqlalchemy import create_engine
import psycopg2.extras
import psycopg2

def team_form(postgres_config):
    """
    Get the teams that are facing opponents with weak defence in the next `NUMBER_OF_FUTURE_MATCHES`. 

    Penalise the team if it concedes a goal at home. Every xgoal at home is multiplied by 1.3.
    Boost the team if it makes a goal away. Every xgoal away is multiplied by 1.3.
    """

    query = """
    select * from team_past_matches
    """
    pg_connection = psycopg2.connect(**postgres_config)

    # first we have to fetch the necessary information from the postgres universe table.
    with pg_connection.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as pg_cur:
        pg_cur.execute(query)
        matches_df = pd.DataFrame(pg_cur.fetchall())

    pg_connection.close()

    ## number of games to look the xgoals against for each team
    NUMBER_OF_GAMES = 6

    print(f'Looking back at {NUMBER_OF_GAMES} games for each team.')
    matches_df['matches_home'] = matches_df.apply(lambda row: 1 if row['h_a'] == 'h' else 0, axis=1)
    matches_df['matches_away'] = matches_df.apply(lambda row: 1 if row['h_a'] == 'a' else 0, axis=1)
    matches_df['attack_home'] = matches_df.apply(lambda row: row['xG'] if row['h_a'] == 'h' else 0, axis=1)
    matches_df['attack_away'] = matches_df.apply(lambda row: row['xG'] if row['h_a'] == 'a' else 0, axis=1)
    matches_df['defence_home'] = matches_df.apply(lambda row: row['xGA'] if row['h_a'] == 'h' else 0, axis=1)
    matches_df['defence_away'] = matches_df.apply(lambda row: row['xGA'] if row['h_a'] == 'a' else 0, axis=1)
    defence_df = matches_df.groupby('team').tail(NUMBER_OF_GAMES).groupby('team', as_index=False).apply(
            lambda row: pd.Series(
                {
                    'xG':sum(row.xG)
                    , 'xGA': sum(row.xGA)
                    , 'xpts': sum(row.xpts)
                    , 'attack_home':sum(row.attack_home)
                    , 'attack_away':sum(row.attack_away)
                    , 'defence_home': sum(row.defence_home)
                    , 'defence_away': sum(row.defence_away)
                    , 'matches_home': sum(row.matches_home)
                    , 'matches_away': sum(row.matches_away)
                }
            )
        )# .sort_values('defending_rating', ascending=False, ignore_index=True)[:]
    defence_df['avg_home_att'] = defence_df['attack_home'] / defence_df['matches_home']
    defence_df['avg_away_att'] = defence_df['attack_away'] / defence_df['matches_away']
    defence_df['avg_home_def'] = defence_df['defence_home'] / defence_df['matches_home']
    defence_df['avg_away_def'] = defence_df['defence_away'] / defence_df['matches_away']
    defence_df['defending_rating'] = defence_df['avg_home_def'] * 1.3 + defence_df['avg_away_def']
    defence_df['attacking_rating'] = defence_df['avg_away_att'] * 1.3 + defence_df['avg_home_att']
    defence_df = defence_df.sort_values(by='defending_rating', ascending=False, ignore_index=True)

    db_url = f"postgresql://{postgres_config['user']}:{postgres_config['password']}@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
    # Create a SQLAlchemy engine
    engine = create_engine(db_url)

    defence_df.to_sql(name='team_form', con=engine, if_exists='replace', index=False)
    

#TODO
def player_form(postgres_config):
    agg = player_data_all.groupby(['player_id', 'player_name', 'team', 'NUMBER_OF_MATCHES'], as_index=False).apply(
        lambda row: pd.Series(
            {
                'total_minutes_played':sum(row.time),
                'avg_minutes_played': np.mean(row.time),
                'xG':sum(row.xG),
                'xA':sum(row.xA),
                'key_passes': sum(row.key_passes),
                'shots':sum(row.shots),
                'goals':sum(row.goals),
                'assists':sum(row.assists),
                'team_played_against':list(row.team_played_against),
                'date_played': list(row.date),
                'oldest_match': min(row.date),
                'latest_match': max(row.date)
            }
        )
    )

    agg['latest_match'] = agg['latest_match'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
    today = datetime.datetime.today()
    agg['days_since_last_match'] = agg['latest_match'].apply(lambda x: (today - x).days)
    agg['xTotal'] = agg['xG'] + agg['xA']
    agg['date_played'] = agg['date_played'].apply(lambda x: sorted(x, reverse=True))
    agg = agg.sort_values(by='xTotal', ascending=False)
    agg.head(20)

# TODO
def team_future_opponents(postgres_config):
    NUMBER_OF_FUTURE_MATCHES = 6
    
    future_matches_df = pd.DataFrame(future_matches_dc)
    future_matches_df = future_matches_df.groupby('team', as_index=False).apply(
            lambda row: pd.Series(
                {
                    'teams_against': list(row.teams_against),
                    'home_or_away': list(row.home_or_away),
                    'when': list(row.when),
                    'sum_of_defence_against': sum(row.defence_of_team_against),
                    'sum_of_attack_against': sum(row.attack_of_team_against),
                    'defending_rating': sum(row.defending_rating),
                    'attacking_rating': sum(row.attacking_rating)
                }
            )
        ).sort_values(by='sum_of_defence_against', ignore_index=True, ascending=False)

    future_matches_df