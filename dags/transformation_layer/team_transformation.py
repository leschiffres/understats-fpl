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
    NUMBER_OF_GAMES_TEAM_FORM = 6

    print(f'Looking back at {NUMBER_OF_GAMES_TEAM_FORM} games for each team.')
    matches_df['matches_home'] = matches_df.apply(lambda row: 1 if row['h_a'] == 'h' else 0, axis=1)
    matches_df['matches_away'] = matches_df.apply(lambda row: 1 if row['h_a'] == 'a' else 0, axis=1)
    matches_df['attack_home'] = matches_df.apply(lambda row: row['xG'] if row['h_a'] == 'h' else 0, axis=1)
    matches_df['attack_away'] = matches_df.apply(lambda row: row['xG'] if row['h_a'] == 'a' else 0, axis=1)
    matches_df['defence_home'] = matches_df.apply(lambda row: row['xGA'] if row['h_a'] == 'h' else 0, axis=1)
    matches_df['defence_away'] = matches_df.apply(lambda row: row['xGA'] if row['h_a'] == 'a' else 0, axis=1)
    defence_df = matches_df.groupby('team').tail(NUMBER_OF_GAMES_TEAM_FORM).groupby('team', as_index=False).apply(
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
    defence_df['defending_rating'] = defence_df['avg_home_def'] + defence_df['avg_away_def']
    defence_df['attacking_rating'] = defence_df['avg_away_att'] + defence_df['avg_home_att']
    defence_df = defence_df.sort_values(by='defending_rating', ascending=False, ignore_index=True)

    db_url = f"postgresql://{postgres_config['user']}:{postgres_config['password']}@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
    # Create a SQLAlchemy engine
    engine = create_engine(db_url)

    defence_df.to_sql(name='team_form', con=engine, if_exists='replace', index=False)
      
def team_future_opponents_ratings(postgres_config):
    """
    The following dag combines the team_form and team_match_data, to determine how difficult are the future matches for each team.
    Using the team form allows us to track down of teams that have been overperforming and teams that have been underperforming
    """
    query = """
    select 
        team, defending_rating, attacking_rating
    from team_form
    """
    pg_connection = psycopg2.connect(**postgres_config)

    # first we have to fetch the necessary information from the postgres universe table.
    with pg_connection.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as pg_cur:
        pg_cur.execute(query)
        df = pd.DataFrame(pg_cur.fetchall())

    pg_connection.close()

    defending_rating = dict(zip(df['team'], df['defending_rating']))
    attacking_rating = dict(zip(df['team'], df['attacking_rating']))


    NUMBER_OF_FUTURE_MATCHES=6
    query = """
    select team, side, home_team, away_team
    from (
        select 
            row_number() over (partition by team order by datetime) as row_num
            , *
        from team_match_data
        where not is_result
    )t
    where row_num <= %(NUMBER_OF_FUTURE_MATCHES)s
    """
    pg_connection = psycopg2.connect(**postgres_config)


    # first we have to fetch the necessary information from the postgres universe table.
    with pg_connection.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as pg_cur:
        pg_cur.execute(query, {"NUMBER_OF_FUTURE_MATCHES":NUMBER_OF_FUTURE_MATCHES})
        df = pd.DataFrame(pg_cur.fetchall())

    pg_connection.close()
    
    df['opponent'] = df.apply(lambda row: row['home_team'] if row['side'] == 'a' else row['away_team'], axis=1)
    df['opponent_attacking_rating'] = df['opponent'].apply(lambda x: attacking_rating[x])
    df['opponent_defending_rating'] = df['opponent'].apply(lambda x: defending_rating[x])

    df = df.groupby('team', as_index=False).apply(
        lambda row: pd.Series(
            {
                'opponents': list(row.opponent),
                'overall_opponent_defending_rating': sum(row.opponent_defending_rating),
                'overall_opponent_attacking_rating': sum(row.opponent_attacking_rating)
            }
        )
    )

    db_url = f"postgresql://{postgres_config['user']}:{postgres_config['password']}@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
    
    # Create a SQLAlchemy engine
    engine = create_engine(db_url)
    df.to_sql(name='team_future_opponents_ratings', con=engine, if_exists='replace', index=False)