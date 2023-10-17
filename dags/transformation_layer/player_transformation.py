import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2.extras
import psycopg2
import datetime


def player_form(postgres_config):
    query = """
    select
        *
    from player_recent_matches
    """
    pg_connection = psycopg2.connect(**postgres_config)

    # first we have to fetch the necessary information from the postgres universe table.
    with pg_connection.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor) as pg_cur:
        pg_cur.execute(query)
        df = pd.DataFrame(pg_cur.fetchall())

    pg_connection.close()

    agg = df.groupby(['player_id', 'player_name', 'team', 'NUMBER_OF_MATCHES'], as_index=False).apply(
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

    db_url = f"postgresql://{postgres_config['user']}:{postgres_config['password']}@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
    # Create a SQLAlchemy engine
    engine = create_engine(db_url)

    agg.to_sql(name='player_form', con=engine, if_exists='replace', index=False)
