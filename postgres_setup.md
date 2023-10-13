First join with postgres user
```bash
psql -h localhost -p5432 -U postgres -d fantasy
```

```sql
CREATE USER airflow_user WITH PASSWORD '';
GRANT CONNECT ON DATABASE fantasy TO airflow_user;
GRANT ALL PRIVILEGES ON DATABASE fantasy TO airflow_user;
GRANT ALL ON SCHEMA public TO airflow_user;
GRANT USAGE ON SCHEMA public TO airflow_user; 
```


```sql
CREATE TABLE player(
    id 				INTEGER,
    player_name 	VARCHAR,
    games 			INTEGER,
    time 			INTEGER,
    goals 			INTEGER,
    xG 				DOUBLE PRECISION,
    assists 		INTEGER,
    xA 				DOUBLE PRECISION,
    shots 			INTEGER,
    key_passes 		INTEGER,
    yellow_cards 	INTEGER,
    red_cards 		INTEGER,
    position 		VARCHAR, 
    team_title 		VARCHAR,
    xGChain 		DOUBLE PRECISION,
    xGBuildup 		DOUBLE PRECISION,
    updated_at      TIMESTAMP
);
```


```python
query = """
INSERT INTO player(id,
    player_name,
    games,
    time,
    goals,
    xG,
    assists,
    xA,
    shots,
    key_passes,
    yellow_cards,
    red_cards,
    position,
    team_title,
    npg,
    npxG,
    xGChain,
    xGBuildup,
    updated_at
)
VALUES %s
"""
```
