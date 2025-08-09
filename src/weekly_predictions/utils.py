import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
from sqlalchemy import create_engine

def detect_upcoming_week():
    load_dotenv()
    DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(DATABASE_URL)    

    today = datetime.now(timezone.utc)
    games = pd.read_sql('SELECT * FROM games', engine)
    games["game_date"] = pd.to_datetime(games["game_date"], utc=True)

    upcoming_games = games[games["game_date"] >= today]
    if not upcoming_games.empty:
        next_week = int(upcoming_games.sort_values("game_date").iloc[0]["week"])
        return next_week
    else:
        # All games passed; return max week + 1
        return int(games["week"].max()) + 1