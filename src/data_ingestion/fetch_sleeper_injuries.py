import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URL)

# Example injury statuses (you'd replace this with a true source later)
data = [
    {"player_id": 101, "week": 1, "status": "Questionable"},
    {"player_id": 202, "week": 1, "status": "Out"},
    {"player_id": 303, "week": 1, "status": "Healthy"}
]
df = pd.DataFrame(data)

df.to_sql('injuries', engine, if_exists='replace', index=False)
print("✅ Mock injury data ingested into PostgreSQL.")