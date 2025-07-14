import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load environment variables from .env file
load_dotenv()

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

DATABASE_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

def initialize_postgres_schema(sql_file_path: str):
    engine = create_engine(DATABASE_URL)
    with engine.begin() as connection:
        with open(sql_file_path, 'r') as sql_file:
            sql_script = sql_file.read()
            connection.execute(text(sql_script))
    print("✅ PostgreSQL schema initialized successfully.")


if __name__ == "__main__":
    sql_file = 'data/postgres_init.sql'
    initialize_postgres_schema(sql_file)
    