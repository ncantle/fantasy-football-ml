import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

def fetch_weekly_ol_rankings(engine, season: int):
    load_dotenv()
    engine = engine
    url = "https://nfllines.com/offensive-line-rankings/"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    df = pd.read_html(str(table))[0]

    df = df.rename(columns={
        'Team': 'abbreviation',
        'Pipeline Overall Win Rate (%)': 'ol_win_rate',  
        'Pass Blocking (%)': 'pass_block_rate',
        'Run Blocking (%)': 'run_block_rate'
    })

    df['season'] = season
    # If there's a week column, include, otherwise infer week based on date or manual input
    df['week'] = pd.NA
    df['source'] = 'NFLLines'

    teams_df = pd.read_sql("SELECT team_id, abbreviation FROM teams", engine)
    df = df.merge(teams_df, how='left', on='abbreviation')
    df = df.dropna(subset=['team_id'])

    final = df[['season', 'week', 'team_id', 'ol_win_rate', 'pass_block_rate', 'run_block_rate', 'source']]
    final.to_sql('weekly_offensive_line_rankings', engine, if_exists='replace', index=False)
    print(f"✅ Ingested OL rankings for {len(final)} teams.")
