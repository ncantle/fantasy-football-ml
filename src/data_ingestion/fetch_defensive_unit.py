import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import text

def fetch_defensive_unit_rankings_sharp(engine, season):
    url = "https://www.sharpfootballanalysis.com/analysis/best-nfl-front-seven-rankings/"
    resp = requests.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    df = pd.read_html(str(table))[0]
    df = df.rename(columns={'Team': 'abbreviation', 'Score': 'front_seven_score'})
    df['season'] = int(season)
    df['secondary_score'] = None  # if no secondary ranking available
    df['source'] = 'SharpFootball'
    teams_df = pd.read_sql('SELECT team_id, abbreviation FROM teams', engine)
    merged = df.merge(teams_df, how='left', on='abbreviation').dropna(subset=['team_id'])
    final = merged[['season', 'team_id', 'front_seven_score', 'secondary_score', 'source']]
    final.to_sql('defensive_unit_rankings', engine, if_exists='replace', index=False)
    print(f"✅ Ingested defensive unit rankings for season {season}.")
