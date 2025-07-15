import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import logging
import time

logging.basicConfig(level=logging.INFO)
load_dotenv()


DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URL)

# Main NFL events URL
base_url = "https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2025/types/2/events?limit=1000"

def fetch_json(url):
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.error(f"Failed to fetch {url}: {e}")
        return None
    
week_cache = {}
def fetch_week_number(week_obj):
    """
    Given a week object, either with number or a $ref URL,
    return the week number as int.
    Uses week_cache to avoid redundant requests.
    """
    if isinstance(week_obj, dict):
        if 'number' in week_obj:
            return int(week_obj['number'])
        elif '$ref' in week_obj:
            ref_url = week_obj['$ref']
            if ref_url in week_cache:
                return week_cache[ref_url]
            try:
                resp = requests.get(ref_url, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                number = int(data.get('number'))
                week_cache[ref_url] = number
                return number
            except Exception as e:
                logging.error(f"Failed to fetch week data from {ref_url}: {e}")
                return None
    elif isinstance(week_obj, int):
        return week_obj
    else:
        return None

team_cache = {}
def fetch_team_abbreviation(team_obj):
    """
    Given a team object, either with abbreviation or a $ref URL,
    return the abbreviation string.
    Uses team_cache to avoid redundant requests.
    """
    if 'abbreviation' in team_obj:
        return team_obj['abbreviation']
    elif '$ref' in team_obj:
        ref_url = team_obj['$ref']
        if ref_url in team_cache:
            return team_cache[ref_url]
        try:
            resp = requests.get(ref_url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            abbreviation = data.get('abbreviation')
            if abbreviation:
                team_cache[ref_url] = abbreviation
                return abbreviation
            else:
                logging.warning(f"No abbreviation found in team data at {ref_url}")
                return None
        except Exception as e:
            logging.error(f"Failed to fetch team data from {ref_url}: {e}")
            return None
    else:
        return None

def parse_event(event):
    try:
        competitions = event.get('competitions', [])
        if not competitions:
            return None
        comp = competitions[0]
        competitors = comp.get('competitors', [])
        if len(competitors) != 2:
            return None

        home_team = None
        away_team = None
        for team in competitors:
            team_obj = team.get('team', {})
            abbreviation = fetch_team_abbreviation(team_obj)
            if abbreviation is None:
                logging.warning(f"Missing abbreviation for team: {team_obj}")
                continue
            if team.get('homeAway') == 'home':
                home_team = abbreviation
            elif team.get('homeAway') == 'away':
                away_team = abbreviation

        venue = comp.get('venue', {}).get('fullName')
        game_date = event.get('date', '').split('T')[0]

        week_obj = event.get('week')
        week = fetch_week_number(week_obj)
        
        season = 2025

        if not home_team or not away_team or not game_date:
            logging.warning(f"Incomplete event data: home_team={home_team}, away_team={away_team}, date={game_date}")
            return None

        return {
            'season': season,
            'week': week,
            'home_team': home_team,
            'away_team': away_team,
            'stadium': venue,
            'game_date': game_date
        }
    except Exception as e:
        logging.error(f"Error parsing event: {e}")
        return None

def main():
    data = fetch_json(base_url)
    if not data:
        logging.error("No data found at main URL")
        return

    items = data.get('items', [])
    logging.info(f"Found {len(items)} event references")

    games = []
    for idx, item in enumerate(items):
        ref_url = item.get('$ref')
        if not ref_url:
            continue

        event_data = fetch_json(ref_url)
        if not event_data:
            continue

        parsed = parse_event(event_data)
        if parsed:
            games.append(parsed)

        # Be kind to ESPN's servers
        time.sleep(0.1)

        if idx % 10 == 0:
            logging.info(f"Processed {idx+1} / {len(items)} events")

    if not games:
        logging.error("No games parsed successfully.")
        return

    df = pd.DataFrame(games)

    teams_df = pd.read_sql('SELECT team_id, abbreviation FROM teams', engine)
    df = df.merge(teams_df, how = 'left', left_on = 'home_team', right_on = 'abbreviation').rename(columns = {'team_id':'home_team_id'}).drop(columns = ['abbreviation'])
    df = df.merge(teams_df, how = 'left', left_on = 'away_team', right_on = 'abbreviation').rename(columns = {'team_id':'away_team_id'}).drop(columns = ['abbreviation'])
    df

    df.drop_duplicates().to_sql('games', engine, if_exists='replace', index=False)
    logging.info("✅ NFL schedule ingested successfully.")

if __name__ == "__main__":
    main()
