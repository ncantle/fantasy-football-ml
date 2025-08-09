from src.modeling.model import model
import argparse
import time

def train_models(season, week):

    for position in ['qb', 'wr', 'rb', 'te']:
        print(f'Training {position} model...')
        start = time.time()
        model(season, week, position)
        end = time.time()
        print(f"Training of {position} model completed in {end - start:.2f} seconds.")
        print('')
        print('')

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Train all models for a given season/week.")
    parser.add_argument("--season", type=int, required=True, help="Season year, e.g., 2023")
    parser.add_argument("--week", type=int, required=True, help="Week number, e.g., 8")
    
    args = parser.parse_args()
    train_models(args.season, args.week)