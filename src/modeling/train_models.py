from src.modeling.qb_model import qb_model
from src.modeling.wr_model import wr_model
from src.modeling.rb_model import rb_model
from src.modeling.te_model import te_model

def train_models(season, week):   
    # Train QB model
    qb_model(season, week)
    
    # Train WR model
    wr_model(season, week)
    
    # Train RB model
    rb_model(season, week)
    
    # Train TE model
    te_model(season, week)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Train all models for a given season/week.")
    parser.add_argument("--season", type=int, required=True, help="Season year, e.g., 2023")
    parser.add_argument("--week", type=int, required=True, help="Week number, e.g., 8")
    
    args = parser.parse_args()
    train_models(args.season, args.week)