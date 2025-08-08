import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from xgboost import XGBRegressor
import joblib
import argparse
from dotenv import load_dotenv
import os
from datetime import datetime
pd.set_option('mode.chained_assignment', None)

# ------------------------
# CONFIG
# ------------------------
MODEL_DIR = "models"
TABLE_NAME = "wr_features"
TARGET = "fantasy_points"
EXCLUDE_COLS = ["player_id", "player_name", "season", "week", "player_display_name", "position", "team_abbreviation", TARGET]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_DIR = os.path.join(BASE_DIR, "models")
LOG_DIR = os.path.join(BASE_DIR, "model_logs")
# ------------------------
# FUNCTIONS
# ------------------------
def load_data():
    print('Loading data from database...')
    load_dotenv()
    DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(DATABASE_URL)

    df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", engine)
    return df


def train_test_split_for_week(df, season, week):
    print(f"Splitting data for season {season}, week {week}...")
    """
    Simulate predicting for a specific season/week by holding out that week as test.
    All prior weeks are used for training.
    """
    train_df = df[(df["season"] < season) | ((df["season"] == season) & (df["week"] < week))]
    test_df = df[(df["season"] == season) & (df["week"] == week)]
    return train_df, test_df


def get_features(df, train_df, test_df):
    print("Selecting features and handling data types...")
    """Select feature columns dynamically and handle categorical/bool types automatically."""
    features = [col for col in df.columns if col not in EXCLUDE_COLS]

    # Detect categorical columns (object or category types)
    cat_cols = train_df.select_dtypes(include=["object", "category"]).columns
    for col in cat_cols:
        train_df[col] = train_df[col].astype("category")
        test_df[col] = test_df[col].astype("category")

    # Detect boolean-like columns (0/1 or True/False)
    bool_cols = [col for col in train_df.columns 
                 if train_df[col].dropna().nunique() == 2 and 
                 sorted(train_df[col].dropna().unique()) in [[0, 1], [False, True], ['false','true']]]
    for col in bool_cols:
        train_df[col] = train_df[col].astype(bool)
        test_df[col] = test_df[col].astype(bool)

    train_df['dome'] = train_df['dome'].astype(bool)
    test_df['dome'] = test_df['dome'].astype(bool)

    X_train = train_df[features]
    y_train = train_df[TARGET]

    X_test = test_df[features]
    y_test = test_df[TARGET]

    return features, X_train, y_train, X_test, y_test

def train_model(train_df, features):
    print("Training XGBoost model...")
    """Train an XGBoost regression model."""
    model = XGBRegressor(
        n_estimators=500,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        enable_categorical=True
    )
    model.fit(train_df[features], train_df[TARGET])
    return model

def evaluate_model(model, X_train, y_train, X_test, y_test, season, week,
                   model_name=None, log_dir=LOG_DIR):
    if model_name is None:
        model_name = f"wr_model_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    print("Evaluating model performance...")
    """Evaluate model performance, print metrics, and log to CSV."""
    
    # Predictions
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)

    # Metrics
    metrics = {
        "rmse_train": np.sqrt(mean_squared_error(y_train, y_pred_train)),
        "mae_train": mean_absolute_error(y_train, y_pred_train),
        "r2_train": r2_score(y_train, y_pred_train),
        "rmse_test": np.sqrt(mean_squared_error(y_test, y_pred_test)),
        "mae_test": mean_absolute_error(y_test, y_pred_test),
        "r2_test": r2_score(y_test, y_pred_test),
        "timestamp": datetime.now().strftime("%Y%m%d%H%M%S"),
        "model_name": f"wr_model_season{season}_week{week}_{datetime.now().strftime('%Y%m%d%H%M%S')}.pkl"
    }

    # Print to console
    print("\nModel Evaluation Metrics:")
    for k, v in metrics.items():
        if isinstance(v, float):
            print(f"{k}: {v:.4f}")
        else:
            print(f"{k}: {v}")

    # Log to CSV
    os.makedirs(log_dir, exist_ok=True)
    csv_path = os.path.join(log_dir, "metrics_log.csv")

    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        df = pd.concat([df, pd.DataFrame([metrics])], ignore_index=True)
    else:
        df = pd.DataFrame([metrics])

    df.to_csv(csv_path, index=False)
    print(f"Metrics appended to {csv_path}")

    return metrics

def save_model(model, filename=None):
    if filename is None:
        filename = f"wr_model_{datetime.now().strftime('%Y%m%d%H%M%S')}.pkl"
    print(f"Saving model to {filename}...")
    os.makedirs(MODEL_DIR, exist_ok=True)
    path = os.path.join(MODEL_DIR, filename)
    joblib.dump(model, path)
    print(f"Model saved to {path}")


def wr_model(season, week):
    print('Training WR model...')
    df = load_data()
    train_df, test_df = train_test_split_for_week(df, season=season, week=week)
    features, X_train, y_train, X_test, y_test = get_features(df, train_df, test_df)
    model = train_model(train_df, features)
    metrics = evaluate_model(model, X_train, y_train, X_test, y_test, season=season, week=week)

    print("Saving model...")
    save_model(model, f"wr_model_season{season}_week{week}_{datetime.now().strftime('%Y%m%d%H%M%S')}.pkl")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train wr model for a given season/week.")
    parser.add_argument("--season", type=int, required=True, help="Season year, e.g., 2024")
    parser.add_argument("--week", type=int, required=True, help="Week number, e.g., 8")
    
    args = parser.parse_args()
    wr_model(args.season, args.week)