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
from skopt import BayesSearchCV
from skopt.space import Real, Integer
pd.set_option('mode.chained_assignment', None)

# ------------------------
# CONFIG
# ------------------------
def set_config(position):
    POSITION = position
    MODEL_DIR = "models"
    TABLE_NAME = f"{POSITION}_features"
    TARGET = "fantasy_points"
    EXCLUDE_COLS = ["player_id", "player_name", "season", "week", "player_display_name", "position", "team_abbreviation", TARGET]

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    MODEL_DIR = os.path.join(BASE_DIR, "models")
    LOG_DIR = os.path.join(BASE_DIR, "./model_logs")
    return POSITION, MODEL_DIR, TABLE_NAME, TARGET, EXCLUDE_COLS, LOG_DIR
# ------------------------
# FUNCTIONS
# ------------------------
def load_data(TABLE_NAME):
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


def get_features(df, train_df, test_df, EXCLUDE_COLS, TARGET):
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


def bayes_hyperparameter_tuning(X_train, y_train):
    print("Starting Bayesian hyperparameter tuning...")

    # Define parameter search space
    param_space = {
        'n_estimators': Integer(100, 1000),
        'max_depth': Integer(3, 10),
        'learning_rate': Real(0.01, 0.2, prior='log-uniform'),
        'subsample': Real(0.5, 1.0),
        'colsample_bytree': Real(0.5, 1.0),
    }

    xgb = XGBRegressor(
        random_state=42,
        enable_categorical=True,
        tree_method='hist',  # faster, optional
        n_jobs=-1,
    )

    opt = BayesSearchCV(
        xgb,
        param_space,
        n_iter=30,  # number of parameter settings that are sampled
        scoring='neg_mean_absolute_error',
        cv=3,
        n_jobs=-1,
        verbose=0,
        random_state=42,
    )

    opt.fit(X_train, y_train)

    print("Best parameters found:")
    print(opt.best_params_)
    print(f"Best MAE: {-opt.best_score_:.4f}")

    return opt.best_estimator_, opt.best_params_


def train_model(train_df, features, TARGET):
    print("Training XGBoost model with Bayesian hyperparameter tuning...")
    X_train = train_df[features]
    y_train = train_df[TARGET]

    best_model, best_params = bayes_hyperparameter_tuning(X_train, y_train)
    return best_model, best_params


def evaluate_model(model,
                   X_train,
                   y_train,
                   X_test,
                   y_test,
                   season,
                   week,
                   best_params,
                   POSITION,
                   LOG_DIR,
                   log_file="metrics_logs.csv",
                   model_name=None):
    """Evaluate the model, log metrics, and decide whether to save it."""

    # Predictions
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)

    if model_name is None:
        model_name = f"{POSITION}_model_season{season}_week{week}_{datetime.now().strftime('%Y%m%d%H%M%S')}.pkl"
    print("Evaluating model performance...")

    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)

    metrics = {
        "season": season,
        "week": week,
        "position": POSITION,
        "rmse_train": np.sqrt(mean_squared_error(y_train, y_pred_train)),
        "mae_train": mean_absolute_error(y_train, y_pred_train),
        "r2_train": r2_score(y_train, y_pred_train),
        "rmse_test": np.sqrt(mean_squared_error(y_test, y_pred_test)),
        "mae_test": mean_absolute_error(y_test, y_pred_test),
        "r2_test": r2_score(y_test, y_pred_test),
        "params": best_params,
        "timestamp": datetime.now().strftime("%Y%m%d%H%M%S"),
        "model_name": model_name
    }

    # Ensure logging directory exists
    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, log_file)

    # Create or load existing log
    if os.path.exists(log_path):
        logs = pd.read_csv(log_path)
    else:
        logs = pd.DataFrame(columns=["season", "week", "position", "mae", "r2"])

    # Filter logs for the same season/week/position
    matching_logs = logs[
        (logs["season"] == season) &
        (logs["week"] == week) &
        (logs["position"] == POSITION)
    ]

    # Save model if first run OR better performance
    save_model_flag = False
    if matching_logs.empty:
        print(f"No prior runs for {season} Week {week} {POSITION} — saving model.")
        save_model_flag = True
    else:
        best_mae = matching_logs["mae"].min()
        if metrics['mae_test'] < best_mae:
            print(f"New model beats best MAE ({best_mae:.4f}).")
            save_model_flag = True
        else:
            print(f"Model did NOT outperform existing best MAE ({best_mae:.4f}). Model will NOT be saved.")

    # Log current metrics
    new_row = pd.DataFrame([{
        "season": season,
        "week": week,
        "position": POSITION,
        "mae": metrics['mae_test'],
        "r2": metrics['r2_test'],
        "model_name": model_name,
        "params": dict(metrics['params'])
    }])
    logs = pd.concat([logs, new_row], ignore_index=True)
    logs.to_csv(log_path, index=False)

    return metrics, save_model_flag


def save_model(model, MODEL_DIR, POSITION, filename=None):
    if filename is None:
        filename = f"{POSITION}_model_{datetime.now().strftime('%Y%m%d%H%M%S')}.pkl"
    print(f"Saving model to {filename}...")
    os.makedirs(MODEL_DIR, exist_ok=True)
    path = os.path.join(MODEL_DIR, filename)
    joblib.dump(model, path)
    print(f"Model saved to {path}")


def model(season, week, position):
    POSITION, MODEL_DIR, TABLE_NAME, TARGET, EXCLUDE_COLS, LOG_DIR = set_config(position)
    df = load_data(TABLE_NAME)
    train_df, test_df = train_test_split_for_week(df, season=season, week=week)
    features, X_train, y_train, X_test, y_test = get_features(df, train_df, test_df, EXCLUDE_COLS, TARGET)
    model, best_params = train_model(train_df, features, TARGET)
    
    metrics, save_model_flag= evaluate_model(model, X_train, y_train, X_test, y_test, season, week, best_params, POSITION, LOG_DIR)

    if save_model_flag:
        print("Saving model...")
        save_model(model, MODEL_DIR, POSITION, f"{POSITION}_model_season{season}_week{week}.pkl")
    else:
        print("Model not saved since it did not improve over the best existing model.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=f"Train all 4 position model for a given season/week.")
    parser.add_argument("--season", type=int, required=True, help="Season year, e.g., 2023")
    parser.add_argument("--week", type=int, required=True, help="Week number, e.g., 8")
    
    args = parser.parse_args()
    model(args.season, args.week, position)
