from src.features.rolling_features import add_season_to_date_aggregates, add_3wk_rolling_averages, add_5wk_rolling_averages
from src.features.home_away_features import add_home_away_rolling_and_std_averages
from src.features.base_features import df

df = add_season_to_date_aggregates(df)
df = add_3wk_rolling_averages(df)
df = add_5wk_rolling_averages(df)

df = add_home_away_rolling_and_std_averages(df, window=3)

print(df.head())