def split_features_by_position(final_df, engine):
    print('Splitting features by position...')
    qb_df = final_df[final_df['position'] == 'QB'].copy()
    rb_df = final_df[final_df['position'] == 'RB'].copy()
    wr_df = final_df[final_df['position'] == 'WR'].copy()
    te_df = final_df[final_df['position'] == 'TE'].copy()

    qb_df.to_csv('data/processed/qb_features.csv', index=False)
    rb_df.to_csv('data/processed/rb_features.csv', index=False)
    wr_df.to_csv('data/processed/wr_features.csv', index=False)
    te_df.to_csv('data/processed/te_features.csv', index=False)

    qb_df.to_sql('qb_features', engine, if_exists='replace', index=False)
    rb_df.to_sql('rb_features', engine, if_exists='replace', index=False)
    wr_df.to_sql('wr_features', engine, if_exists='replace', index=False)
    te_df.to_sql('te_features', engine, if_exists='replace', index=False)

    return qb_df, rb_df, wr_df, te_df