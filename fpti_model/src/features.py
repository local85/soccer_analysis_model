import pandas as pd
from sklearn.model_selection import GroupShuffleSplit

FEATURE_COLS = [
    # Offensive (predict S/F from these, not goal_share directly)
    'xg_p90', 'xa_p90', 'npxg_p90', 'shots_p90', 'key_passes_p90',
    'xg_buildup_p90',

    # Defensive (predict W/P from these, not defensive_actions_p90 directly)
    'tackles_per_90', 'interceptions_per_90', 'clearances_per_90',

    # Temperament
    'fouls_p90', 'yellow_cards_p90', 'red_cards_p90',

    # Context
    'minutes',
]

POS_GROUP_COLS = ['pos_FWD', 'pos_MID', 'pos_DEF']

def build_feature_matrix(df):
    """
    Given a DataFrame `df` containing a 'pos_group' column and the feature columns
    defined in FEATURE_COLS, return a DataFrame X with the selected features
    and position dummy columns.
    """
    pos_dummies = pd.get_dummies(df["pos_group"], prefix='pos')
    df_with_pos = pd.concat([df, pos_dummies], axis=1)

    ALL_FEATURES = FEATURE_COLS + POS_GROUP_COLS
    X = df_with_pos[ALL_FEATURES]
    return X

def split_data(df):
    X = build_feature_matrix(df)
    groups = df['player_name']

    gss1 = GroupShuffleSplit(n_splits=1, test_size=0.3, random_state=42)
    train_idx, temp_idx = next(gss1.split(X, groups=groups))

    X_train = X.iloc[train_idx]
    X_temp = X.iloc[temp_idx]
    df_temp = df.iloc[temp_idx]

    groups_temp = df_temp['player_name']
    gss2 = GroupShuffleSplit(n_splits=1, test_size=0.5, random_state=42)
    val_idx, test_idx = next(gss2.split(X_temp, groups=groups_temp))

    X_val = X_temp.iloc[val_idx]
    X_test = X_temp.iloc[test_idx]

    dims = [('mentality', 'S'), ('work_ethic', 'W'), ('presence', 'I'), ('temperament', 'N')]

    y_train = {d: (df.iloc[train_idx][d] == p).astype(int) for d, p in dims}
    y_val = {d: (df_temp.iloc[val_idx][d] == p).astype(int) for d, p in dims}
    y_test = {d: (df_temp.iloc[test_idx][d] == p).astype(int) for d, p in dims}

    return X_train, X_val, X_test, y_train, y_val, y_test
