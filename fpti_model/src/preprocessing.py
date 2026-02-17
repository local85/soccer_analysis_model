import numpy as np
import pandas as pd

def preprocess_data(filepath):

    df = pd.read_csv(filepath)

    df = df[df['minutes'] >= 1500].copy()
    df = df.dropna(subset=['tackles_per_90', 'interceptions_per_90'])
    df['xg_p90'] = df['xg'] / df['minutes'] * 90
    df['xa_p90'] = df['xa'] / df['minutes'] * 90
    df['npxg_p90'] = df['npxg'] / df['minutes'] * 90
    df['shots_p90'] = df['shots'] / df['minutes'] * 90
    df['key_passes_p90'] = df['key_passes'] / df['minutes'] * 90
    df['xg_chain_p90'] = df['xg_chain'] / df['minutes'] * 90
    df['xg_buildup_p90'] = df['xg_buildup'] / df['minutes'] * 90
    df['fouls_p90'] = df['fouls_committed'] / df['minutes'] * 90
    df['yellow_cards_p90'] = df['yellow_cards'] / df['minutes'] * 90
    df['red_cards_p90'] = df['red_cards'] / df['minutes'] * 90

    df['goal_share'] = df['xg'] / (df['xg'] + df['xa']).replace(0, np.nan)
    df['goal_share'] = df['goal_share'].fillna(0.5)

    df['defensive_actions_p90'] = (df['tackles_per_90'] + df['interceptions_per_90'])

    def map_pos_group(pos):
        if pd.isna(pos):
            return "UNK"
        pos = str(pos).strip().upper()

        if pos.startswith("F") or pos in ['S', 'SUB']:
            return "FWD"
        elif pos.startswith("M"):
            return "MID"
        elif pos.startswith("D"):
            return "DEF"
        elif pos.startswith("G"):
            return "GK"
        return "UNK"

    df['pos_group'] = df['position'].apply(map_pos_group)
    df = df[df['pos_group'].isin(['FWD', 'MID', 'DEF'])]

    return df