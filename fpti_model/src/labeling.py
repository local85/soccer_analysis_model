from scipy.stats import zscore
import numpy as np

def compute_dimension_scores(df):
    df['scorer_facilitator'] = zscore(df['goal_share'].fillna(0.5))
    df['warrior_specialist'] = zscore(df['defensive_actions_p90'].fillna(0))
    df['involved_clinical'] = zscore(df['xg_chain_p90'].fillna(0))
    df['intense_composed'] = zscore(
        0.5 * df['fouls_p90'].fillna(0) +
        0.3 * df['yellow_cards_p90'].fillna(0) +
        0.2 * df['red_cards_p90'].fillna(0)
    )

    return df

def assign_fpti(df):
    df = compute_dimension_scores(df)

    df['mentality'] = np.where(df['scorer_facilitator'] >= 0, 'S', 'F')
    df['work_ethic'] = np.where(df['warrior_specialist'] >= 0, 'W', 'P')
    df['presence'] = np.where(df['involved_clinical'] >= 0, 'I', 'C')
    df['temperament'] = np.where(df['intense_composed'] >= 0, 'N', 'O')

    df['fpti'] = df['mentality'] + df['work_ethic'] + df["presence"] + df["temperament"]
    return df
