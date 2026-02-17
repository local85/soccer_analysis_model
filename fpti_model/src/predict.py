import pandas as pd
import numpy as np
import xgboost as xgb
from src.features import FEATURE_COLS, POS_GROUP_COLS
from src.preprocessing import preprocess_data
from src.labeling import assign_fpti


DIM_ORDER = [
    ('dim1_scorer_facilitator', 'S', 'F', 'S/F'),
    ('dim2_warrior_specialist', 'W', 'P', 'W/P'),
    ('dim3_involved_clinical', 'I', 'C', 'I/C'),
    ('dim4_intense_composed', 'N', 'O', 'N/O'),
]


def load_models(models_dir='../models'):
    """Load all 4 trained dimension models."""
    models = {}
    for model_name, _, _, _ in DIM_ORDER:
        model = xgb.XGBClassifier()
        model.load_model(f'{models_dir}/{model_name}.json')
        models[model_name] = model
    return models


def prepare_features(player_stats):
    """Prepare feature vector from a dict or DataFrame row."""
    if isinstance(player_stats, dict):
        player_stats = pd.DataFrame([player_stats])

    ALL_FEATURES = FEATURE_COLS + POS_GROUP_COLS

    # Create position dummies if not present
    for col in POS_GROUP_COLS:
        if col not in player_stats.columns:
            player_stats[col] = False

    return player_stats[ALL_FEATURES]


def predict_fpti(player_stats, models):
    """
    Predict FPTI code for a player.

    Args:
        player_stats: dict or DataFrame row with feature columns
        models: dict of loaded XGBoost models

    Returns:
        dict with 'fpti', 'confidence' per dimension, and 'overall_confidence'
    """
    features = prepare_features(player_stats)

    result = {'fpti': '', 'confidence': {}}

    for model_name, pos, neg, label in DIM_ORDER:
        model = models[model_name]
        prob = model.predict_proba(features)[0][1]
        letter = pos if prob >= 0.5 else neg
        result['fpti'] += letter
        result['confidence'][label] = round(max(prob, 1 - prob), 3)

    result['overall_confidence'] = round(
        np.mean(list(result['confidence'].values())), 3
    )
    return result


def predict_batch(df, models):
    """
    Predict FPTI for all rows in a DataFrame.

    Args:
        df: DataFrame with feature columns and pos_group
        models: dict of loaded XGBoost models

    Returns:
        DataFrame with predicted_fpti and confidence columns added
    """
    from src.features import build_feature_matrix
    X = build_feature_matrix(df)

    df = df.copy()
    df['predicted_fpti'] = ''

    for model_name, pos, neg, label in DIM_ORDER:
        model = models[model_name]
        probs = model.predict_proba(X)[:, 1]
        letters = np.where(probs >= 0.5, pos, neg)
        df['predicted_fpti'] = df['predicted_fpti'] + letters
        df[f'conf_{label}'] = np.round(np.maximum(probs, 1 - probs), 3)

    conf_cols = [f'conf_{label}' for _, _, _, label in DIM_ORDER]
    df['overall_confidence'] = df[conf_cols].mean(axis=1).round(3)

    return df
