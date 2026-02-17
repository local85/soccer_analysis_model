import optuna
import xgboost as xgb
from sklearn.metrics import accuracy_score


def objective(trial, X_train, y_train, X_val, y_val):
    params = {
        'objective': 'binary:logistic',
        'eval_metric': 'logloss',
        'max_depth': trial.suggest_int('max_depth', 3, 7),
        'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.2, log=True),
        'n_estimators': 500,
        'min_child_weight': trial.suggest_int('min_child_weight', 1, 7),
        'subsample': trial.suggest_float('subsample', 0.6, 1.0),
        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
        'early_stopping_rounds': 50,
        'random_state': 42,
    }
    model = xgb.XGBClassifier(**params)
    model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
    return accuracy_score(y_val, model.predict(X_val))


def tune_dimension(X_train, y_train, X_val, y_val, dim_name, n_trials=50):
    """Tune hyperparameters for a single dimension model."""
    print(f"\nTuning {dim_name}...")
    study = optuna.create_study(direction='maximize')
    study.optimize(
        lambda t: objective(t, X_train, y_train, X_val, y_val),
        n_trials=n_trials,
        show_progress_bar=True,
    )
    print(f"  Best accuracy: {study.best_value:.4f}")
    print(f"  Best params: {study.best_params}")
    return study


def tune_all_dimensions(X_train, y_train, X_val, y_val, n_trials=50):
    """Tune each dimension separately and return best params."""
    dim_names = {
        'mentality': 'dim1_scorer_facilitator',
        'work_ethic': 'dim2_warrior_specialist',
        'presence': 'dim3_involved_clinical',
        'temperament': 'dim4_intense_composed',
    }

    best_params = {}
    for dim_key, model_name in dim_names.items():
        study = tune_dimension(
            X_train, y_train[dim_key], X_val, y_val[dim_key],
            model_name, n_trials
        )
        best_params[dim_key] = study.best_params

    return best_params


def train_with_best_params(X_train, y_train, X_val, y_val, best_params):
    """Retrain all models using tuned hyperparameters."""
    dim_names = {
        'mentality': 'dim1_scorer_facilitator',
        'work_ethic': 'dim2_warrior_specialist',
        'presence': 'dim3_involved_clinical',
        'temperament': 'dim4_intense_composed',
    }

    models = {}
    for dim_key, model_name in dim_names.items():
        params = best_params[dim_key]
        model = xgb.XGBClassifier(
            objective='binary:logistic',
            eval_metric='logloss',
            n_estimators=500,
            early_stopping_rounds=50,
            random_state=42,
            **params,
        )
        model.fit(
            X_train, y_train[dim_key],
            eval_set=[(X_val, y_val[dim_key])],
            verbose=False,
        )
        model.save_model(f'../models/{model_name}.json')
        models[model_name] = model
        print(f"Trained {model_name} with tuned params")

    return models
