import xgboost as xgb


def train_dimension_model(X_train, y_train, X_val, y_val, dim_name):
    model = xgb.XGBClassifier(
        objective='binary:logistic',
        eval_metric='logloss',
        max_depth=5,
        learning_rate=0.05,
        n_estimators=500,
        min_child_weight=3,
        subsample=0.8,
        colsample_bytree=0.8,
        early_stopping_rounds=50,
        random_state=42
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=20
    )

    model.save_model(f'../models/{dim_name}.json')
    return model


def train_all_models(X_train, y_train, X_val, y_val):
    dim_names = {
        'mentality': 'dim1_scorer_facilitator',
        'work_ethic': 'dim2_warrior_specialist',
        'presence': 'dim3_involved_clinical',
        'temperament': 'dim4_intense_composed',
    }
    models = {}
    for dim_key, model_name in dim_names.items():
        models[model_name] = train_dimension_model(
            X_train, y_train[dim_key], X_val, y_val[dim_key], model_name
        )
        print(f"Trained {model_name}")
    return models
