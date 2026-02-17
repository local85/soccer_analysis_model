from sklearn.metrics import accuracy_score, roc_auc_score, f1_score, confusion_matrix
import numpy as np


DIM_ORDER = [
    ('mentality', 'dim1_scorer_facilitator', 'S', 'F'),
    ('work_ethic', 'dim2_warrior_specialist', 'W', 'P'),
    ('presence', 'dim3_involved_clinical', 'I', 'C'),
    ('temperament', 'dim4_intense_composed', 'N', 'O'),
]


def evaluate_per_dimension(models, X_test, y_test):
    """Evaluate each binary classifier individually."""
    results = {}
    for dim_key, model_name, pos, neg in DIM_ORDER:
        model = models[model_name]
        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]
        y_true = y_test[dim_key]

        acc = accuracy_score(y_true, y_pred)
        auc = roc_auc_score(y_true, y_prob)
        f1 = f1_score(y_true, y_pred)
        cm = confusion_matrix(y_true, y_pred)

        results[model_name] = {'accuracy': acc, 'roc_auc': auc, 'f1': f1}

        print(f"\n=== {model_name} ({pos} vs {neg}) ===")
        print(f"Accuracy:  {acc:.3f}")
        print(f"ROC-AUC:   {auc:.3f}")
        print(f"F1-Score:  {f1:.3f}")
        print(f"Confusion Matrix:\n{cm}")

    return results


def evaluate_full_fpti(models, X_test, y_test, df_test):
    """Evaluate full 4-letter FPTI prediction accuracy."""
    # Build predicted FPTI codes
    predicted_codes = [''] * len(X_test)
    for dim_key, model_name, pos, neg in DIM_ORDER:
        model = models[model_name]
        preds = model.predict(X_test)
        for i in range(len(X_test)):
            predicted_codes[i] += pos if preds[i] == 1 else neg

    actual_codes = df_test['fpti'].values

    # Exact match
    exact = np.mean([p == a for p, a in zip(predicted_codes, actual_codes)])

    # Partial matches
    dims_correct = []
    for p, a in zip(predicted_codes, actual_codes):
        n_correct = sum(p[i] == a[i] for i in range(4))
        dims_correct.append(n_correct)

    partial_4 = np.mean([d == 4 for d in dims_correct])
    partial_3 = np.mean([d >= 3 for d in dims_correct])
    partial_2 = np.mean([d >= 2 for d in dims_correct])
    avg_dims = np.mean(dims_correct)

    print(f"\n=== Full FPTI Evaluation ===")
    print(f"Exact match (4/4):    {partial_4:.3f}")
    print(f"3/4 dimensions:       {partial_3:.3f}")
    print(f"2/4 dimensions:       {partial_2:.3f}")
    print(f"Avg dimensions correct: {avg_dims:.2f} / 4")

    # Per-dimension accuracy summary
    print(f"\nPer-dimension accuracy:")
    for dim_key, model_name, pos, neg in DIM_ORDER:
        model = models[model_name]
        preds = model.predict(X_test)
        acc = accuracy_score(y_test[dim_key], preds)
        print(f"  {dim_key:15s} ({pos}/{neg}): {acc:.3f}")

    return {
        'exact_match': partial_4,
        'partial_3': partial_3,
        'partial_2': partial_2,
        'avg_dims_correct': avg_dims,
        'predicted_codes': predicted_codes,
    }
