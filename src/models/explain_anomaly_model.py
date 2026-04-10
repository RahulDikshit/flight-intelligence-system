import joblib
import pandas as pd
from pathlib import Path


MODEL_PATH = Path("models/random_forest.pkl")
DATA_PATH = Path("data/processed/airline_ml_features.csv")
OUTPUT_DIR = Path("data/processed")
OUTPUT_DIR.mkdir(exist_ok=True)


def load_model():
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"Model file not found: {MODEL_PATH}")
    return joblib.load(MODEL_PATH)


def load_data():
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Feature file not found: {DATA_PATH}")
    return pd.read_csv(DATA_PATH)


def get_feature_names_from_pipeline(model, X):
    preprocessor = model.named_steps["preprocessor"]

    categorical_cols = ["mapped_airline", "departure_iata"]
    numeric_cols = [col for col in X.columns if col not in categorical_cols]

    cat_transformer = preprocessor.named_transformers_["cat"]
    onehot = cat_transformer.named_steps["onehot"]
    cat_feature_names = onehot.get_feature_names_out(categorical_cols)

    feature_names = list(numeric_cols) + list(cat_feature_names)
    return feature_names


def main():
    model = load_model()
    df = load_data()

    X = df.drop(columns=["target_anomaly", "ingestion_time_utc"])
    y = df["target_anomaly"]

    preds = model.predict(X)
    probs = model.predict_proba(X)[:, 1]

    df["predicted_anomaly"] = preds
    df["predicted_probability"] = probs

    # Save scored dataset
    scored_path = OUTPUT_DIR / "airline_ml_features_scored.csv"
    df.to_csv(scored_path, index=False)

    print("Saved scored dataset to:")
    print(scored_path)

    # Feature importance
    classifier = model.named_steps["classifier"]
    feature_names = get_feature_names_from_pipeline(model, X)

    importances = classifier.feature_importances_
    feature_importance_df = pd.DataFrame({
        "feature": feature_names,
        "importance": importances
    }).sort_values(by="importance", ascending=False)

    importance_path = OUTPUT_DIR / "random_forest_feature_importance.csv"
    feature_importance_df.to_csv(importance_path, index=False)

    print("\nTop 20 feature importances:")
    print(feature_importance_df.head(20))

    print("\nSaved feature importance to:")
    print(importance_path)

    # Top predicted anomalies
    top_anomalies = df.sort_values(by="predicted_probability", ascending=False).head(20)

    print("\nTop predicted anomaly rows:")
    print(
        top_anomalies[
            [
                "ingestion_time_utc",
                "mapped_airline",
                "live_flights",
                "count_change",
                "route_flights",
                "has_aviationstack_context",
                "predicted_anomaly",
                "predicted_probability",
            ]
        ]
    )


if __name__ == "__main__":
    main()