import pandas as pd
import joblib
from pathlib import Path


DATA_PATH = Path("data/processed/airline_ml_features_v2.csv")
MODEL_PATH = Path("models/logistic_anomaly_model_v2.pkl")
OUTPUT_SCORED = Path("data/processed/airline_ml_features_v2_scored.csv")


def load_data():
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Missing feature file: {DATA_PATH}")
    return pd.read_csv(DATA_PATH)


def main():
    df = load_data()

    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"Missing V2 model file: {MODEL_PATH}")

    model = joblib.load(MODEL_PATH)

    drop_cols = ["target_anomaly", "count_change", "prev_live_flights"]
    X = df.drop(columns=drop_cols, errors="ignore")

    # model expects no ingestion_time_utc as input
    X_model = X.drop(columns=["ingestion_time_utc"], errors="ignore")

    preds = model.predict(X_model)

    if hasattr(model, "predict_proba"):
        probs = model.predict_proba(X_model)[:, 1]
    else:
        probs = [None] * len(df)

    scored_df = df.copy()
    scored_df["predicted_anomaly"] = preds
    scored_df["predicted_probability"] = probs

    scored_df.to_csv(OUTPUT_SCORED, index=False)

    print(f"Saved V2 scored dataset to: {OUTPUT_SCORED}")
    print("\nTop predicted anomaly rows:")
    top_df = scored_df.sort_values("predicted_probability", ascending=False).head(20)
    cols_to_show = [
        "ingestion_time_utc",
        "mapped_airline",
        "live_flights",
        "avg_velocity",
        "avg_altitude",
        "predicted_anomaly",
        "predicted_probability",
    ]
    existing_cols = [c for c in cols_to_show if c in top_df.columns]
    print(top_df[existing_cols])


if __name__ == "__main__":
    main()