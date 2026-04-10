import pandas as pd
from pathlib import Path
import joblib

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score, f1_score
from sklearn.model_selection import GroupShuffleSplit


DATA_PATH = Path("data/processed/airline_ml_features_v2.csv")
MODEL_DIR = Path("models")
MODEL_DIR.mkdir(exist_ok=True)


def load_data() -> pd.DataFrame:
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Feature table not found: {DATA_PATH}")
    return pd.read_csv(DATA_PATH)


def prepare_features(df: pd.DataFrame):
    # REMOVE leakage-heavy columns from X
    drop_cols = [
        "target_anomaly",
        "count_change",
        "prev_live_flights",
    ]

    X = df.drop(columns=drop_cols)
    y = df["target_anomaly"]
    groups = df["ingestion_time_utc"]

    return X, y, groups


def build_preprocessor(X: pd.DataFrame):
    categorical_cols = ["mapped_airline"]
    numeric_cols = [col for col in X.columns if col not in categorical_cols + ["ingestion_time_utc"]]

    numeric_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
    ])

    categorical_transformer = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("onehot", OneHotEncoder(handle_unknown="ignore")),
    ])

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_cols),
            ("cat", categorical_transformer, categorical_cols),
        ]
    )

    return preprocessor


def evaluate_model(name, model, X_train, X_test, y_train, y_test):
    model.fit(X_train, y_train)
    preds = model.predict(X_test)

    print(f"\n{name} Results")
    print("-" * 50)
    print("Accuracy:", round(accuracy_score(y_test, preds), 4))
    print("F1 Score:", round(f1_score(y_test, preds), 4))
    print("\nConfusion Matrix:")
    print(confusion_matrix(y_test, preds))
    print("\nClassification Report:")
    print(classification_report(y_test, preds))

    return model


def main():
    df = load_data()
    print("Loaded V2 feature table shape:", df.shape)

    X, y, groups = prepare_features(df)
    print("Target distribution:")
    print(y.value_counts())

    preprocessor = build_preprocessor(X)

    splitter = GroupShuffleSplit(n_splits=1, test_size=0.25, random_state=42)
    train_idx, test_idx = next(splitter.split(X, y, groups=groups))

    X_train = X.iloc[train_idx].drop(columns=["ingestion_time_utc"])
    X_test = X.iloc[test_idx].drop(columns=["ingestion_time_utc"])
    y_train = y.iloc[train_idx]
    y_test = y.iloc[test_idx]

    print("\nTrain shape:", X_train.shape)
    print("Test shape:", X_test.shape)
    print("Unique train snapshots:", len(set(groups.iloc[train_idx])))
    print("Unique test snapshots:", len(set(groups.iloc[test_idx])))

    logistic_pipeline = Pipeline(steps=[
        ("preprocessor", preprocessor),
        ("classifier", LogisticRegression(max_iter=1000, class_weight="balanced")),
    ])

    rf_pipeline = Pipeline(steps=[
        ("preprocessor", preprocessor),
        ("classifier", RandomForestClassifier(
            n_estimators=200,
            random_state=42,
            class_weight="balanced"
        )),
    ])

    logistic_model = evaluate_model(
        "Logistic Regression V2",
        logistic_pipeline,
        X_train, X_test, y_train, y_test
    )

    rf_model = evaluate_model(
        "Random Forest V2",
        rf_pipeline,
        X_train, X_test, y_train, y_test
    )

    joblib.dump(logistic_model, MODEL_DIR / "logistic_anomaly_model_v2.pkl")
    joblib.dump(rf_model, MODEL_DIR / "rf_anomaly_model_v2.pkl")

    print("\nSaved V2 models to:")
    print(MODEL_DIR / "logistic_anomaly_model_v2.pkl")
    print(MODEL_DIR / "rf_anomaly_model_v2.pkl")


if __name__ == "__main__":
    main()