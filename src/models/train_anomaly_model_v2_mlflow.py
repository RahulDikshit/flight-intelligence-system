import pandas as pd
from pathlib import Path
import joblib
import mlflow
import mlflow.sklearn

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
)
from sklearn.model_selection import GroupShuffleSplit


DATA_PATH = Path("data/processed/airline_ml_features_v2.csv")
MODEL_DIR = Path("models")
MODEL_DIR.mkdir(exist_ok=True)


def load_data() -> pd.DataFrame:
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Feature table not found: {DATA_PATH}")
    return pd.read_csv(DATA_PATH)


def prepare_features(df: pd.DataFrame):
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


def evaluate_and_log_model(model_name, pipeline, X_train, X_test, y_train, y_test, params: dict):
    with mlflow.start_run(run_name=model_name):
        mlflow.log_param("model_name", model_name)

        for k, v in params.items():
            mlflow.log_param(k, v)

        pipeline.fit(X_train, y_train)
        preds = pipeline.predict(X_test)

        accuracy = accuracy_score(y_test, preds)
        f1 = f1_score(y_test, preds)
        precision = precision_score(y_test, preds)
        recall = recall_score(y_test, preds)

        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("f1_score", f1)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)

        print(f"\n{model_name} Results")
        print("-" * 50)
        print("Accuracy:", round(accuracy, 4))
        print("F1 Score:", round(f1, 4))
        print("Precision:", round(precision, 4))
        print("Recall:", round(recall, 4))
        print("\nConfusion Matrix:")
        print(confusion_matrix(y_test, preds))
        print("\nClassification Report:")
        print(classification_report(y_test, preds))

        mlflow.sklearn.log_model(
            sk_model=pipeline,
            artifact_path=model_name.lower().replace(" ", "_")
        )

        output_path = MODEL_DIR / f"{model_name.lower().replace(' ', '_')}.pkl"
        joblib.dump(pipeline, output_path)
        mlflow.log_artifact(str(output_path))

        print(f"Saved model to: {output_path}")


def main():
    mlflow.set_experiment("flight_ops_anomaly_detection_v2")

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

    evaluate_and_log_model(
        model_name="Logistic Regression V2",
        pipeline=logistic_pipeline,
        X_train=X_train,
        X_test=X_test,
        y_train=y_train,
        y_test=y_test,
        params={
            "split_type": "GroupShuffleSplit",
            "feature_version": "v2_leakage_safe",
        }
    )

    evaluate_and_log_model(
        model_name="Random Forest V2",
        pipeline=rf_pipeline,
        X_train=X_train,
        X_test=X_test,
        y_train=y_train,
        y_test=y_test,
        params={
            "split_type": "GroupShuffleSplit",
            "feature_version": "v2_leakage_safe",
        }
    )

    print("\nMLflow V2 experiment logging complete.")


if __name__ == "__main__":
    main()