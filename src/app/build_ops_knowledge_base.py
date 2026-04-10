from pathlib import Path
import pandas as pd


PROCESSED_DIR = Path("data/processed")
OUTPUT_PATH = PROCESSED_DIR / "ops_knowledge_base.txt"


def load_csv_if_exists(filename: str):
    path = PROCESSED_DIR / filename
    if path.exists():
        return pd.read_csv(path)
    return None


def build_feature_importance_section() -> str:
    df = load_csv_if_exists("random_forest_feature_importance.csv")
    if df is None:
        return "Feature Importance:\nNot available.\n"

    top_df = df.head(15)
    lines = ["Feature Importance (Top 15):"]
    for _, row in top_df.iterrows():
        lines.append(f"- {row['feature']}: importance={row['importance']:.6f}")
    return "\n".join(lines) + "\n"


def build_ml_feature_summary() -> str:
    df = load_csv_if_exists("airline_ml_features_v2.csv")
    if df is None:
        return "ML Feature Table Summary:\nNot available.\n"

    lines = [
        "ML Feature Table Summary:",
        f"- Total rows: {len(df)}",
        f"- Total columns: {len(df.columns)}",
    ]

    if "target_anomaly" in df.columns:
        counts = df["target_anomaly"].value_counts().to_dict()
        lines.append(f"- Target distribution: {counts}")

    if "mapped_airline" in df.columns:
        top_airlines = df["mapped_airline"].value_counts().head(10)
        lines.append("- Top airlines by row count:")
        for airline, count in top_airlines.items():
            lines.append(f"  - {airline}: {count}")

    return "\n".join(lines) + "\n"


def build_fused_intelligence_summary() -> str:
    df = load_csv_if_exists("fused_intelligence.csv")
    if df is None:
        return "Fused Intelligence Summary:\nNot available.\n"

    lines = [
        "Fused Intelligence Summary:",
        f"- Total fused rows: {len(df)}",
    ]

    if "mapped_airline" in df.columns:
        top_airlines = df["mapped_airline"].value_counts().head(10)
        lines.append("- Top airlines in fused intelligence:")
        for airline, count in top_airlines.items():
            lines.append(f"  - {airline}: {count}")

    if "route_flights" in df.columns:
        top_routes = df.sort_values("route_flights", ascending=False).head(10)
        lines.append("- Highest route_flights rows:")
        for _, row in top_routes.iterrows():
            airline = row.get("mapped_airline", "Unknown")
            dep = row.get("departure_iata", "Unknown")
            rf = row.get("route_flights", 0)
            lines.append(f"  - {airline} from {dep}: route_flights={rf}")

    return "\n".join(lines) + "\n"


def build_scored_anomaly_summary() -> str:
    df = load_csv_if_exists("airline_ml_features_v2_scored.csv")
    if df is None:
        return "Scored Anomaly Summary:\nNot available.\n"

    lines = [
        "Scored Anomaly Summary:",
        f"- Total scored rows: {len(df)}",
    ]

    if "predicted_probability" in df.columns:
        top_df = df.sort_values("predicted_probability", ascending=False).head(15)
        lines.append("- Highest predicted anomaly probabilities:")
        for _, row in top_df.iterrows():
            airline = row.get("mapped_airline", "Unknown")
            prob = row.get("predicted_probability", 0)
            lf = row.get("live_flights", 0)
            lines.append(f"  - {airline}: predicted_probability={prob:.4f}, live_flights={lf}")

    return "\n".join(lines) + "\n"


def main():
    sections = [
        "Flight Operations Knowledge Base",
        "=" * 40,
        build_feature_importance_section(),
        build_ml_feature_summary(),
        build_fused_intelligence_summary(),
        build_scored_anomaly_summary(),
    ]

    text = "\n\n".join(sections)
    OUTPUT_PATH.write_text(text, encoding="utf-8")

    print(f"Knowledge base saved to: {OUTPUT_PATH}")
    print("\nPreview:\n")
    print(text[:3000])


if __name__ == "__main__":
    main()