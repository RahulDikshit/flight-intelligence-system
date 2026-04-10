import pandas as pd

from src.database.connection import get_engine


def load_snapshot_level_counts() -> pd.DataFrame:
    engine = get_engine()

    query = """
    SELECT
        ingestion_time_utc,
        mapped_airline,
        COUNT(*) AS live_flights,
        ROUND(AVG(velocity), 2) AS avg_velocity,
        ROUND(AVG(baro_altitude), 2) AS avg_altitude,
        SUM(CASE WHEN on_ground = 1 THEN 1 ELSE 0 END) AS on_ground_count,
        SUM(CASE WHEN on_ground = 0 THEN 1 ELSE 0 END) AS airborne_count
    FROM flight_states
    GROUP BY ingestion_time_utc, mapped_airline
    ORDER BY ingestion_time_utc ASC, mapped_airline ASC;
    """
    return pd.read_sql(query, engine)


def load_aviation_context_aggregated() -> pd.DataFrame:
    engine = get_engine()

    query = """
    SELECT
        airline_name,
        COUNT(*) AS total_route_flights,
        COUNT(DISTINCT departure_iata) AS distinct_departure_hubs
    FROM aviationstack_flights
    GROUP BY airline_name
    """
    return pd.read_sql(query, engine)


def build_target(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["prev_live_flights"] = df.groupby("mapped_airline")["live_flights"].shift(1)
    df["prev_live_flights"] = df["prev_live_flights"].fillna(df["live_flights"])

    # This stays for target creation only
    df["count_change"] = df["live_flights"] - df["prev_live_flights"]
    df["count_change"] = df["count_change"].fillna(0)

    # label only
    df["target_anomaly"] = df["count_change"].apply(lambda x: 1 if abs(x) >= 5 else 0)

    return df


def build_feature_table() -> pd.DataFrame:
    snapshot_df = load_snapshot_level_counts()
    context_df = load_aviation_context_aggregated()

    df = build_target(snapshot_df)

    df = df.merge(
        context_df,
        left_on="mapped_airline",
        right_on="airline_name",
        how="left"
    )

    df["total_route_flights"] = df["total_route_flights"].fillna(0)
    df["distinct_departure_hubs"] = df["distinct_departure_hubs"].fillna(0)
    df["has_aviationstack_context"] = df["airline_name"].notna().astype(int)

    if "airline_name" in df.columns:
        df = df.drop(columns=["airline_name"])

    df["airborne_ratio"] = df["airborne_count"] / df["live_flights"].replace(0, 1)
    df["on_ground_ratio"] = df["on_ground_count"] / df["live_flights"].replace(0, 1)

    # keep count_change only for analysis if you want, but do NOT use it in model
    df = df.sort_values(by=["ingestion_time_utc", "mapped_airline"]).reset_index(drop=True)

    return df


def main() -> None:
    df = build_feature_table()

    print("Airline ML V2 feature table:")
    print(df.head(20))
    print("\nShape:", df.shape)
    print("\nColumns:")
    print(df.columns.tolist())

    output_path = "data/processed/airline_ml_features_v2.csv"
    df.to_csv(output_path, index=False)

    print(f"\nSaved ML V2 feature table to: {output_path}")
    print("\nTarget distribution:")
    print(df["target_anomaly"].value_counts())


if __name__ == "__main__":
    main()