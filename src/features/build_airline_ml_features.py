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


def load_aviation_route_features() -> pd.DataFrame:
    engine = get_engine()

    query = """
    SELECT
        airline_name,
        departure_iata,
        COUNT(*) AS route_flights
    FROM aviationstack_flights
    GROUP BY airline_name, departure_iata
    """
    return pd.read_sql(query, engine)


def load_anomaly_flags() -> pd.DataFrame:
    engine = get_engine()

    query = """
    SELECT
        ingestion_time_utc,
        mapped_airline,
        COUNT(*) AS live_flights
    FROM flight_states
    GROUP BY ingestion_time_utc, mapped_airline
    ORDER BY ingestion_time_utc ASC;
    """
    df = pd.read_sql(query, engine)

    df["prev_live_flights"] = df.groupby("mapped_airline")["live_flights"].shift(1)
    df["count_change"] = df["live_flights"] - df["prev_live_flights"]

    df["prev_live_flights"] = df["prev_live_flights"].fillna(df["live_flights"])
    df["count_change"] = df["count_change"].fillna(0)

    df["target_anomaly"] = df["count_change"].apply(lambda x: 1 if abs(x) >= 5 else 0)

    return df[["ingestion_time_utc", "mapped_airline", "prev_live_flights", "count_change", "target_anomaly"]]


def build_feature_table() -> pd.DataFrame:
    snapshot_df = load_snapshot_level_counts()
    route_df = load_aviation_route_features()
    anomaly_df = load_anomaly_flags()

    df = snapshot_df.merge(
        anomaly_df,
        on=["ingestion_time_utc", "mapped_airline"],
        how="left"
    )

    df = df.merge(
        route_df,
        left_on="mapped_airline",
        right_on="airline_name",
        how="left"
    )

    df["route_flights"] = df["route_flights"].fillna(0)
    df["has_aviationstack_context"] = df["airline_name"].notna().astype(int)

    if "airline_name" in df.columns:
        df = df.drop(columns=["airline_name"])

    df["airborne_ratio"] = df["airborne_count"] / df["live_flights"].replace(0, 1)
    df["on_ground_ratio"] = df["on_ground_count"] / df["live_flights"].replace(0, 1)

    df = df.sort_values(by=["ingestion_time_utc", "mapped_airline"]).reset_index(drop=True)

    return df


def main() -> None:
    df = build_feature_table()

    print("Airline ML feature table:")
    print(df.head(20))
    print("\nShape:", df.shape)
    print("\nColumns:")
    print(df.columns.tolist())

    output_path = "data/processed/airline_ml_features.csv"
    df.to_csv(output_path, index=False)

    print(f"\nSaved ML feature table to: {output_path}")
    print("\nTarget distribution:")
    print(df["target_anomaly"].value_counts())


if __name__ == "__main__":
    main()