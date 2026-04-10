import pandas as pd

from src.database.connection import get_engine


def get_latest_two_snapshots(engine):
    query = """
    SELECT DISTINCT ingestion_time_utc
    FROM flight_states
    ORDER BY ingestion_time_utc DESC
    LIMIT 2;
    """
    df = pd.read_sql(query, engine)

    if len(df) < 2:
        raise ValueError("Not enough snapshots available.")

    return df["ingestion_time_utc"].tolist()


def get_airline_counts(engine, snapshot_time):
    query = f"""
    SELECT mapped_airline, COUNT(*) AS flight_count
    FROM flight_states
    WHERE ingestion_time_utc = '{snapshot_time}'
    GROUP BY mapped_airline
    """
    return pd.read_sql(query, engine)


def detect_anomalies(df):
    """
    Detect anomalies using simple statistical deviation.
    """
    mean = df["count_change"].mean()
    std = df["count_change"].std()

    df["z_score"] = (df["count_change"] - mean) / (std + 1e-6)

    # Mark anomalies
    df["anomaly_flag"] = df["z_score"].apply(
        lambda x: "SURGE" if x > 1.5 else ("DROP" if x < -1.5 else "NORMAL")
    )

    return df.sort_values(by="count_change", ascending=False)


def main():
    engine = get_engine()

    latest, previous = get_latest_two_snapshots(engine)

    latest_df = get_airline_counts(engine, latest)
    previous_df = get_airline_counts(engine, previous)

    merged = latest_df.merge(
        previous_df,
        on="mapped_airline",
        how="outer",
        suffixes=("_latest", "_previous")
    ).fillna(0)

    merged["count_change"] = merged["flight_count_latest"] - merged["flight_count_previous"]

    result = detect_anomalies(merged)

    print("\nAirline Anomaly Detection Report")
    print("-" * 50)
    print(f"Latest snapshot: {latest}")
    print(f"Previous snapshot: {previous}\n")

    print(result)


if __name__ == "__main__":
    main()