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
        raise ValueError("Not enough snapshots available for comparison.")

    return df["ingestion_time_utc"].tolist()


def get_snapshot_counts(engine, snapshot_time: str) -> pd.DataFrame:
    query = f"""
    SELECT
        mapped_airline,
        COUNT(*) AS flight_count
    FROM flight_states
    WHERE ingestion_time_utc = '{snapshot_time}'
    GROUP BY mapped_airline
    """
    return pd.read_sql(query, engine)


def main() -> None:
    engine = get_engine()

    latest_snapshot, previous_snapshot = get_latest_two_snapshots(engine)

    latest_df = get_snapshot_counts(engine, latest_snapshot)
    previous_df = get_snapshot_counts(engine, previous_snapshot)

    merged = latest_df.merge(
        previous_df,
        on="mapped_airline",
        how="outer",
        suffixes=("_latest", "_previous")
    ).fillna(0)

    merged["count_change"] = merged["flight_count_latest"] - merged["flight_count_previous"]
    merged = merged.sort_values(by="count_change", ascending=False)

    print(f"Latest snapshot: {latest_snapshot}")
    print(f"Previous snapshot: {previous_snapshot}")
    print("\nLatest vs previous airline comparison:")
    print(merged)


if __name__ == "__main__":
    main()