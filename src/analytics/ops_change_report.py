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


def get_total_rows(engine, snapshot_time: str) -> int:
    query = f"""
    SELECT COUNT(*) AS total_rows
    FROM flight_states
    WHERE ingestion_time_utc = '{snapshot_time}'
    """
    df = pd.read_sql(query, engine)
    return int(df.loc[0, "total_rows"])


def get_top_airline(engine, snapshot_time: str) -> pd.DataFrame:
    query = f"""
    SELECT mapped_airline, COUNT(*) AS flight_count
    FROM flight_states
    WHERE ingestion_time_utc = '{snapshot_time}'
    GROUP BY mapped_airline
    ORDER BY flight_count DESC
    LIMIT 1;
    """
    return pd.read_sql(query, engine)


def main() -> None:
    engine = get_engine()

    latest_snapshot, previous_snapshot = get_latest_two_snapshots(engine)

    latest_total = get_total_rows(engine, latest_snapshot)
    previous_total = get_total_rows(engine, previous_snapshot)

    latest_top = get_top_airline(engine, latest_snapshot)
    previous_top = get_top_airline(engine, previous_snapshot)

    print("Flight Operations Change Report")
    print("-" * 45)
    print(f"Latest snapshot:   {latest_snapshot}")
    print(f"Previous snapshot: {previous_snapshot}")
    print()
    print(f"Latest total monitored states:   {latest_total}")
    print(f"Previous total monitored states: {previous_total}")
    print(f"Net change: {latest_total - previous_total}")
    print()
    print(
        f"Latest most active airline: {latest_top.loc[0, 'mapped_airline']} "
        f"({int(latest_top.loc[0, 'flight_count'])})"
    )
    print(
        f"Previous most active airline: {previous_top.loc[0, 'mapped_airline']} "
        f"({int(previous_top.loc[0, 'flight_count'])})"
    )


if __name__ == "__main__":
    main()