import pandas as pd

from src.database.connection import get_engine


def get_latest_snapshot_summary() -> pd.DataFrame:
    engine = get_engine()

    query = """
    SELECT
        ingestion_time_utc,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT mapped_airline) AS distinct_airlines,
        COUNT(DISTINCT callsign) AS distinct_callsigns,
        ROUND(AVG(velocity), 2) AS avg_velocity,
        ROUND(AVG(baro_altitude), 2) AS avg_baro_altitude
    FROM flight_states
    GROUP BY ingestion_time_utc
    ORDER BY ingestion_time_utc DESC
    LIMIT 5;
    """

    df = pd.read_sql(query, engine)
    return df


def main() -> None:
    df = get_latest_snapshot_summary()

    print("Latest snapshot summary:")
    print(df)


if __name__ == "__main__":
    main()