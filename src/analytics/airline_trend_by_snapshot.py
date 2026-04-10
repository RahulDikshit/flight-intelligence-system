import pandas as pd

from src.database.connection import get_engine


def get_airline_trend_by_snapshot() -> pd.DataFrame:
    engine = get_engine()

    query = """
    SELECT
        ingestion_time_utc,
        mapped_airline,
        COUNT(*) AS flight_count
    FROM flight_states
    GROUP BY ingestion_time_utc, mapped_airline
    ORDER BY ingestion_time_utc ASC, flight_count DESC;
    """

    df = pd.read_sql(query, engine)
    return df


def main() -> None:
    df = get_airline_trend_by_snapshot()

    print("Airline trend by snapshot:")
    print(df.head(50))


if __name__ == "__main__":
    main()