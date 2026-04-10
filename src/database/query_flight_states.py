import pandas as pd

from src.database.connection import get_engine


def main() -> None:
    engine = get_engine()

    query = """
    SELECT
        mapped_airline,
        COUNT(*) AS flight_count
    FROM flight_states
    GROUP BY mapped_airline
    ORDER BY flight_count DESC
    LIMIT 20;
    """

    df = pd.read_sql(query, engine)

    print("Flight counts by mapped airline:")
    print(df)


if __name__ == "__main__":
    main()