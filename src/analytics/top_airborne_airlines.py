import pandas as pd

from src.database.connection import get_engine


def get_top_airborne_airlines() -> pd.DataFrame:
    engine = get_engine()

    query = """
    SELECT
        mapped_airline,
        COUNT(*) AS airborne_flights
    FROM flight_states
    WHERE on_ground = 0
    GROUP BY mapped_airline
    ORDER BY airborne_flights DESC
    LIMIT 10;
    """

    df = pd.read_sql(query, engine)
    return df


def main() -> None:
    df = get_top_airborne_airlines()

    print("Top airborne airlines:")
    print(df)


if __name__ == "__main__":
    main()