import pandas as pd

from src.database.connection import get_engine


def main() -> None:
    engine = get_engine()

    query = """
    SELECT
        airline_name,
        departure_iata,
        arrival_iata,
        flight_status,
        COUNT(*) AS flight_count
    FROM aviationstack_flights
    GROUP BY airline_name, departure_iata, arrival_iata, flight_status
    ORDER BY flight_count DESC
    LIMIT 20;
    """

    df = pd.read_sql(query, engine)

    print("Aviationstack flight route/status summary:")
    print(df)


if __name__ == "__main__":
    main()