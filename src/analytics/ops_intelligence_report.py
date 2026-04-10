import pandas as pd

from src.database.connection import get_engine


def get_ops_report_data():
    engine = get_engine()

    query_total = """
    SELECT COUNT(*) AS total_rows
    FROM flight_states;
    """

    query_airlines = """
    SELECT COUNT(DISTINCT mapped_airline) AS total_airlines
    FROM flight_states;
    """

    query_top_airline = """
    SELECT mapped_airline, COUNT(*) AS flight_count
    FROM flight_states
    GROUP BY mapped_airline
    ORDER BY flight_count DESC
    LIMIT 1;
    """

    query_fastest_airline = """
    SELECT mapped_airline, ROUND(AVG(velocity), 2) AS avg_velocity
    FROM flight_states
    WHERE velocity IS NOT NULL
    GROUP BY mapped_airline
    ORDER BY avg_velocity DESC
    LIMIT 1;
    """

    query_highest_airline = """
    SELECT mapped_airline, ROUND(AVG(baro_altitude), 2) AS avg_baro_altitude
    FROM flight_states
    WHERE baro_altitude IS NOT NULL
    GROUP BY mapped_airline
    ORDER BY avg_baro_altitude DESC
    LIMIT 1;
    """

    total_rows = pd.read_sql(query_total, engine)
    total_airlines = pd.read_sql(query_airlines, engine)
    top_airline = pd.read_sql(query_top_airline, engine)
    fastest_airline = pd.read_sql(query_fastest_airline, engine)
    highest_airline = pd.read_sql(query_highest_airline, engine)

    return total_rows, total_airlines, top_airline, fastest_airline, highest_airline


def main() -> None:
    total_rows, total_airlines, top_airline, fastest_airline, highest_airline = get_ops_report_data()

    print("Flight Operations Intelligence Report")
    print("-" * 40)
    print(f"Total monitored flight states: {int(total_rows.loc[0, 'total_rows'])}")
    print(f"Total monitored airlines: {int(total_airlines.loc[0, 'total_airlines'])}")
    print()
    print(
        f"Most active airline: {top_airline.loc[0, 'mapped_airline']} "
        f"({int(top_airline.loc[0, 'flight_count'])} flight states)"
    )
    print(
        f"Fastest airline by average velocity: {fastest_airline.loc[0, 'mapped_airline']} "
        f"({fastest_airline.loc[0, 'avg_velocity']} m/s)"
    )
    print(
        f"Highest airline by average barometric altitude: {highest_airline.loc[0, 'mapped_airline']} "
        f"({highest_airline.loc[0, 'avg_baro_altitude']} m)"
    )


if __name__ == "__main__":
    main()