import pandas as pd

from src.database.connection import get_engine


def get_airline_activity_metrics() -> pd.DataFrame:
    engine = get_engine()

    query = """
    SELECT
        mapped_airline,
        COUNT(*) AS total_flight_states,
        SUM(CASE WHEN on_ground = 1 THEN 1 ELSE 0 END) AS on_ground_count,
        SUM(CASE WHEN on_ground = 0 THEN 1 ELSE 0 END) AS airborne_count,
        ROUND(AVG(velocity), 2) AS avg_velocity,
        ROUND(AVG(baro_altitude), 2) AS avg_baro_altitude,
        ROUND(AVG(geo_altitude), 2) AS avg_geo_altitude
    FROM flight_states
    GROUP BY mapped_airline
    ORDER BY total_flight_states DESC;
    """

    df = pd.read_sql(query, engine)
    return df


def main() -> None:
    df = get_airline_activity_metrics()

    print("Airline activity metrics:")
    print(df)


if __name__ == "__main__":
    main()