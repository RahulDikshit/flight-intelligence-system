from sqlalchemy import text
from src.database.connection import get_engine


def initialize_database() -> None:
    engine = get_engine()

    create_flight_states_table = """
    CREATE TABLE IF NOT EXISTS flight_states (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ingestion_time_utc TEXT,
        icao24 TEXT,
        callsign TEXT,
        origin_country TEXT,
        time_position REAL,
        last_contact REAL,
        longitude REAL,
        latitude REAL,
        baro_altitude REAL,
        on_ground BOOLEAN,
        velocity REAL,
        true_track REAL,
        vertical_rate REAL,
        sensors TEXT,
        geo_altitude REAL,
        squawk TEXT,
        spi BOOLEAN,
        position_source INTEGER,
        mapped_airline TEXT
    );
    """

    create_aviationstack_flights_table = """
    CREATE TABLE IF NOT EXISTS aviationstack_flights (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_hub TEXT,
        flight_date TEXT,
        flight_status TEXT,
        departure_airport TEXT,
        departure_iata TEXT,
        departure_icao TEXT,
        departure_delay REAL,
        departure_scheduled TEXT,
        departure_actual TEXT,
        arrival_airport TEXT,
        arrival_iata TEXT,
        arrival_icao TEXT,
        arrival_delay REAL,
        arrival_scheduled TEXT,
        arrival_actual TEXT,
        airline_name TEXT,
        airline_iata TEXT,
        airline_icao TEXT,
        flight_number TEXT,
        flight_iata TEXT,
        flight_icao TEXT,
        ingestion_time_utc TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """

    create_flight_enriched_table = """
    CREATE TABLE IF NOT EXISTS flight_enriched (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ingestion_time_utc TEXT,
        icao24 TEXT,
        callsign TEXT,
        mapped_airline TEXT,
        origin_country TEXT,
        longitude REAL,
        latitude REAL,
        velocity REAL,
        baro_altitude REAL,
        on_ground BOOLEAN,
        callsign_prefix TEXT,
        callsign_number TEXT,
        airline_name TEXT,
        departure_iata TEXT,
        arrival_iata TEXT,
        flight_status TEXT,
        flight_number TEXT,
        flight_iata TEXT,
        flight_icao TEXT
    );
    """

    with engine.begin() as connection:
        connection.execute(text(create_flight_states_table))
        connection.execute(text(create_aviationstack_flights_table))
        connection.execute(text(create_flight_enriched_table))

    print("Database initialized successfully. Required tables are ready.")


if __name__ == "__main__":
    initialize_database()