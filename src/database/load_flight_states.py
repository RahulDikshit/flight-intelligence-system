from datetime import datetime, timezone

import pandas as pd

from src.config import PROCESSED_DATA_DIR
from src.database.connection import get_engine


def load_scoped_csv() -> pd.DataFrame:
    file_path = PROCESSED_DATA_DIR / "opensky_project_scope.csv"
    if not file_path.exists():
        raise FileNotFoundError(f"Scoped CSV not found: {file_path}")

    df = pd.read_csv(file_path)
    return df


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Add ingestion timestamp for this batch load
    ingestion_time_utc = datetime.now(timezone.utc).isoformat()
    df["ingestion_time_utc"] = ingestion_time_utc

    # Ensure all expected columns exist
    expected_columns = [
        "ingestion_time_utc",
        "icao24",
        "callsign",
        "origin_country",
        "time_position",
        "last_contact",
        "longitude",
        "latitude",
        "baro_altitude",
        "on_ground",
        "velocity",
        "true_track",
        "vertical_rate",
        "sensors",
        "geo_altitude",
        "squawk",
        "spi",
        "position_source",
        "mapped_airline",
    ]

    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    df = df[expected_columns]
    return df


def insert_into_database(df: pd.DataFrame) -> None:
    engine = get_engine()
    df.to_sql("flight_states", con=engine, if_exists="append", index=False)
    print(f"Inserted {len(df)} rows into 'flight_states' table.")


def main() -> None:
    df = load_scoped_csv()
    print(f"Loaded scoped CSV with shape: {df.shape}")

    prepared_df = prepare_dataframe(df)
    print(f"Prepared dataframe shape: {prepared_df.shape}")

    insert_into_database(prepared_df)


if __name__ == "__main__":
    main()