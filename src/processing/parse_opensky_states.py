import json
from pathlib import Path
from typing import List

import pandas as pd

from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR


OPENSKY_COLUMNS: List[str] = [
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
    "category",
]


def get_latest_raw_file() -> Path:
    files = sorted(RAW_DATA_DIR.glob("opensky_states_*.json"))
    if not files:
        raise FileNotFoundError("No OpenSky raw JSON files found in data/raw.")
    return files[-1]


def load_raw_json(file_path: Path) -> dict:
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_states_to_dataframe(data: dict) -> pd.DataFrame:
    states = data.get("states", [])
    df = pd.DataFrame(states, columns=OPENSKY_COLUMNS[: len(states[0])] if states else OPENSKY_COLUMNS)
    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # Clean callsign whitespace
    if "callsign" in df.columns:
        df["callsign"] = df["callsign"].astype(str).str.strip()

    # Add ingestion timestamp from file processing time if needed later
    return df


def save_processed_dataframe(df: pd.DataFrame) -> Path:
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_file = PROCESSED_DATA_DIR / "opensky_latest_processed.csv"
    df.to_csv(output_file, index=False)
    return output_file


def main() -> None:
    latest_file = get_latest_raw_file()
    print(f"Latest raw file found: {latest_file}")

    raw_data = load_raw_json(latest_file)
    df = parse_states_to_dataframe(raw_data)
    df = clean_dataframe(df)

    print(f"Processed dataframe shape: {df.shape}")
    print("Columns:")
    print(df.columns.tolist())
    print("\nSample rows:")
    print(df.head(5))

    output_file = save_processed_dataframe(df)
    print(f"\nProcessed CSV saved to: {output_file}")


if __name__ == "__main__":
    main()