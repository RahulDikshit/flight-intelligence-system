import json
from pathlib import Path
from typing import List

import pandas as pd

from src.config import RAW_DATA_DIR, PROCESSED_DATA_DIR


def get_latest_aviationstack_file() -> Path:
    files = sorted(RAW_DATA_DIR.glob("aviationstack_flights_*.json"))
    if not files:
        raise FileNotFoundError("No aviationstack raw JSON files found in data/raw.")
    return files[-1]


def load_raw_json(file_path: Path) -> dict:
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_flights_to_dataframe(data: dict) -> pd.DataFrame:
    rows = data.get("data", [])
    parsed_rows = []

    for row in rows:
        parsed_rows.append({
            "source_hub": row.get("source_hub"),
            "flight_date": row.get("flight_date"),
            "flight_status": row.get("flight_status"),
            "departure_airport": (row.get("departure") or {}).get("airport"),
            "departure_iata": (row.get("departure") or {}).get("iata"),
            "departure_icao": (row.get("departure") or {}).get("icao"),
            "departure_delay": (row.get("departure") or {}).get("delay"),
            "departure_scheduled": (row.get("departure") or {}).get("scheduled"),
            "departure_actual": (row.get("departure") or {}).get("actual"),
            "arrival_airport": (row.get("arrival") or {}).get("airport"),
            "arrival_iata": (row.get("arrival") or {}).get("iata"),
            "arrival_icao": (row.get("arrival") or {}).get("icao"),
            "arrival_delay": (row.get("arrival") or {}).get("delay"),
            "arrival_scheduled": (row.get("arrival") or {}).get("scheduled"),
            "arrival_actual": (row.get("arrival") or {}).get("actual"),
            "airline_name": (row.get("airline") or {}).get("name"),
            "airline_iata": (row.get("airline") or {}).get("iata"),
            "airline_icao": (row.get("airline") or {}).get("icao"),
            "flight_number": (row.get("flight") or {}).get("number"),
            "flight_iata": (row.get("flight") or {}).get("iata"),
            "flight_icao": (row.get("flight") or {}).get("icao"),
        })

    return pd.DataFrame(parsed_rows)


def save_processed_dataframe(df: pd.DataFrame) -> Path:
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_file = PROCESSED_DATA_DIR / "aviationstack_latest_processed.csv"
    df.to_csv(output_file, index=False)
    return output_file


def main() -> None:
    latest_file = get_latest_aviationstack_file()
    print(f"Latest aviationstack raw file found: {latest_file}")

    raw_data = load_raw_json(latest_file)
    df = parse_flights_to_dataframe(raw_data)

    print(f"Processed dataframe shape: {df.shape}")
    print("Columns:")
    print(df.columns.tolist())
    print("\nSample rows:")
    print(df.head(5))

    output_file = save_processed_dataframe(df)
    print(f"\nProcessed aviationstack CSV saved to: {output_file}")


if __name__ == "__main__":
    main()