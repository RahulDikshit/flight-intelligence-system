import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests

from src.config import RAW_DATA_DIR, AVIATIONSTACK_API_KEY


AVIATIONSTACK_FLIGHTS_URL = "https://api.aviationstack.com/v1/flights"
TARGET_HUBS = [
    "DEL", "BOM", "DXB", "AUH", "LHR",
    "FRA", "CDG", "AMS", "JFK", "LAX",
    "ORD", "SIN", "HKG", "BKK", "SYD"
]


def fetch_flights_for_airport(dep_iata: str, flight_status: str = "active", limit: int = 100) -> Dict[str, Any]:
    if not AVIATIONSTACK_API_KEY:
        raise ValueError("AVIATIONSTACK_API_KEY is missing in .env")

    params = {
        "access_key": AVIATIONSTACK_API_KEY,
        "dep_iata": dep_iata,
        "flight_status": flight_status,
        "limit": limit,
    }

    response = requests.get(AVIATIONSTACK_FLIGHTS_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    if "error" in data:
        raise ValueError(f"Aviationstack API error for {dep_iata}: {data['error']}")

    return data


def fetch_multi_hub_flights() -> Dict[str, Any]:
    combined_rows: List[Dict[str, Any]] = []

    for hub in TARGET_HUBS:
        print(f"Fetching aviationstack active flights for departure airport: {hub}")
        data = fetch_flights_for_airport(dep_iata=hub)

        rows = data.get("data", [])
        print(f"{hub}: returned rows = {len(rows)}")

        for row in rows:
            row["source_hub"] = hub

        combined_rows.extend(rows)

    return {"data": combined_rows}


def save_raw_response(data: Dict[str, Any]) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    file_path = RAW_DATA_DIR / f"aviationstack_flights_multihub_{timestamp}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    return file_path


def main() -> None:
    data = fetch_multi_hub_flights()

    print(f"Top-level keys: {list(data.keys())}")
    print(f"Returned rows: {len(data.get('data', []))}")

    saved_file = save_raw_response(data)
    print(f"Raw aviationstack response saved to: {saved_file}")


if __name__ == "__main__":
    main()