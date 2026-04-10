import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import requests

from src.config import RAW_DATA_DIR


OPENSKY_STATES_URL = "https://opensky-network.org/api/states/all"


def fetch_opensky_states() -> Dict[str, Any]:
    """
    Fetch live aircraft states from the OpenSky API.
    """
    response = requests.get(OPENSKY_STATES_URL, timeout=30)
    response.raise_for_status()
    return response.json()


def save_raw_response(data: Dict[str, Any]) -> Path:
    """
    Save raw API response to data/raw as a timestamped JSON file.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_path = RAW_DATA_DIR / f"opensky_states_{timestamp}.json"

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    return file_path


def main() -> None:
    print("Fetching live flight states from OpenSky...")
    data = fetch_opensky_states()

    print(f"Top-level keys: {list(data.keys())}")
    print(f"Timestamp: {data.get('time')}")
    print(f"Number of state rows: {len(data.get('states') or [])}")

    saved_file = save_raw_response(data)
    print(f"Raw response saved to: {saved_file}")


if __name__ == "__main__":
    main()