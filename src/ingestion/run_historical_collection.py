import time
from datetime import datetime, timezone
from pathlib import Path

from src.ingestion.opensky_client import fetch_opensky_states, save_raw_response
from src.processing.parse_opensky_states import (
    load_raw_json,
    parse_states_to_dataframe,
    clean_dataframe,
)
from src.processing.filter_project_scope import (
    filter_scope,
)
from src.database.load_flight_states import prepare_dataframe, insert_into_database


SLEEP_SECONDS = 300   # temporary test value
NUM_ITERATIONS = 12   # temporary test value


def run_opensky_pipeline_once():
    print(f"\nRunning OpenSky collection at {datetime.now(timezone.utc).isoformat()}")

    raw_data = fetch_opensky_states()
    raw_path = save_raw_response(raw_data)
    print(f"Saved raw OpenSky snapshot to: {raw_path}")

    raw_json = load_raw_json(raw_path)
    parsed_df = parse_states_to_dataframe(raw_json)
    parsed_df = clean_dataframe(parsed_df)
    print(f"Parsed dataframe shape: {parsed_df.shape}")

    scoped_df = filter_scope(parsed_df)
    print(f"Scoped dataframe shape: {scoped_df.shape}")

    prepared_df = prepare_dataframe(scoped_df)
    insert_into_database(prepared_df)

    print("Inserted processed snapshot into SQLite.")

    try:
        Path(raw_path).unlink(missing_ok=True)
        print(f"Deleted raw file: {raw_path}")
    except Exception as e:
        print(f"Warning: could not delete raw file {raw_path}: {e}")

    print("OpenSky pipeline run complete.")


def main():
    for i in range(NUM_ITERATIONS):
        print(f"\n========== Iteration {i + 1}/{NUM_ITERATIONS} ==========")
        run_opensky_pipeline_once()

        if i < NUM_ITERATIONS - 1:
            print(f"Sleeping for {SLEEP_SECONDS} seconds...")
            time.sleep(SLEEP_SECONDS)

    print("\nHistorical collection finished.")


if __name__ == "__main__":
    main()