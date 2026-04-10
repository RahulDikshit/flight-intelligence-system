import time
from datetime import datetime

from src.ingestion.opensky_client import fetch_opensky_states, save_raw_response
from src.processing.parse_opensky_states import (
    load_raw_json,
    parse_states_to_dataframe,
    clean_dataframe,
)
from src.processing.filter_project_scope import filter_scope
from src.database.load_flight_states import prepare_dataframe, insert_into_database


def run_pipeline_once():
    print(f"\nRunning pipeline at {datetime.utcnow().isoformat()} UTC")

    # Step 1: Fetch live OpenSky data
    raw_data = fetch_opensky_states()

    # Step 2: Save raw JSON snapshot
    saved_file = save_raw_response(raw_data)
    print(f"Raw snapshot saved to: {saved_file}")

    # Step 3: Load raw JSON from saved file
    data = load_raw_json(saved_file)

    # Step 4: Parse into dataframe
    df = parse_states_to_dataframe(data)
    df = clean_dataframe(df)
    print(f"Parsed dataframe shape: {df.shape}")

    # Step 5: Filter to project scope
    scoped_df = filter_scope(df)
    print(f"Scoped dataframe shape: {scoped_df.shape}")

    # Step 6: Prepare for database insertion
    prepared_df = prepare_dataframe(scoped_df)
    print(f"Prepared dataframe shape: {prepared_df.shape}")

    # Step 7: Insert into database
    insert_into_database(prepared_df)

    print("Pipeline run complete.")


def main():
    while True:
        run_pipeline_once()
        print("Sleeping for 300 seconds...\n")
        time.sleep(300)


if __name__ == "__main__":
    main()