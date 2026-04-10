from datetime import datetime, timezone

import pandas as pd

from src.config import PROCESSED_DATA_DIR
from src.database.connection import get_engine


def load_processed_csv() -> pd.DataFrame:
    file_path = PROCESSED_DATA_DIR / "aviationstack_latest_processed.csv"
    if not file_path.exists():
        raise FileNotFoundError(f"Processed CSV not found: {file_path}")
    return pd.read_csv(file_path)


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ingestion_time_utc"] = datetime.now(timezone.utc).isoformat()
    return df


def insert_into_database(df: pd.DataFrame) -> None:
    engine = get_engine()
    df.to_sql("aviationstack_flights", con=engine, if_exists="append", index=False)
    print(f"Inserted {len(df)} rows into 'aviationstack_flights' table.")


def main() -> None:
    df = load_processed_csv()
    print(f"Loaded aviationstack CSV with shape: {df.shape}")

    prepared_df = prepare_dataframe(df)
    print(f"Prepared dataframe shape: {prepared_df.shape}")

    insert_into_database(prepared_df)


if __name__ == "__main__":
    main()