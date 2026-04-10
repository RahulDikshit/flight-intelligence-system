from pathlib import Path
from typing import Optional

import pandas as pd

from src.config import PROCESSED_DATA_DIR, AIRLINE_CALLSIGN_PATTERNS


def load_processed_data() -> pd.DataFrame:
    file_path = PROCESSED_DATA_DIR / "opensky_latest_processed.csv"
    if not file_path.exists():
        raise FileNotFoundError(f"Processed file not found: {file_path}")
    return pd.read_csv(file_path)


def map_airline_from_callsign(callsign: Optional[str]) -> Optional[str]:
    if pd.isna(callsign):
        return None

    callsign = str(callsign).strip().upper()

    for airline, patterns in AIRLINE_CALLSIGN_PATTERNS.items():
        for pattern in patterns:
            if callsign.startswith(pattern):
                return airline

    return None


def filter_scope(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Clean callsign
    df["callsign"] = df["callsign"].astype(str).str.strip()

    # Keep only rows with valid callsigns and coordinates
    df = df[df["callsign"].notna()]
    df = df[df["callsign"] != ""]
    df = df[df["longitude"].notna()]
    df = df[df["latitude"].notna()]

    # Map airline names from callsign patterns
    df["mapped_airline"] = df["callsign"].apply(map_airline_from_callsign)

    # Keep only rows that match our target airlines
    df = df[df["mapped_airline"].notna()]

    return df


def save_scoped_data(df: pd.DataFrame) -> Path:
    output_file = PROCESSED_DATA_DIR / "opensky_project_scope.csv"
    df.to_csv(output_file, index=False)
    return output_file


def main() -> None:
    df = load_processed_data()
    print(f"Original dataframe shape: {df.shape}")

    scoped_df = filter_scope(df)
    print(f"Scoped dataframe shape: {scoped_df.shape}")

    print("\nMapped airlines count:")
    print(scoped_df["mapped_airline"].value_counts().head(20))

    print("\nSample scoped rows:")
    print(scoped_df[["callsign", "origin_country", "mapped_airline", "longitude", "latitude"]].head(10))

    output_file = save_scoped_data(scoped_df)
    print(f"\nScoped CSV saved to: {output_file}")


if __name__ == "__main__":
    main()