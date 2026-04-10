import pandas as pd

from src.config import PROCESSED_DATA_DIR
from src.processing.flight_key_utils import split_callsign


def load_opensky_scope() -> pd.DataFrame:
    file_path = PROCESSED_DATA_DIR / "opensky_project_scope.csv"
    if not file_path.exists():
        raise FileNotFoundError(f"OpenSky scoped CSV not found: {file_path}")
    return pd.read_csv(file_path)


def load_aviationstack_processed() -> pd.DataFrame:
    file_path = PROCESSED_DATA_DIR / "aviationstack_latest_processed.csv"
    if not file_path.exists():
        raise FileNotFoundError(f"Aviationstack processed CSV not found: {file_path}")
    return pd.read_csv(file_path)


def enrich_opensky_keys(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["callsign"] = df["callsign"].astype(str).str.strip().str.upper()
    extracted = df["callsign"].apply(split_callsign)
    df["callsign_prefix"] = extracted.apply(lambda x: x[0])
    df["callsign_number"] = extracted.apply(lambda x: x[1])

    # Best direct join key
    df["callsign_clean"] = df["callsign"]

    return df


def enrich_aviationstack_keys(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["flight_number"] = df["flight_number"].astype(str).str.strip()
    df["airline_icao"] = df["airline_icao"].astype(str).str.strip().str.upper()
    df["flight_icao"] = df["flight_icao"].astype(str).str.strip().str.upper()

    # Direct comparable key
    df["flight_icao_clean"] = df["flight_icao"]

    return df


def fuse_data(opensky_df: pd.DataFrame, aviation_df: pd.DataFrame) -> pd.DataFrame:
    # Primary match: full callsign <-> full ICAO flight code
    merged = opensky_df.merge(
        aviation_df,
        left_on="callsign_clean",
        right_on="flight_icao_clean",
        how="left",
        suffixes=("_opensky", "_aviation")
    )
    return merged


def save_fused_dataframe(df: pd.DataFrame) -> str:
    output_file = PROCESSED_DATA_DIR / "flight_enriched_latest.csv"
    df.to_csv(output_file, index=False)
    return str(output_file)


def main() -> None:
    opensky_df = load_opensky_scope()
    aviation_df = load_aviationstack_processed()

    opensky_df = enrich_opensky_keys(opensky_df)
    aviation_df = enrich_aviationstack_keys(aviation_df)

    fused_df = fuse_data(opensky_df, aviation_df)

    print(f"OpenSky scoped shape: {opensky_df.shape}")
    print(f"Aviationstack processed shape: {aviation_df.shape}")
    print(f"Fused dataframe shape: {fused_df.shape}")

    matched_rows = fused_df["flight_icao"].notna().sum()
    print(f"Matched enriched rows: {matched_rows}")

    print("\nSample fused rows:")
    sample_cols = [
        "callsign",
        "mapped_airline",
        "callsign_clean",
        "airline_name",
        "flight_icao",
        "departure_iata",
        "arrival_iata",
        "flight_status",
        "velocity",
        "baro_altitude",
    ]
    existing_cols = [c for c in sample_cols if c in fused_df.columns]
    print(fused_df[existing_cols].head(15))

    output_file = save_fused_dataframe(fused_df)
    print(f"\nFused CSV saved to: {output_file}")


if __name__ == "__main__":
    main()