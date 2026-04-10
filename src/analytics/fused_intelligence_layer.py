import pandas as pd

from src.config import PROCESSED_DATA_DIR


def load_opensky():
    path = PROCESSED_DATA_DIR / "opensky_project_scope.csv"
    return pd.read_csv(path)


def load_aviationstack():
    path = PROCESSED_DATA_DIR / "aviationstack_latest_processed.csv"
    return pd.read_csv(path)


def opensky_airline_metrics(df):
    return (
        df.groupby("mapped_airline")
        .agg(
            live_flights=("icao24", "count"),
            avg_velocity=("velocity", "mean"),
            avg_altitude=("baro_altitude", "mean"),
        )
        .reset_index()
    )


def aviationstack_airline_routes(df):
    return (
        df.groupby(["airline_name", "departure_iata"])
        .agg(route_flights=("flight_number", "count"))
        .reset_index()
    )


def fuse_intelligence(opensky_df, aviation_df):
    opensky_metrics = opensky_airline_metrics(opensky_df)
    aviation_metrics = aviationstack_airline_routes(aviation_df)

    fused = opensky_metrics.merge(
        aviation_metrics,
        left_on="mapped_airline",
        right_on="airline_name",
        how="left"
    )

    return fused


def main():
    opensky_df = load_opensky()
    aviation_df = load_aviationstack()

    fused_df = fuse_intelligence(opensky_df, aviation_df)

    print("\nFused Intelligence Layer:")
    print(fused_df.head(20))

    output_file = PROCESSED_DATA_DIR / "fused_intelligence.csv"
    fused_df.to_csv(output_file, index=False)

    print(f"\nSaved to: {output_file}")


if __name__ == "__main__":
    main()