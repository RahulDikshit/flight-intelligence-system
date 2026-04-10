from src.config import (
    BASE_DIR,
    DATA_DIR,
    RAW_DATA_DIR,
    PROCESSED_DATA_DIR,
    LOGS_DIR,
    DB_PATH,
)

def main() -> None:
    print("Flight Operations Intelligence System")
    print(f"Base directory: {BASE_DIR}")
    print(f"Data directory: {DATA_DIR}")
    print(f"Raw data directory: {RAW_DATA_DIR}")
    print(f"Processed data directory: {PROCESSED_DATA_DIR}")
    print(f"Logs directory: {LOGS_DIR}")
    print(f"Database path: {DB_PATH}")

if __name__ == "__main__":
    main()