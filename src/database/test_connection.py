import pandas as pd
from src.database.connection import get_engine

def test_db_connection() -> None:
    engine = get_engine()
    query = "SELECT 1 AS test_value"
    df = pd.read_sql(query, engine)
    print(df)

if __name__ == "__main__":
    test_db_connection()