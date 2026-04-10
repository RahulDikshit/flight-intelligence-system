from sqlalchemy import text
from src.database.connection import get_engine


def main() -> None:
    engine = get_engine()

    with engine.begin() as connection:
        connection.execute(text("DROP TABLE IF EXISTS aviationstack_flights;"))

    print("Dropped existing 'aviationstack_flights' table successfully.")


if __name__ == "__main__":
    main()