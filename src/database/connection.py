from sqlalchemy import create_engine
from src.config import DB_FILE

def get_engine():
    connection_string = f"sqlite:///{DB_FILE}"
    engine = create_engine(connection_string, echo=False)
    return engine