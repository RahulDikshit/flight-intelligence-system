from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
LOGS_DIR = BASE_DIR / "logs"

DB_PATH = os.getenv("DB_PATH", "flight_ops.db")
DB_FILE = BASE_DIR / DB_PATH

AVIATIONSTACK_API_KEY = os.getenv("AVIATIONSTACK_API_KEY", "")
OPENSKY_USERNAME = os.getenv("OPENSKY_USERNAME", "")
OPENSKY_PASSWORD = os.getenv("OPENSKY_PASSWORD", "")


TARGET_AIRPORTS = [
    "DEL", "BOM", "DXB", "AUH", "LHR",
    "FRA", "CDG", "AMS", "JFK", "LAX",
    "ORD", "SIN", "HKG", "BKK", "SYD"
]

TARGET_AIRLINES = [
    "Air India",
    "IndiGo",
    "SpiceJet",
    "Emirates",
    "Etihad Airways",
    "Qatar Airways",
    "Lufthansa",
    "British Airways",
    "Air France",
    "KLM",
    "Delta Air Lines",
    "United Airlines",
    "American Airlines",
    "Singapore Airlines",
    "Cathay Pacific",
    "Thai Airways",
    "Qantas",
    "Turkish Airlines",
    "ANA",
    "Japan Airlines"
]

AIRLINE_CALLSIGN_PATTERNS = {
    "Air India": ["AIC", "AI"],
    "IndiGo": ["IGO", "6E"],
    "SpiceJet": ["SEJ", "SG"],
    "Emirates": ["UAE", "EK"],
    "Etihad Airways": ["ETD", "EY"],
    "Qatar Airways": ["QTR", "QR"],
    "Lufthansa": ["DLH", "LH"],
    "British Airways": ["BAW", "BA"],
    "Air France": ["AFR", "AF"],
    "KLM": ["KLM", "KL"],
    "Delta Air Lines": ["DAL", "DL"],
    "United Airlines": ["UAL", "UA"],
    "American Airlines": ["AAL", "AA"],
    "Singapore Airlines": ["SIA", "SQ"],
    "Cathay Pacific": ["CPA", "CX"],
    "Thai Airways": ["THA", "TG"],
    "Qantas": ["QFA", "QF"],
    "Turkish Airlines": ["THY", "TK"],
    "ANA": ["ANA", "NH"],
    "Japan Airlines": ["JAL", "JL"],
}