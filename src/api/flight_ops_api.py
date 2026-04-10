from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from src.config import PROCESSED_DATA_DIR
from src.app.flight_ops_analyst_assistant import (
    load_knowledge_base,
    split_into_sections,
    retrieve_relevant_sections,
    build_analyst_summary,
)

app = FastAPI(
    title="Flight Operations Intelligence API",
    description="API for anomaly detection, route intelligence, model insights, and analyst assistant",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===============================
# Paths
# ===============================

ML_PATH = PROCESSED_DATA_DIR / "airline_ml_features_v2.csv"
ANOMALY_PATH = PROCESSED_DATA_DIR / "airline_ml_features_scored_v2.csv"
FUSED_PATH = PROCESSED_DATA_DIR / "fused_intelligence.csv"
FI_PATH = PROCESSED_DATA_DIR / "random_forest_feature_importance.csv"

# ===============================
# Helpers
# ===============================

def load_df(path: Path) -> Optional[pd.DataFrame]:
    if path.exists():
        return pd.read_csv(path)
    return None

# ===============================
# Root / Health
# ===============================

@app.get("/")
def root():
    return {
        "message": "Flight Operations Intelligence API is running.",
        "docs": "/docs",
        "version": "1.0.0",
        "available_endpoints": [
            "/health",
            "/overview",
            "/anomalies",
            "/routes",
            "/model-insights",
            "/ask",
        ],
    }

@app.get("/health")
def health():
    return {"status": "ok"}

# ===============================
# Overview
# ===============================

@app.get("/overview")
def overview():
    df = load_df(ML_PATH)

    if df is None:
        return {"error": "ML dataset not found"}

    total_rows = len(df)
    anomaly_rows = int(df["target_anomaly"].sum())

    return {
        "total_rows": total_rows,
        "anomaly_rows": anomaly_rows,
        "anomaly_rate": anomaly_rows / total_rows if total_rows > 0 else 0,
    }

# ===============================
# Anomaly Monitor
# ===============================

@app.get("/anomalies")
def get_anomalies(
    airline: str = Query(default=None),
    min_prob: float = Query(default=0.8),
):
    df = load_df(ANOMALY_PATH)

    if df is None:
        return {"error": "Scored anomaly file not found"}

    df = df[df["predicted_probability"] >= min_prob]

    if airline and airline != "All":
        df = df[df["mapped_airline"] == airline]

    return df.head(50).to_dict(orient="records")

# ===============================
# Route Intelligence
# ===============================

@app.get("/routes")
def get_routes(
    airline: str = Query(default=None),
    hub: str = Query(default=None),
):
    df = load_df(FUSED_PATH)

    if df is None:
        return {"error": "Fused intelligence file not found"}

    if airline and airline != "All":
        df = df[df["mapped_airline"] == airline]

    if hub and hub != "All":
        df = df[df["departure_iata"] == hub]

    return df.head(50).to_dict(orient="records")

# ===============================
# Model Insights
# ===============================

@app.get("/model-insights")
def model_insights():
    fi_df = load_df(FI_PATH)
    ml_df = load_df(ML_PATH)

    if fi_df is None or ml_df is None:
        return {"error": "Missing model data files"}

    return {
        "top_features": fi_df.head(10).to_dict(orient="records"),
        "target_distribution": ml_df["target_anomaly"].value_counts().to_dict(),
    }

# ===============================
# Analyst Assistant
# ===============================

@app.get("/ask")
def ask_question(q: str):
    kb_text = load_knowledge_base()
    sections = split_into_sections(kb_text)
    retrieved = retrieve_relevant_sections(q, sections, top_k=3)

    return {
        "question": q,
        "retrieved_sections": [
            {"section": s, "score": score}
            for _, s, score in retrieved
        ],
        "summary": build_analyst_summary(q, retrieved),
    }