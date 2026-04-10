from pathlib import Path

import pandas as pd
import streamlit as st


st.set_page_config(
    page_title="Flight Operations Intelligence System",
    page_icon="✈️",
    layout="wide",
)

# st.markdown(
#     '<div class="section-header">✈️ Flight Operations Intelligence System</div>',
#     unsafe_allow_html=True,
# )

st.markdown(
    '<div class="subtle-text">This dashboard presents airline anomaly monitoring, route intelligence, model insights, and an analyst assistant built on top of real-time aviation data.</div>',
    unsafe_allow_html=True,
)

st.markdown(
    """
    <style>
    .main {
        padding-top: 1.2rem;
    }

    .block-container {
        padding-top: 1rem;
        padding-bottom: 2rem;
        padding-left: 2rem;
        padding-right: 2rem;
    }

    div[data-testid="stMetric"] {
        background-color: #f8f9fa;
        border: 1px solid #e6e6e6;
        padding: 14px 18px;
        border-radius: 12px;
    }

    div[data-testid="stMetricLabel"] {
        font-size: 15px;
        font-weight: 600;
    }

    div[data-testid="stMetricValue"] {
        font-size: 32px;
        font-weight: 700;
    }

    .section-header {
        font-size: 28px;
        font-weight: 700;
        margin-top: 10px;
        margin-bottom: 10px;
    }

    .subtle-text {
        color: #555;
        font-size: 15px;
        margin-bottom: 18px;
    }

    .insight-card {
        background-color: #f8f9fa;
        border: 1px solid #e6e6e6;
        padding: 16px;
        border-radius: 12px;
        margin-bottom: 14px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

PROCESSED_DIR = Path("data/processed")


def load_csv_if_exists(filename: str):
    path = PROCESSED_DIR / filename
    if path.exists():
        return pd.read_csv(path)
    return None


ml_df = load_csv_if_exists("airline_ml_features_v2.csv")
scored_df = load_csv_if_exists("airline_ml_features_v2_scored.csv")
fused_df = load_csv_if_exists("fused_intelligence.csv")
fi_df = load_csv_if_exists("random_forest_feature_importance.csv")


st.title("✈️ Flight Operations Intelligence System")
st.markdown(
    """
    This dashboard presents airline anomaly monitoring, route intelligence,
    model insights, and an analyst assistant built on top of real-time aviation data.
    """
)

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    [
        "Overview",
        "Anomaly Monitor",
        "Route Intelligence",
        "Model Insights",
        "Analyst Assistant",
    ]
)

with tab1:
    st.subheader("Overview")

    if ml_df is None:
        st.error("Missing file: airline_ml_features_v2.csv")
    else:
        total_rows = len(ml_df)
        anomaly_rows = int(ml_df["target_anomaly"].sum()) if "target_anomaly" in ml_df.columns else 0
        tracked_airlines = ml_df["mapped_airline"].nunique() if "mapped_airline" in ml_df.columns else 0

        tracked_hubs = 0
        if fused_df is not None and "departure_iata" in fused_df.columns:
            tracked_hubs = fused_df["departure_iata"].nunique()

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total ML Rows", total_rows)
        col2.metric("Anomaly Rows", anomaly_rows)
        col3.metric("Tracked Airlines", tracked_airlines)
        col4.metric("Tracked Hubs", tracked_hubs)

        st.markdown("---")

        left, right = st.columns(2)

        with left:
            st.markdown("### Top anomaly snapshots")
            if scored_df is not None and "predicted_probability" in scored_df.columns:
                top_anomalies = scored_df.sort_values(
                    "predicted_probability", ascending=False
                ).head(10)

                cols = [
                    "ingestion_time_utc",
                    "mapped_airline",
                    "live_flights",
                    "predicted_probability",
                ]
                existing_cols = [c for c in cols if c in top_anomalies.columns]
                st.dataframe(top_anomalies[existing_cols], use_container_width=True)
            else:
                st.info("Scored anomaly file not available.")

        with right:
            st.markdown("### Top route coverage")
            if fused_df is not None and "route_flights" in fused_df.columns:
                top_routes = fused_df.sort_values("route_flights", ascending=False).head(10)

                cols = [
                    "mapped_airline",
                    "departure_iata",
                    "route_flights",
                ]
                existing_cols = [c for c in cols if c in top_routes.columns]
                st.dataframe(top_routes[existing_cols], use_container_width=True)
            else:
                st.info("Fused intelligence file not available.")

        st.markdown("---")

        st.markdown("### Top model drivers")
        if fi_df is not None:
            st.dataframe(fi_df.head(10), use_container_width=True)
        else:
            st.info("Feature importance file not available.")

with tab2:
    st.subheader("Anomaly Monitor")

    if scored_df is None:
        st.error("Missing file: airline_ml_features_v2_scored.csv")
    else:
        working_df = scored_df.copy()

        if "predicted_probability" not in working_df.columns:
            st.error("Column 'predicted_probability' not found in scored anomaly file.")
        else:
            working_df["predicted_probability"] = pd.to_numeric(
                working_df["predicted_probability"], errors="coerce"
            )

            airline_options = ["All"] + sorted(
                working_df["mapped_airline"].dropna().unique().tolist()
            )

            col1, col2 = st.columns(2)

            with col1:
                selected_airline = st.selectbox(
                    "Filter by airline",
                    options=airline_options,
                    index=0,
                )

            with col2:
                min_prob = st.slider(
                    "Minimum anomaly probability",
                    min_value=0.0,
                    max_value=1.0,
                    value=0.80,
                    step=0.01,
                )

            filtered_df = working_df[working_df["predicted_probability"] >= min_prob]

            if selected_airline != "All":
                filtered_df = filtered_df[
                    filtered_df["mapped_airline"] == selected_airline
                ]

            filtered_df = filtered_df.sort_values(
                "predicted_probability", ascending=False
            )

            st.markdown("### High-risk anomaly rows")

            display_cols = [
                "ingestion_time_utc",
                "mapped_airline",
                "live_flights",
                "avg_velocity",
                "avg_altitude",
                "predicted_anomaly",
                "predicted_probability",
            ]
            existing_cols = [c for c in display_cols if c in filtered_df.columns]

            st.dataframe(filtered_df[existing_cols], use_container_width=True)

            st.markdown("### Top anomaly probabilities by airline")
            if not filtered_df.empty:
                chart_df = (
                    filtered_df.groupby("mapped_airline", as_index=False)["predicted_probability"]
                    .max()
                    .sort_values("predicted_probability", ascending=False)
                    .head(10)
                )
                st.bar_chart(chart_df.set_index("mapped_airline"))
            else:
                st.info("No anomaly rows match the current filter.")

with tab3:
    st.subheader("Route Intelligence")

    if fused_df is None:
        st.error("Missing file: fused_intelligence.csv")
    else:
        working_df = fused_df.copy()

        # Filters
        airline_options = ["All"] + sorted(
            working_df["mapped_airline"].dropna().unique().tolist()
        )
        hub_options = ["All"] + sorted(
            working_df["departure_iata"].dropna().unique().tolist()
        )

        col1, col2 = st.columns(2)

        with col1:
            selected_airline = st.selectbox(
                "Filter by airline",
                options=airline_options,
                index=0,
                key="route_airline",
            )

        with col2:
            selected_hub = st.selectbox(
                "Filter by departure hub",
                options=hub_options,
                index=0,
                key="route_hub",
            )

        filtered_df = working_df.copy()

        if selected_airline != "All":
            filtered_df = filtered_df[
                filtered_df["mapped_airline"] == selected_airline
            ]

        if selected_hub != "All":
            filtered_df = filtered_df[
                filtered_df["departure_iata"] == selected_hub
            ]

        filtered_df = filtered_df.sort_values(
            "route_flights", ascending=False
        )

        st.markdown("### Top routes")

        display_cols = [
            "mapped_airline",
            "departure_iata",
            "route_flights",
        ]
        existing_cols = [c for c in display_cols if c in filtered_df.columns]

        st.dataframe(filtered_df[existing_cols], use_container_width=True)

        st.markdown("### Top airlines by route coverage")

        if not filtered_df.empty:
            chart_df = (
                filtered_df.groupby("mapped_airline", as_index=False)["route_flights"]
                .sum()
                .sort_values("route_flights", ascending=False)
                .head(10)
            )

            st.bar_chart(chart_df.set_index("mapped_airline"))
        else:
            st.info("No route data matches the current filter.")


with tab4:
    st.subheader("Model Insights")

    if fi_df is None and ml_df is None:
        st.error("Missing both feature importance and ML feature table files.")
    else:
        left, right = st.columns(2)

        with left:
            st.markdown("### Feature importance table")
            if fi_df is not None:
                st.dataframe(fi_df.head(15), use_container_width=True)
            else:
                st.info("Feature importance file not available.")

        with right:
            st.markdown("### Top feature importance chart")
            if fi_df is not None and "feature" in fi_df.columns and "importance" in fi_df.columns:
                chart_df = fi_df.head(10).copy()
                chart_df = chart_df.sort_values("importance", ascending=True)
                st.bar_chart(chart_df.set_index("feature"))
            else:
                st.info("Feature importance data not available for chart.")

        st.markdown("---")

        st.markdown("### Target anomaly distribution")
        if ml_df is not None and "target_anomaly" in ml_df.columns:
            target_counts = (
                ml_df["target_anomaly"]
                .value_counts()
                .sort_index()
                .rename_axis("target_anomaly")
                .reset_index(name="count")
            )
            st.dataframe(target_counts, use_container_width=True)
            st.bar_chart(target_counts.set_index("target_anomaly"))
        else:
            st.info("ML target anomaly data not available.")

        st.markdown("---")

        st.markdown("### Model interpretation summary")
        summary_lines = []

        if fi_df is not None and not fi_df.empty:
            top_features = fi_df.head(5)["feature"].tolist()
            summary_lines.append(
                f"Top model drivers are: {', '.join(top_features)}."
            )

        if ml_df is not None and "target_anomaly" in ml_df.columns:
            total_rows = len(ml_df)
            anomaly_rows = int(ml_df["target_anomaly"].sum())
            anomaly_rate = anomaly_rows / total_rows if total_rows > 0 else 0
            summary_lines.append(
                f"Anomaly class proportion in the ML dataset is {anomaly_rate:.2%} ({anomaly_rows} out of {total_rows} rows)."
            )

        if summary_lines:
            for line in summary_lines:
                st.write(f"- {line}")
        else:
            st.info("Model summary could not be generated.")

with tab5:
    st.subheader("Analyst Assistant")

    kb_path = PROCESSED_DIR / "ops_knowledge_base.txt"

    if not kb_path.exists():
        st.error("Missing file: ops_knowledge_base.txt")
    else:
        with open(kb_path, "r", encoding="utf-8") as f:
            kb_text = f.read()

        sections = [section.strip() for section in kb_text.split("\n\n") if section.strip()]

        st.markdown("Ask a question about anomalies, route coverage, airlines, hubs, or model drivers.")

        user_query = st.text_input(
            "Enter your question",
            placeholder="Example: Which airlines have the highest anomaly probabilities?",
        )

        def simple_retrieve(query: str, text_sections, top_k: int = 3):
            query_words = set(query.lower().split())
            scored = []

            for section in text_sections:
                section_words = set(section.lower().split())
                overlap = len(query_words.intersection(section_words))
                scored.append((overlap, section))

            scored.sort(key=lambda x: x[0], reverse=True)
            return [section for score, section in scored[:top_k] if score > 0]

        if st.button("Get Answer"):
            if not user_query.strip():
                st.warning("Please enter a question.")
            else:
                matches = simple_retrieve(user_query, sections, top_k=3)

                st.markdown("### Retrieved evidence")
                if matches:
                    for i, section in enumerate(matches, start=1):
                        st.markdown(f"**Section {i}**")
                        st.code(section)
                else:
                    st.info("No strongly relevant section found.")

                st.markdown("### Analyst summary")
                if matches:
                    st.write(
                        "Based on the retrieved project knowledge, the most relevant operational evidence is shown above. "
                        "Use these sections to answer the question with current tracked airline, route, anomaly, and model information."
                    )
                else:
                    st.write(
                        "I could not find a strong match in the current knowledge base for this question."
                    )