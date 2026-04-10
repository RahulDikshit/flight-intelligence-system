from pathlib import Path
from typing import List, Tuple
import re

from src.config import PROCESSED_DATA_DIR


KNOWLEDGE_BASE_PATH = PROCESSED_DATA_DIR / "ops_knowledge_base.txt"


STOPWORDS = {
    "the", "is", "are", "a", "an", "of", "to", "for", "and", "or", "in", "on",
    "from", "with", "which", "what", "have", "has", "highest", "top", "show",
    "tell", "me", "about", "do", "does", "how"
}


QUERY_INTENT_KEYWORDS = {
    "anomaly": ["anomaly", "risk", "probability", "alert", "high-risk", "scored"],
    "route": ["route", "coverage", "hub", "departure", "airport"],
    "model": ["model", "feature", "importance", "driver", "drivers", "explain"],
    "airline": ["airline", "airlines", "carrier", "carriers"],
    "summary": ["summary", "overview", "intelligence"],
}


SECTION_BOOST_RULES = {
    "scored anomaly summary": ["anomaly", "risk", "probability", "scored", "alert"],
    "fused intelligence summary": ["route", "hub", "coverage", "airline", "airlines"],
    "feature importance": ["feature", "importance", "driver", "drivers", "model"],
    "ml feature table summary": ["dataset", "distribution", "rows", "anomaly"],
}


def load_knowledge_base() -> str:
    if not KNOWLEDGE_BASE_PATH.exists():
        raise FileNotFoundError(f"Knowledge base not found: {KNOWLEDGE_BASE_PATH}")

    with open(KNOWLEDGE_BASE_PATH, "r", encoding="utf-8") as f:
        return f.read()


def split_into_sections(text: str) -> List[str]:
    sections = [section.strip() for section in text.split("\n\n") if section.strip()]
    return sections


def tokenize(text: str) -> List[str]:
    words = re.findall(r"[a-zA-Z0-9_+-]+", text.lower())
    return [w for w in words if w not in STOPWORDS]


def detect_query_intents(query: str) -> List[str]:
    query_lower = query.lower()
    matched_intents = []

    for intent, keywords in QUERY_INTENT_KEYWORDS.items():
        if any(keyword in query_lower for keyword in keywords):
            matched_intents.append(intent)

    return matched_intents


def compute_section_score(query: str, section: str) -> float:
    query_lower = query.lower()
    section_lower = section.lower()

    query_tokens = tokenize(query)
    section_tokens = tokenize(section)

    score = 0.0

    # 1. direct token overlap
    overlap_count = sum(1 for token in query_tokens if token in section_tokens)
    score += overlap_count * 2.0

    # 2. exact phrase presence
    if query_lower in section_lower:
        score += 8.0

    # 3. intent-aware keyword boosts
    detected_intents = detect_query_intents(query)
    for intent in detected_intents:
        keywords = QUERY_INTENT_KEYWORDS.get(intent, [])
        for keyword in keywords:
            if keyword in section_lower:
                score += 2.5

    # 4. section title/domain boost
    for section_title, related_keywords in SECTION_BOOST_RULES.items():
        if section_title in section_lower:
            for keyword in query_tokens:
                if keyword in related_keywords:
                    score += 4.0

    # 5. numeric / metric query boost
    if any(word in query_lower for word in ["highest", "top", "most", "largest", "strongest"]):
        if any(word in section_lower for word in ["top", "highest", "importance", "probability", "route_flights"]):
            score += 2.0

    return score


def retrieve_relevant_sections(query: str, sections: List[str], top_k: int = 3) -> List[Tuple[int, str, float]]:
    scored_sections = []

    for idx, section in enumerate(sections):
        score = compute_section_score(query, section)
        scored_sections.append((idx, section, score))

    scored_sections.sort(key=lambda x: x[2], reverse=True)

    filtered = [item for item in scored_sections if item[2] > 0]
    return filtered[:top_k]


def build_analyst_summary(query: str, retrieved_sections: List[Tuple[int, str, float]]) -> str:
    query_lower = query.lower()

    if not retrieved_sections:
        return (
            "I could not find a strong match in the current project knowledge base. "
            "Try asking about anomalies, airlines, route coverage, hubs, or model drivers."
        )

    if "anomaly" in query_lower or "probability" in query_lower or "risk" in query_lower:
        return (
            "The retrieved sections indicate the highest-risk airlines or airline snapshots based on the scored anomaly output. "
            "Focus on predicted anomaly probability, live flight counts, and count_change as the most relevant monitoring signals."
        )

    if "route" in query_lower or "hub" in query_lower or "coverage" in query_lower:
        return (
            "The retrieved sections highlight which airlines have the strongest route presence across tracked hubs. "
            "Focus on route_flights and departure hub patterns to identify the strongest route coverage."
        )

    if "feature" in query_lower or "driver" in query_lower or "model" in query_lower:
        return (
            "The retrieved sections show the main model drivers behind anomaly detection. "
            "The strongest signals should be interpreted from feature importance, especially operational change and flight volume related fields."
        )

    if "airline" in query_lower or "airlines" in query_lower:
        return (
            "The retrieved sections summarize the most operationally prominent airlines in the current tracked dataset. "
            "Review fused intelligence, route coverage, and anomaly scoring together for a balanced interpretation."
        )

    return (
        "Based on the retrieved evidence, the most relevant operational findings are summarized from the project knowledge base. "
        "Review the sections above for exact airline-level, hub-level, route-level, or anomaly-level details."
    )


def format_response(query: str, retrieved_sections: List[Tuple[int, str, float]]) -> str:
    lines = []
    lines.append("Flight Ops Analyst Assistant")
    lines.append("=" * 30)
    lines.append(f"Question: {query}")
    lines.append("")
    lines.append("Retrieved Evidence:")

    if not retrieved_sections:
        lines.append("- No relevant sections found.")
    else:
        for i, (idx, section, score) in enumerate(retrieved_sections, start=1):
            lines.append("")
            lines.append(f"[Section {i} | Score={score:.2f}]")
            lines.append(section)

    lines.append("")
    lines.append("Analyst Summary:")
    lines.append(build_analyst_summary(query, retrieved_sections))

    return "\n".join(lines)


def main() -> None:
    kb_text = load_knowledge_base()
    sections = split_into_sections(kb_text)

    print("Flight Ops Analyst Assistant Ready.")
    print("Type a question, or type 'exit' to stop.\n")

    while True:
        query = input("Your question: ").strip()

        if query.lower() == "exit":
            print("Exiting assistant.")
            break

        retrieved = retrieve_relevant_sections(query, sections, top_k=3)
        response = format_response(query, retrieved)

        print()
        print(response)
        print("\n" + "-" * 60 + "\n")


if __name__ == "__main__":
    main()