import re
from typing import Optional, Tuple


def split_callsign(callsign: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Split callsign into airline prefix and numeric suffix where possible.
    Example:
        AIC1713 -> ("AIC", "1713")
        IGO5396 -> ("IGO", "5396")
        ETD4407 -> ("ETD", "4407")
    """
    if not callsign:
        return None, None

    callsign = str(callsign).strip().upper()
    match = re.match(r"^([A-Z]+)(\d+)$", callsign)

    if match:
        return match.group(1), match.group(2)

    return callsign, None