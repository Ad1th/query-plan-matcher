# fingerprint.py
import json
import hashlib
from typing import Dict, Any


def plan_fingerprint(normalized_plan: Dict[str, Any]) -> str:
    """
    Deterministically fingerprint a normalized plan tree.
    """

    serialized = json.dumps(
        normalized_plan,
        sort_keys=True,
        separators=(",", ":"),
    )

    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()