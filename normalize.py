# normalize.py
from typing import Any, Dict

DENY_KEYS = {
    # execution / timing
    "Actual Startup Time",
    "Actual Total Time",
    "Actual Rows",
    "Actual Loops",
    "Execution Time",
    "Planning Time",

    # cost estimates
    "Startup Cost",
    "Total Cost",
    "Plan Rows",
    "Plan Width",

    # buffers / IO
    "Shared Hit Blocks",
    "Shared Read Blocks",
    "Shared Dirtied Blocks",
    "Shared Written Blocks",
    "Local Hit Blocks",
    "Local Read Blocks",
    "Local Dirtied Blocks",
    "Local Written Blocks",
    "Temp Read Blocks",
    "Temp Written Blocks",

    # parallel / runtime
    "Workers",
    "Workers Planned",
    "Workers Launched",
    "Peak Memory Usage",
    "Disk Usage",
    "HashAgg Batches",
    "Batches",

    # misc
    "Async Capable",
    "Parent Relationship",

    "Filters",
    "filter",
}


def normalize_plan(plan_json: dict) -> dict:
    """
    Normalize a PostgreSQL EXPLAIN (FORMAT JSON) plan into a
    canonical, execution-invariant tree.
    """

    # Handle string input (text EXPLAIN output, not JSON format)
    if isinstance(plan_json, str):
        return {"_raw_text": plan_json, "_format": "text"}

    # unwrap EXPLAIN's list wrapper
    if isinstance(plan_json, list):
        plan_json = plan_json[0]

    # Handle case where unwrapped list item is a string
    if isinstance(plan_json, str):
        return {"_raw_text": plan_json, "_format": "text"}

    # unwrap top-level Plan
    node = plan_json["Plan"] if "Plan" in plan_json else plan_json

    def normalize_node(node: Dict[str, Any]) -> Dict[str, Any]:
        normalized = {}

        for key, value in node.items():
            if key in DENY_KEYS:
                continue

            if key == "Plans":
                normalized["Plans"] = [
                    normalize_node(child) for child in value
                ]
            else:
                normalized[key] = value

        return normalized

    return normalize_node(node)