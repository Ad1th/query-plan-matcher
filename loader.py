# loader.py
import json
from pathlib import Path
from typing import List, Tuple


def load_plan_runs(
    experiments_root: str,
    database: str,
    query_id: str,
) -> List[Tuple[str, dict]]:
    """
    Load all plan.json files for a given query.
    Returns: [(timestamp, plan_json), ...]
    """

    base = Path(experiments_root) / database / query_id
    if not base.exists():
        return []

    runs = []
    for run_dir in sorted(base.iterdir()):
        plan_file = run_dir / "plan.json"
        if plan_file.exists():
            with open(plan_file) as f:
                runs.append((run_dir.name, json.load(f)))

    return runs