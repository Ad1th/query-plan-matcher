# loader.py
import json
from pathlib import Path
from typing import List, Tuple, Union


def load_plan_runs(
    experiments_root: Union[str, Path],
    database: str,
    query_id: str,
) -> List[Tuple[str, dict]]:
    """
    Loads:
    experiments/<database>/<query_id>/<timestamp>/plan.json

    Returns:
        List of (run_timestamp, plan_json)
    """

    experiments_root = Path(experiments_root).resolve()

    base = experiments_root / database / query_id
    if not base.exists():
        return []

    runs: List[Tuple[str, dict]] = []

    for run_dir in sorted(p for p in base.iterdir() if p.is_dir()):
        plan_file = run_dir / "plan.json"
        if plan_file.exists():
            try:
                with open(plan_file, "r") as f:
                    plan_json = json.load(f)
                runs.append((run_dir.name, plan_json))
            except Exception as e:
                print(f"[WARN] Failed to load {plan_file}: {e}")

    return runs