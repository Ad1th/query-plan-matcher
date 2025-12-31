# loader.py
import json
from pathlib import Path
from typing import List, Tuple, Union


def load_plan_runs(
    experiments_root: Union[str, Path],
    database: str,
    query_id: str,
) -> List[Tuple[str, dict, dict]]:
    """
    Loads:
    experiments/<database>/<query_id>/<timestamp>/plan.json
    experiments/<database>/<query_id>/<timestamp>/metrics.json

    Returns:
        List of (run_timestamp, plan_json, metrics_json)
    """

    experiments_root = Path(experiments_root).resolve()

    base = experiments_root / database / query_id
    if not base.exists():
        return []

    runs: List[Tuple[str, dict, dict]] = []

    run_dirs = sorted(p for p in base.iterdir() if p.is_dir())
    print(f"[loader] Found {len(run_dirs)} run directories in {base}")

    for run_dir in run_dirs:
        plan_file = run_dir / "plan.json"
        metrics_file = run_dir / "metrics.json"
        if plan_file.exists():
            try:
                with open(plan_file, "r") as f:
                    plan_json = json.load(f)
                
                metrics_json = {}
                if metrics_file.exists():
                    with open(metrics_file, "r") as f:
                        metrics_json = json.load(f)
                
                runs.append((run_dir.name, plan_json, metrics_json))
            except Exception as e:
                print(f"[WARN] Failed to load {run_dir}: {e}")

    return runs