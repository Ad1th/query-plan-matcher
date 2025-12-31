# loader.py
import json
from pathlib import Path
from typing import List, Tuple


def load_plan_runs(
    experiments_root: Path | str,
    database: str,
    query_id: str,
) -> List[Tuple[str, dict]]:
    """
    Loads:
    experiments/<database>/<query_id>/<timestamp>/plan.json
    """

    experiments_root = Path(experiments_root)

    base = experiments_root / database / query_id
    if not base.exists():
        return []

    runs = []
    for run_dir in sorted(p for p in base.iterdir() if p.is_dir()):
        plan_file = run_dir / "plan.json"
        if plan_file.exists():
            runs.append((run_dir.name, json.loads(plan_file.read_text())))

    return runs