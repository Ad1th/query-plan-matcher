# main.py

import json
from pathlib import Path
from query_matcher import compare_all_runs


def format_plan(plan: dict, indent: int = 2) -> str:
    """Format a plan dict as indented JSON for terminal readability."""
    return json.dumps(plan, indent=indent)


def main():
    # Resolve DB_Performance/ from Query_Plan_Matcher/main.py
    # parents[0] = Query_Plan_Matcher/, parents[1] = DB_Performance/
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    EXPERIMENTS_ROOT = PROJECT_ROOT / "experiments"

    DATABASE = "postgres"
    QUERY_ID = ""

    print(f"Looking for experiments in: {EXPERIMENTS_ROOT}")

    results = compare_all_runs(
        experiments_root=EXPERIMENTS_ROOT,
        database=DATABASE,
        query_id=QUERY_ID,
    )

    if not results:
        print("No plan runs found.")
        return

    print(f"\nPlan comparison results for {DATABASE}/{QUERY_ID}:\n")

    for r in results:
        print(f"Run {r['run_a']} → {r['run_b']}")
        print(f"Same plan: {r['same_plan']}")
        print()

        # Runtime comparison
        runtime_a = r['metrics_a'].get('runtime_ms', 0)
        runtime_b = r['metrics_b'].get('runtime_ms', 0)
        diff = runtime_b - runtime_a
        diff_sign = "+" if diff >= 0 else ""
        
        if runtime_a != 0:
            ratio = runtime_b / runtime_a
        else:
            ratio = float('inf') if runtime_b > 0 else 1.0
        
        print("Runtime: (seconds)")
        print(f"  A: {runtime_a:.3f}")
        print(f"  B: {runtime_b:.3f}")
        print(f"  Δ: {diff_sign}{diff:.3f}")
        print(f"  Ratio: {ratio:.3f}x")
        print()

        print(f"[Raw Plan – Run {r['run_a']}]")
        print(format_plan(r['raw_plan_a']))
        print()

        print(f"[Raw Plan – Run {r['run_b']}]")
        print(format_plan(r['raw_plan_b']))
        print()

        print(f"[Normalized Plan – Run {r['run_a']}]")
        print(format_plan(r['normalized_plan_a']))
        print()

        print(f"[Normalized Plan – Run {r['run_b']}]")
        print(format_plan(r['normalized_plan_b']))
        print()

        print("=" * 60)
        print()


if __name__ == "__main__":
    main()