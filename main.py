# main.py

from query_matcher import compare_all_runs


def main():
    EXPERIMENTS_ROOT = "../query_executer/experiments"
    DATABASE = "postgres"
    QUERY_ID = "TPCH_1" 

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
        print("-" * 40)


if __name__ == "__main__":
    main()