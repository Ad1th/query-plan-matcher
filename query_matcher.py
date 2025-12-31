# query_matcher.py
from normalize import normalize_plan
from fingerprint import plan_fingerprint
from matcher import matcher
from loader import load_plan_runs


def compare_two_runs(plan_a: dict, plan_b: dict) -> dict:
    norm_a = normalize_plan(plan_a)
    norm_b = normalize_plan(plan_b)

    fp_a = plan_fingerprint(norm_a)
    fp_b = plan_fingerprint(norm_b)

    return {
        "same_plan": matcher(fp_a, fp_b),
        "fingerprint_a": fp_a,
        "fingerprint_b": fp_b,
    }


def compare_all_runs(
    experiments_root: str,
    database: str,
    query_id: str,
):
    runs = load_plan_runs(experiments_root, database, query_id)

    results = []
    for i in range(len(runs) - 1):
        ts_a, plan_a = runs[i]
        ts_b, plan_b = runs[i + 1]

        comparison = compare_two_runs(plan_a, plan_b)
        comparison["run_a"] = ts_a
        comparison["run_b"] = ts_b

        results.append(comparison)

    return results
