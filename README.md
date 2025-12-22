## Query Plan Matcher

A small utility that simplifies PostgreSQL EXPLAIN (FORMAT JSON) query plans to a canonical, engine-agnostic representation, computes deterministic fingerprints for those simplified plans, and compares fingerprints to detect equivalence.

This repository contains a single module, `query_matcher.py`, which implements three core functions:

- `simplifier(plan_json: Dict[str, Any]) -> Dict[str, Any]` — Convert a Postgres EXPLAIN JSON plan node into a canonical tree of nodes with keys `op`, `algo`, `relation`, and `children`.
- `plan_fingerprint(simplified_plan: Dict[str, Any]) -> str` — Produce a deterministic SHA-256 hex fingerprint from the canonical JSON representation.
- `matcher(fp1: str, fp2: str) -> bool` — Compare two fingerprints for equality.

## Features

- Simplifies a subset of Postgres plan node types (e.g. `Seq Scan`, `Index Scan`, `HashAggregate`, `Sort`) into a normalized representation.
- Deterministic plan fingerprinting using JSON serialization + SHA-256 hashing.
- Simple CLI demo that compares three sample plans shipped with the module.

## Requirements

This project uses only the Python standard library. No external packages are required.

- Recommended Python: 3.8+

See `requirements.txt` for a short note.

## PostgreSQL execution-plan analysis for TPC-H

This repository contains a small research-oriented tool for analyzing variability in the observed performance of the same SQL workload (TPC-H) on PostgreSQL. The tool compares two EXPLAIN (ANALYZE, FORMAT JSON) outputs for the same SQL statement and helps determine whether observed runtime differences correlate with changes in the executed plan or with other factors (filter selectivity, I/O, etc.).

The tone and assumptions are academic: the tool reports correlations and structural differences in execution plans — it does not claim causal proof nor does it attempt to predict or optimize planner choices.

## Core idea

The tool follows a simple pipeline:

- Normalize execution plans by removing run-specific noise (timing, cost estimates, buffer/I/O counters, worker counts).
- Produce a deterministic fingerprint for each normalized plan so identical plan structure hashes to the same value.
- Compare fingerprints to detect structural plan changes.
- Extract runtime (total execution time) and filter predicate selectivity statistics from each EXPLAIN output.
- Correlate plan equivalence with runtime and selectivity differences to help identify when plan changes are likely responsible for performance differences.

The implementation in `query_matcher.py` was developed and evaluated on PostgreSQL running TPC-H queries. It is intentionally small and focused on analysis rather than automation or remediation.

## Key components

- `normalizePlan(plan_json: dict) -> dict` — Remove execution-time noise (see `DENY_KEYS`) while preserving the plan's structural shape and important attributes (node types, relation names, predicates). Input accepts a single JSON plan or the list wrapper produced by `EXPLAIN (ANALYZE, FORMAT JSON)`.
- `planFingerprint(normalized_plan: dict) -> str` — Deterministically serializes and hashes a normalized plan (SHA-256) to produce a stable fingerprint for identity comparisons.
- `extractRuntime(plan_json: dict) -> float` — Extract the top-level `Execution Time` (in milliseconds) from an EXPLAIN output.
- `extractFilterStats(plan_json: dict) -> dict` — Walk the plan tree, collect filter predicates, sum actual rows and rows removed by filter, and compute empirical selectivity (actual_rows / (actual_rows + rows_removed)).
- `compare_plans(plan_a: dict, plan_b: dict) -> dict` — High-level comparator that returns whether plans are identical (by fingerprint), the runtime difference, selectivity difference, and lists of predicates for each plan.
- `print_comparison(result: dict)` — Nicely formatted CLI summary of the comparison result.

## Output summary (example)

Typical summary printed by the tool:

## Plan A vs Plan B

Same plan: YES
Runtime difference: 0.000 ms
Filter selectivity difference: 0.000000e+00

Detailed Plan A Stats:
Execution Runtime (ms): 595.961
Filter stats:
{'actual_rows': 19,
'filters': ["(l_shipdate <= '1992-01-03'::date)"],
'rows_removed': 2000386,
'selectivity': 9.498076639480506e-06}

Detailed Plan B Stats:
Execution Runtime (ms): 595.961
Filter stats:
{'actual_rows': 19,
'filters': ["(l_shipdate <= '1992-01-03'::date)"],
'rows_removed': 2000386,
'selectivity': 9.498076639480506e-06}

Another (different) example showing a plan/behavior change:

## Plan A vs Plan B

Same plan: NO
Runtime difference: 436.872 ms
Filter selectivity difference: 5.838818e-04

Detailed Plan A Stats:
Execution Runtime (ms): 595.961
Filter stats: (same as above)

Detailed Plan B Stats:
Execution Runtime (ms): 159.089
Filter stats:
{'actual_rows': 1187,
'filters': ["(l_shipdate <= '1992-01-19'::date)"],
'rows_removed': 1999218,
'selectivity': 0.0005933798405822821}

These examples illustrate how the tool surfaces whether two runs used the same structural plan, how much the runtimes differed, and how filter selectivity changed between runs.

## Scope and assumptions

- Workload: targeted at PostgreSQL TPC-H queries (benchmarks used during development). The code should work for other PostgreSQL plans but has not been exhaustively validated across all node types.
- Focus: structural plan comparison and empirical performance analysis (correlation). The tool does not try to explain the planner's decision process or to recommend fixes.
- Normalization: the normalization step removes planner noise (timing, cost estimates, buffer counters) to focus on operator structure and predicates. If you want more aggressive normalization (e.g., reorder commutative children), extend `normalizePlan` accordingly.

## How to use

1. Run your SQL statement in PostgreSQL with EXPLAIN ANALYZE and JSON format:

```sql
EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) <your-query>;
```

Save the JSON output for each measured run (Plan A and Plan B).

2. Provide two JSON plan objects to the script. The repository contains an embedded demo that compares sample plans. To adapt the script for file input, replace the demo section with code that loads two JSON files and calls `compare_plans(plan_a, plan_b)`.

Minimal invocation (demo):

```bash
python query_matcher.py
```

To compare two saved EXPLAIN outputs (example pattern to modify the script):

```python
import json
from query_matcher import compare_plans, print_comparison

with open('planA.json') as f:
	plan_a = json.load(f)
with open('planB.json') as f:
	plan_b = json.load(f)

result = compare_plans(plan_a, plan_b)
print_comparison(result)
```

3. Interpret the output:

- If `Same plan: YES` and runtimes differ, runtime variance is likely caused by non-structural factors (cache, I/O noise, skew, resource contention).
- If `Same plan: NO` and runtime difference is large, the plan change is a strong candidate explanation; examine the `filters_*` lists and `selectivity_*` values for insight.
- Use the printed filter predicates and selectivity numbers to understand whether different predicate selectivities (data changes or parameter values) may have influenced planner choice.

## Development notes and next steps

- Add a small test suite (pytest) to lock behavior for `normalizePlan`, `planFingerprint`, `extractRuntime`, and `extractFilterStats`.
- Add a CLI wrapper (argparse) that accepts two JSON files and prints machine-readable output (JSON) for pipeline integration.
- Extend normalization to support more node types and optional canonicalization rules (e.g., commutative reordering).

## Credits

Developed for exploratory analysis of TPC-H workloads on PostgreSQL. Use for research and systems analysis; contributions and PRs welcome.
