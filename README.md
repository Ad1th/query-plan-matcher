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

## Installation

1. (Optional) Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

2. Install dependencies (none required):

```bash
pip install -r requirements.txt
```

## Usage

Run the small interactive demo from the repository root:

```bash
python query_matcher.py
```

You'll be prompted to select two sample plans (1/2/3). The script will print the simplified plans, their fingerprints, and whether the fingerprints match.

## Programmatic usage

You can import the functions and use them directly from other Python code:

```python
from query_matcher import simplifier, plan_fingerprint, matcher, SAMPLE_PLANS

plan = SAMPLE_PLANS[1]
simple = simplifier(plan)
fp = plan_fingerprint(simple)
print(simple)
print(fp)

# compare two plans
same = matcher(plan_fingerprint(simplifier(SAMPLE_PLANS[1])), plan_fingerprint(simplifier(SAMPLE_PLANS[2])))
print('Equivalent?', same)
```

## Canonical node format

A simplified node follows this structure:

```json
{
	"op": "Scan|Aggregate|Sort|...",
	"algo": "SeqScan|IndexScan|HashAggregate|InMemorySort|...",
	"relation": "table_name_or_null",
	"children": [ ... ]
}
```

The implementation currently supports a handful of Postgres node types. Unsupported node types will raise `ValueError`.

## Example output (demo)

The CLI prints each simplified plan and its fingerprint. Example (abbreviated):

```
--- Simplified Plan 1 ---
{'op': 'Sort', 'algo': 'InMemorySort', 'relation': None, 'children': [...]}

--- Fingerprints ---
Plan 1: 2b1f... (sha256 hex)
Plan 2: 7a5c... (sha256 hex)

--- Result ---
Plans are equivalent? False
```

## Extending and Notes

- Add support for more Postgres `Node Type` values by extending the mapping logic in `simplifier`.
- Consider adding JSON file input and a CLI argument parser (argparse or click) to compare saved EXPLAIN outputs instead of the interactive menu.
- Add unit tests for `simplifier` behaviour for different node shapes and for fingerprint stability.

## Development

Run the demo locally as shown above. To run quick checks, import functions in a Python REPL and call them with sample or real EXPLAIN JSON.

Suggested next steps:

1. Add unit tests (pytest) and a continuous integration workflow.
2. Add support for additional plan node types and plan normalization rules (e.g. ignore cost/rows/stats fields when building the canonical representation).
3. Add an option to canonicalize minor differences such as join order or commutative operator ordering if desired for higher-level matching.

## License & Contribution

Feel free to fork and open PRs. Add a LICENSE file if you want to specify a license.

---

If you'd like, I can also:

- Add a small test suite (pytest) demonstrating expected outputs for the sample plans.
- Wire a simple argparse-driven CLI to accept JSON files or strings.

Tell me which of these you'd prefer next.
