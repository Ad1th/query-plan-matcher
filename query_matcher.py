import json
import hashlib
import pprint
from typing import Dict, Any



# F1: Plan Simplification

def simplifier(plan_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simplifies a PostgreSQL EXPLAIN (FORMAT JSON) query plan into a
    canonical, engine-agnostic representation.

    Canonical node format:
    {
        "op": <logical operator>,
        "algo": <physical algorithm>,
        "relation": <table name or None>,
        "children": [<child nodes>]
    }
    """

    # Unwrap top-level Postgres EXPLAIN JSON if present
    node = plan_json["Plan"] if "Plan" in plan_json else plan_json

    node_type = node.get("Node Type")

    op = None
    algo = None
    relation = None
    children = []

    # Map PostgreSQL node types to canonical operators
    if node_type == "Seq Scan":
        op = "Scan"
        algo = "SeqScan"
        relation = node.get("Relation Name")

    elif node_type == "Index Scan":
        op = "Scan"
        algo = "IndexScan"
        relation = node.get("Relation Name")

    elif node_type == "HashAggregate":
        op = "Aggregate"
        algo = "HashAggregate"

    elif node_type == "Sort":
        op = "Sort"
        algo = "InMemorySort"
        
    elif node_type == "Hash Join":
        op = "Join"
        algo = "HashJoin"

    elif node_type == "Nested Loop":
        op = "Join"
        algo = "NestedLoop"

    elif node_type == "Merge Join":
        op = "Join"
        algo = "MergeJoin"   

    else:
        raise ValueError(f"Unsupported node type: {node_type}")

    # Recursively process child plan nodes
    for child in node.get("Plans", []):
        children.append(simplifier(child))

    return {
        "op": op,
        "algo": algo,
        "relation": relation,
        "children": children
    }




# F2: Plan Fingerprinting

def plan_fingerprint(simplified_plan: Dict[str, Any]) -> str:
    """
    Generates a deterministic fingerprint for a canonical query plan
    by hashing its serialized JSON representation.
    """

    serialized = json.dumps(
        simplified_plan,
        sort_keys=True,
        separators=(",", ":")
    )

    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()





# F3: Plan Matching

def matcher(fp1: str, fp2: str) -> bool:
    """
    Compares two plan fingerprints and determines equivalence.
    """
    return fp1 == fp2



# Sample Plans (for testing)

SAMPLE_PLANS = {
    1: {
        "Plan": {
            "Node Type": "Sort",
            "Plans": [
                {
                    "Node Type": "HashAggregate",
                    "Plans": [
                        {
                            "Node Type": "Seq Scan",
                            "Relation Name": "lineitem"
                        }
                    ]
                }
            ]
        }
    },
    2: {
        "Plan": {
            "Node Type": "Sort",
            "Plans": [
                {
                    "Node Type": "HashAggregate",
                    "Plans": [
                        {
                            "Node Type": "Index Scan",
                            "Relation Name": "lineitem"
                        }
                    ]
                }
            ]
        }
    },
    3: {
        "Plan": {
            "Node Type": "HashAggregate",
            "Plans": [
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "lineitem"
                }
            ]
        }
    }
}




# CLI Driver

def run_menu():
    print("\n=== Query Plan Matcher ===\n")
    print("Available Plans:")
    print("1. Seq Scan + HashAggregate + Sort")
    print("2. Index Scan + HashAggregate + Sort")
    print("3. Seq Scan + HashAggregate (No Sort)")

    p1 = int(input("\nSelect Plan 1 (1/2/3): "))
    p2 = int(input("Select Plan 2 (1/2/3): "))

    plan1 = SAMPLE_PLANS[p1]
    plan2 = SAMPLE_PLANS[p2]

    # F1
    simplified_1 = simplifier(plan1)
    simplified_2 = simplifier(plan2)

    # F2
    fp1 = plan_fingerprint(simplified_1)
    fp2 = plan_fingerprint(simplified_2)

    # F3
    is_same = matcher(fp1, fp2)

    print("\n--- Simplified Plan 1 ---")
    pprint.pprint(simplified_1)

    print("\n--- Simplified Plan 2 ---")
    pprint.pprint(simplified_2)

    print("\n--- Fingerprints ---")
    print("Plan 1:", fp1)
    print("Plan 2:", fp2)

    print("\n--- Result ---")
    print("Plans are equivalent?", is_same)


if __name__ == "__main__":
    run_menu()