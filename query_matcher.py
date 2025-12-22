import json
import hashlib
import pprint
from typing import Dict, Any


#Removing fields that we know is noise and differs execution to execution
DENY_KEYS = {
    # Timing & execution stats
    "Actual Startup Time",
    "Actual Total Time",
    "Actual Rows",
    "Actual Loops",
    "Execution Time",
    "Planning Time",

    # Cost estimates
    "Startup Cost",
    "Total Cost",
    "Plan Rows",
    "Plan Width",

    # Buffer / I/O stats
    "Shared Hit Blocks",
    "Shared Read Blocks",
    "Shared Dirtied Blocks",
    "Shared Written Blocks",
    "Local Hit Blocks",
    "Local Read Blocks",
    "Local Dirtied Blocks",
    "Local Written Blocks",
    "Temp Read Blocks",
    "Temp Written Blocks",

    # Parallel / worker info
    "Workers",
    "Workers Planned",
    "Workers Launched",

    # Misc metadata
    "Parent Relationship",
    "Async Capable",

    # Sort / runtime internals
"Sort Method",
"Sort Space Used",
"Sort Space Type",

# Runtime-dependent selectivity counters
"Rows Removed by Filter",
}


def normalizePlan(plan_json:dict)->dict:
    """
    Docstring for normalizePlan
    
    :param plan_json: Here we normalize a postgreSQL plan (EXPLAIN ANALYZE, FORMAT JSON) by remove all those fields in the 
    JSON that are run-specific or change after every run and are not unique to the query & data.
    Runtime data and runtime summaries are intentionally ignored.
    :type plan_json: dict
    :return: dictionary which is normalized after removing noise
    :rtype: dict
    """
    #first we unwrap instance if needed, else if it is one json of a plan, we continue
    if isinstance(plan_json, list):
        plan_json = plan_json[0]
    
    if "Plan" in plan_json:
        node = plan_json["Plan"]
    else:
        node = plan_json
    
    #remove deny keys, by adding a subfunction (if not so efficient, will find another approach)
    def normalize_node(node:dict)->dict:
        normalized = {}
        for key, value in node.items():
            if key in DENY_KEYS:
                continue
            #this normalises the plan and takes care of children if necessary
            if key == "Plans" and isinstance(value, list):
                normalized["Plans"] = [normalize_node(child) for child in value]
            else:
                normalized[key] = value
        return normalized
    
    return normalize_node(node)


    
# F1: Plan Simplification
# def simplifier(plan_json: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Simplifies a PostgreSQL EXPLAIN (FORMAT JSON) query plan into a
#     canonical, engine-agnostic representation.

#     Canonical node format:
#     {
#         "op": <logical operator>,
#         "algo": <physical algorithm>,
#         "relation": <table name or None>,
#         "children": [<child nodes>]
#     }
#     """

#     # Unwrap top-level Postgres EXPLAIN JSON if present
#     node = plan_json["Plan"] if "Plan" in plan_json else plan_json

#     node_type = node.get("Node Type")

#     op = None
#     algo = None
#     relation = None
#     children = []

#         # Step 3: map raw operator to canonical op and algo
#     if node_type in OPERATOR_MAP:
#         op, algo = OPERATOR_MAP[node_type]

#         # Extract relation name only for scan operators
#         if op == "Scan":
#             relation = node.get("Relation Name")

#     else:
#         # Fallback for unsupported operators
#         op = "Unknown"
#         algo = node_type

#     # Recursively process child plan nodes
#     for child in node.get("Plans", []):
#         children.append(simplifier(child))

#     return {
#         "op": op,
#         "algo": algo,
#         "relation": relation,
#         "children": children
#     }




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



SAMPLE_PLANS = {

    # ----------------------------
    # No-join plans (baseline)
    # ----------------------------

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

    # ----------------------------
    # Simple join plans
    # ----------------------------

    3: {
        "Plan": {
            "Node Type": "Hash Join",
            "Plans": [
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "orders"
                },
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "lineitem"
                }
            ]
        }
    },

    4: {
        "Plan": {
            "Node Type": "Nested Loop",
            "Plans": [
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "orders"
                },
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "lineitem"
                }
            ]
        }
    },

    # ----------------------------
    # Join with aggregation
    # ----------------------------

    5: {
        "Plan": {
            "Node Type": "HashAggregate",
            "Plans": [
                {
                    "Node Type": "Hash Join",
                    "Plans": [
                        {
                            "Node Type": "Seq Scan",
                            "Relation Name": "orders"
                        },
                        {
                            "Node Type": "Seq Scan",
                            "Relation Name": "lineitem"
                        }
                    ]
                }
            ]
        }
    },

    # ----------------------------
    # Join order variation
    # ----------------------------

    6: {
        "Plan": {
            "Node Type": "Hash Join",
            "Plans": [
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "lineitem"
                },
                {
                    "Node Type": "Seq Scan",
                    "Relation Name": "orders"
                }
            ]
        }
    }
}

p_x={"day":1,"cutoff_date":"1992-01-03","started_at":"2025-09-29 14:50:01,3N","finished_at":"2025-09-29 14:50:02,3N","plan":[  {    "Plan": {      "Node Type": "Aggregate",      "Strategy": "Sorted",      "Partial Mode": "Finalize",      "Parallel Aware": "false",      "Async Capable": "false",      "Startup Cost": 144867.61,      "Total Cost": 144878.91,      "Plan Rows": 6,      "Plan Width": 236,      "Actual Startup Time": 595.333,      "Actual Total Time": 595.902,      "Actual Rows": 2,      "Actual Loops": 1,      "Group Key": ["l_returnflag", "l_linestatus"],      "Shared Hit Blocks": 208,      "Shared Read Blocks": 112504,      "Shared Dirtied Blocks": 0,      "Shared Written Blocks": 0,      "Local Hit Blocks": 0,      "Local Read Blocks": 0,      "Local Dirtied Blocks": 0,      "Local Written Blocks": 0,      "Temp Read Blocks": 0,      "Temp Written Blocks": 0,      "Plans": [        {          "Node Type": "Gather Merge",          "Parent Relationship": "Outer",          "Parallel Aware": "false",          "Async Capable": "false",          "Startup Cost": 144867.61,          "Total Cost": 144878.36,          "Plan Rows": 12,          "Plan Width": 236,          "Actual Startup Time": 595.295,          "Actual Total Time": 595.868,          "Actual Rows": 6,          "Actual Loops": 1,          "Workers Planned": 2,          "Workers Launched": 2,          "Shared Hit Blocks": 208,          "Shared Read Blocks": 112504,          "Shared Dirtied Blocks": 0,          "Shared Written Blocks": 0,          "Local Hit Blocks": 0,          "Local Read Blocks": 0,          "Local Dirtied Blocks": 0,          "Local Written Blocks": 0,          "Temp Read Blocks": 0,          "Temp Written Blocks": 0,          "Plans": [            {              "Node Type": "Aggregate",              "Strategy": "Sorted",              "Partial Mode": "Partial",              "Parent Relationship": "Outer",              "Parallel Aware": "false",              "Async Capable": "false",              "Startup Cost": 143867.59,              "Total Cost": 143876.95,              "Plan Rows": 6,              "Plan Width": 236,              "Actual Startup Time": 592.593,              "Actual Total Time": 592.601,              "Actual Rows": 2,              "Actual Loops": 3,              "Group Key": ["l_returnflag", "l_linestatus"],              "Shared Hit Blocks": 208,              "Shared Read Blocks": 112504,              "Shared Dirtied Blocks": 0,              "Shared Written Blocks": 0,              "Local Hit Blocks": 0,              "Local Read Blocks": 0,              "Local Dirtied Blocks": 0,              "Local Written Blocks": 0,              "Temp Read Blocks": 0,              "Temp Written Blocks": 0,              "Workers": [              ],              "Plans": [                {                  "Node Type": "Sort",                  "Parent Relationship": "Outer",                  "Parallel Aware": "false",                  "Async Capable": "false",                  "Startup Cost": 143867.59,                  "Total Cost": 143868.20,                  "Plan Rows": 246,                  "Plan Width": 25,                  "Actual Startup Time": 592.578,                  "Actual Total Time": 592.578,                  "Actual Rows": 19,                  "Actual Loops": 3,                  "Sort Key": ["l_returnflag", "l_linestatus"],                  "Sort Method": "quicksort",                  "Sort Space Used": 26,                  "Sort Space Type": "Memory",                  "Shared Hit Blocks": 208,                  "Shared Read Blocks": 112504,                  "Shared Dirtied Blocks": 0,                  "Shared Written Blocks": 0,                  "Local Hit Blocks": 0,                  "Local Read Blocks": 0,                  "Local Dirtied Blocks": 0,                  "Local Written Blocks": 0,                  "Temp Read Blocks": 0,                  "Temp Written Blocks": 0,                  "Workers": [                    {                      "Worker Number": 0,                      "Sort Method": "quicksort",                      "Sort Space Used": 25,                      "Sort Space Type": "Memory"                    },                    {                      "Worker Number": 1,                      "Sort Method": "quicksort",                      "Sort Space Used": 25,                      "Sort Space Type": "Memory"                    }                  ],                  "Plans": [                    {                      "Node Type": "Seq Scan",                      "Parent Relationship": "Outer",                      "Parallel Aware": "true",                      "Async Capable": "false",                      "Relation Name": "lineitem",                      "Alias": "lineitem",                      "Startup Cost": 0.00,                      "Total Cost": 143857.82,                      "Plan Rows": 246,                      "Plan Width": 25,                      "Actual Startup Time": 25.821,                      "Actual Total Time": 592.493,                      "Actual Rows": 19,                      "Actual Loops": 3,                      "Filter": "(l_shipdate <= '1992-01-03'::date)",                      "Rows Removed by Filter": 2000386,                      "Shared Hit Blocks": 96,                      "Shared Read Blocks": 112504,                      "Shared Dirtied Blocks": 0,                      "Shared Written Blocks": 0,                      "Local Hit Blocks": 0,                      "Local Read Blocks": 0,                      "Local Dirtied Blocks": 0,                      "Local Written Blocks": 0,                      "Temp Read Blocks": 0,                      "Temp Written Blocks": 0,                      "Workers": [                      ]                    }                  ]                }              ]            }          ]        }      ]    },    "Planning": {      "Shared Hit Blocks": 102,      "Shared Read Blocks": 9,      "Shared Dirtied Blocks": 0,      "Shared Written Blocks": 0,      "Local Hit Blocks": 0,      "Local Read Blocks": 0,      "Local Dirtied Blocks": 0,      "Local Written Blocks": 0,      "Temp Read Blocks": 0,      "Temp Written Blocks": 0    },    "Planning Time": 1.420,    "Triggers": [    ],    "Execution Time": 595.961  }]}


# CLI Driver

def run_menu():
    plan3 = p_x["plan"]
    simplified_3 = normalizePlan(plan3)
    # fp3 = plan_fingerprint(simplified_3)
    print("\n\n\n\n\n\n")
    print("PLAN:")
    pprint.pprint(plan3)
    print("\n\n\n\n\n\n")
    print("\nSIMPLIFIED PLAN:")
    pprint.pprint(simplified_3)
    # print("\n\n\n\n\n\n")
    # print("\nSIMPLIFIED PLAN JSON:")
    # pprint.pprint(json.dumps(simplified_3, indent=4))


if __name__ == "__main__":
    run_menu()