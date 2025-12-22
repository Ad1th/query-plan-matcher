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
                if isinstance(value, str) and value.lower() in {"true", "false"}: #normalized boolean values to strings, night also have to add for float and integer if needed
                    normalized[key] = value.lower() == "true"  
                else:
                    normalized[key] = value
        return normalized
    
    return normalize_node(node)


def extractRuntime(plan_json:dict)->float:
    """
    Docstring for extractRuntime
    
    :param plan_json: Extracts total execution runtime (in milliseconds) from a PostgreSQL
    EXPLAIN (ANALYZE, FORMAT JSON) output.
    This uses the top-level 'Execution Time' field and ignores per-node timings.

    We are using the original plan only, and not the normalised plan.
    
    :type plan_json: dict
    :return: returns the runtime in milliseconds as a floating point number
    :rtype: float
    
    """
    #first we unwrap instance if needed, else if it is one json of a plan, we continue (same as normalizePlan)
    if isinstance(plan_json, list):
        plan_json = plan_json[0]
    
    #Raising a key error if we don't find the execution time in the plan
    if "Execution Time" not in plan_json:
        raise KeyError("Execution Time not found in plan JSON")
    return float(plan_json["Execution Time"])


def extractFilterStats(plan_json:dict)->Dict[str,Any]:
    """
    Docstring for extractFilterStats
    
    :param plan_json: Description
    :type plan_json: dict
    :return: Description
    :rtype: Dict[str, Any]
    """

    #again unwrapping if needed
    if isinstance(plan_json, list):
        plan_json = plan_json[0]
    if "Plan" in plan_json:
        node = plan_json["Plan"]
    else:
        node = plan_json

    filters = []
    total_actual_rows = 0
    total_rows_removed = 0
    def traverse(node:dict):
        nonlocal total_actual_rows, total_rows_removed
        # Check for filter in the current node
        if "Filter" in node:
            filters.append(node["Filter"])
            total_actual_rows += node.get("Actual Rows", 0)
            total_rows_removed += node.get("Rows Removed by Filter", 0)
        # Recursively traverse child plans if they exist
        if "Plans" in node:
            for child in node["Plans"]:
                traverse(child)
    traverse(node)
    input_rows = total_actual_rows + total_rows_removed
    selectivity = (
        total_actual_rows / input_rows if input_rows > 0 else None
    )

    return {
        "filters": filters,
        "actual_rows": total_actual_rows,
        "rows_removed": total_rows_removed,
        "selectivity": selectivity,
    }

def planFingerprint(normalized_plan:dict)->str:
    """
    Docstring for planFingerprint
    
    :param normalized_plan: Description
    :type normalized_plan: dict
    :return: Description
    :rtype: str
    """
    plan_str = json.dumps(normalized_plan, sort_keys=True)
    plan_hash = hashlib.sha256(plan_str.encode("utf-8")).hexdigest()
    return plan_hash

def matcher(hash_a:str, hash_b:str)->bool:

    """
    Docstring for matcher
    
    :param hash_a: Description
    :type hash_a: str
    :param hash_b: Description
    :type hash_b: str
    :return: Description
    :rtype: bool
    """
    return hash_a == hash_b



# Sample plan for testing
p_x={"day":1,"cutoff_date":"1992-01-03","started_at":"2025-09-29 14:50:01,3N","finished_at":"2025-09-29 14:50:02,3N","plan":[  {    "Plan": {      "Node Type": "Aggregate",      "Strategy": "Sorted",      "Partial Mode": "Finalize",      "Parallel Aware": False,      "Async Capable": "False",      "Startup Cost": 144867.61,      "Total Cost": 144878.91,      "Plan Rows": 6,      "Plan Width": 236,      "Actual Startup Time": 595.333,      "Actual Total Time": 595.902,      "Actual Rows": 2,      "Actual Loops": 1,      "Group Key": ["l_returnflag", "l_linestatus"],      "Shared Hit Blocks": 208,      "Shared Read Blocks": 112504,      "Shared Dirtied Blocks": 0,      "Shared Written Blocks": 0,      "Local Hit Blocks": 0,      "Local Read Blocks": 0,      "Local Dirtied Blocks": 0,      "Local Written Blocks": 0,      "Temp Read Blocks": 0,      "Temp Written Blocks": 0,      "Plans": [        {          "Node Type": "Gather Merge",          "Parent Relationship": "Outer",          "Parallel Aware": "False",          "Async Capable": "False",          "Startup Cost": 144867.61,          "Total Cost": 144878.36,          "Plan Rows": 12,          "Plan Width": 236,          "Actual Startup Time": 595.295,          "Actual Total Time": 595.868,          "Actual Rows": 6,          "Actual Loops": 1,          "Workers Planned": 2,          "Workers Launched": 2,          "Shared Hit Blocks": 208,          "Shared Read Blocks": 112504,          "Shared Dirtied Blocks": 0,          "Shared Written Blocks": 0,          "Local Hit Blocks": 0,          "Local Read Blocks": 0,          "Local Dirtied Blocks": 0,          "Local Written Blocks": 0,          "Temp Read Blocks": 0,          "Temp Written Blocks": 0,          "Plans": [            {              "Node Type": "Aggregate",              "Strategy": "Sorted",              "Partial Mode": "Partial",              "Parent Relationship": "Outer",              "Parallel Aware": "False",              "Async Capable": "False",              "Startup Cost": 143867.59,              "Total Cost": 143876.95,              "Plan Rows": 6,              "Plan Width": 236,              "Actual Startup Time": 592.593,              "Actual Total Time": 592.601,              "Actual Rows": 2,              "Actual Loops": 3,              "Group Key": ["l_returnflag", "l_linestatus"],              "Shared Hit Blocks": 208,              "Shared Read Blocks": 112504,              "Shared Dirtied Blocks": 0,              "Shared Written Blocks": 0,              "Local Hit Blocks": 0,              "Local Read Blocks": 0,              "Local Dirtied Blocks": 0,              "Local Written Blocks": 0,              "Temp Read Blocks": 0,              "Temp Written Blocks": 0,              "Workers": [              ],              "Plans": [                {                  "Node Type": "Sort",                  "Parent Relationship": "Outer",                  "Parallel Aware": "False",                  "Async Capable": "False",                  "Startup Cost": 143867.59,                  "Total Cost": 143868.20,                  "Plan Rows": 246,                  "Plan Width": 25,                  "Actual Startup Time": 592.578,                  "Actual Total Time": 592.578,                  "Actual Rows": 19,                  "Actual Loops": 3,                  "Sort Key": ["l_returnflag", "l_linestatus"],                  "Sort Method": "quicksort",                  "Sort Space Used": 26,                  "Sort Space Type": "Memory",                  "Shared Hit Blocks": 208,                  "Shared Read Blocks": 112504,                  "Shared Dirtied Blocks": 0,                  "Shared Written Blocks": 0,                  "Local Hit Blocks": 0,                  "Local Read Blocks": 0,                  "Local Dirtied Blocks": 0,                  "Local Written Blocks": 0,                  "Temp Read Blocks": 0,                  "Temp Written Blocks": 0,                  "Workers": [                    {                      "Worker Number": 0,                      "Sort Method": "quicksort",                      "Sort Space Used": 25,                      "Sort Space Type": "Memory"                    },                    {                      "Worker Number": 1,                      "Sort Method": "quicksort",                      "Sort Space Used": 25,                      "Sort Space Type": "Memory"                    }                  ],                  "Plans": [                    {                      "Node Type": "Seq Scan",                      "Parent Relationship": "Outer",                      "Parallel Aware": "true",                      "Async Capable": "False",                      "Relation Name": "lineitem",                      "Alias": "lineitem",                      "Startup Cost": 0.00,                      "Total Cost": 143857.82,                      "Plan Rows": 246,                      "Plan Width": 25,                      "Actual Startup Time": 25.821,                      "Actual Total Time": 592.493,                      "Actual Rows": 19,                      "Actual Loops": 3,                      "Filter": "(l_shipdate <= '1992-01-03'::date)",                      "Rows Removed by Filter": 2000386,                      "Shared Hit Blocks": 96,                      "Shared Read Blocks": 112504,                      "Shared Dirtied Blocks": 0,                      "Shared Written Blocks": 0,                      "Local Hit Blocks": 0,                      "Local Read Blocks": 0,                      "Local Dirtied Blocks": 0,                      "Local Written Blocks": 0,                      "Temp Read Blocks": 0,                      "Temp Written Blocks": 0,                      "Workers": [                      ]                    }                  ]                }              ]            }          ]        }      ]    },    "Planning": {      "Shared Hit Blocks": 102,      "Shared Read Blocks": 9,      "Shared Dirtied Blocks": 0,      "Shared Written Blocks": 0,      "Local Hit Blocks": 0,      "Local Read Blocks": 0,      "Local Dirtied Blocks": 0,      "Local Written Blocks": 0,      "Temp Read Blocks": 0,      "Temp Written Blocks": 0    },    "Planning Time": 1.420,    "Triggers": [    ],    "Execution Time": 595.961  }]}
p_y={"day":17,"cutoff_date":"1992-01-19","started_at":"2025-09-29 14:50:05,3N","finished_at":"2025-09-29 14:50:06,3N","plan":[  {    "Plan": {      "Node Type": "Aggregate",      "Strategy": "Sorted",      "Partial Mode": "Finalize",      "Parallel Aware": False,      "Async Capable": False,      "Startup Cost": 145205.36,      "Total Cost": 145416.84,      "Plan Rows": 6,      "Plan Width": 236,      "Actual Startup Time": 158.718,      "Actual Total Time": 159.052,      "Actual Rows": 2,      "Actual Loops": 1,      "Group Key": ["l_returnflag", "l_linestatus"],      "Shared Hit Blocks": 1744,      "Shared Read Blocks": 110968,      "Shared Dirtied Blocks": 0,      "Shared Written Blocks": 0,      "Local Hit Blocks": 0,      "Local Read Blocks": 0,      "Local Dirtied Blocks": 0,      "Local Written Blocks": 0,      "Temp Read Blocks": 0,      "Temp Written Blocks": 0,      "Plans": [        {          "Node Type": "Gather Merge",          "Parent Relationship": "Outer",          "Parallel Aware": False,          "Async Capable": False,          "Startup Cost": 145205.36,          "Total Cost": 145416.28,          "Plan Rows": 12,          "Plan Width": 236,          "Actual Startup Time": 158.425,          "Actual Total Time": 159.038,          "Actual Rows": 6,          "Actual Loops": 1,          "Workers Planned": 2,          "Workers Launched": 2,          "Shared Hit Blocks": 1744,          "Shared Read Blocks": 110968,          "Shared Dirtied Blocks": 0,          "Shared Written Blocks": 0,          "Local Hit Blocks": 0,          "Local Read Blocks": 0,          "Local Dirtied Blocks": 0,          "Local Written Blocks": 0,          "Temp Read Blocks": 0,          "Temp Written Blocks": 0,          "Plans": [            {              "Node Type": "Aggregate",              "Strategy": "Sorted",              "Partial Mode": "Partial",              "Parent Relationship": "Outer",              "Parallel Aware": False,              "Async Capable": False,              "Startup Cost": 144205.34,              "Total Cost": 144414.88,              "Plan Rows": 6,              "Plan Width": 236,              "Actual Startup Time": 156.879,              "Actual Total Time": 157.123,              "Actual Rows": 2,              "Actual Loops": 3,              "Group Key": ["l_returnflag", "l_linestatus"],              "Shared Hit Blocks": 1744,              "Shared Read Blocks": 110968,              "Shared Dirtied Blocks": 0,              "Shared Written Blocks": 0,              "Local Hit Blocks": 0,              "Local Read Blocks": 0,              "Local Dirtied Blocks": 0,              "Local Written Blocks": 0,              "Temp Read Blocks": 0,              "Temp Written Blocks": 0,              "Workers": [              ],              "Plans": [                {                  "Node Type": "Sort",                  "Parent Relationship": "Outer",                  "Parallel Aware": False,                  "Async Capable": False,                  "Startup Cost": 144205.34,                  "Total Cost": 144219.30,                  "Plan Rows": 5584,                  "Plan Width": 25,                  "Actual Startup Time": 156.626,                  "Actual Total Time": 156.651,                  "Actual Rows": 1187,                  "Actual Loops": 3,                  "Sort Key": ["l_returnflag", "l_linestatus"],                  "Sort Method": "quicksort",                  "Sort Space Used": 105,                  "Sort Space Type": "Memory",                  "Shared Hit Blocks": 1744,                  "Shared Read Blocks": 110968,                  "Shared Dirtied Blocks": 0,                  "Shared Written Blocks": 0,                  "Local Hit Blocks": 0,                  "Local Read Blocks": 0,                  "Local Dirtied Blocks": 0,                  "Local Written Blocks": 0,                  "Temp Read Blocks": 0,                  "Temp Written Blocks": 0,                  "Workers": [                    {                      "Worker Number": 0,                      "Sort Method": "quicksort",                      "Sort Space Used": 103,                      "Sort Space Type": "Memory"                    },                    {                      "Worker Number": 1,                      "Sort Method": "quicksort",                      "Sort Space Used": 102,                      "Sort Space Type": "Memory"                    }                  ],                  "Plans": [                    {                      "Node Type": "Seq Scan",                      "Parent Relationship": "Outer",                      "Parallel Aware": True,                      "Async Capable": False,                      "Relation Name": "lineitem",                      "Alias": "lineitem",                      "Startup Cost": 0.00,                      "Total Cost": 143857.82,                      "Plan Rows": 5584,                      "Plan Width": 25,                      "Actual Startup Time": 0.351,                      "Actual Total Time": 156.168,                      "Actual Rows": 1187,                      "Actual Loops": 3,                      "Filter": "(l_shipdate <= '1992-01-19'::date)",                      "Rows Removed by Filter": 1999218,                      "Shared Hit Blocks": 1632,                      "Shared Read Blocks": 110968,                      "Shared Dirtied Blocks": 0,                      "Shared Written Blocks": 0,                      "Local Hit Blocks": 0,                      "Local Read Blocks": 0,                      "Local Dirtied Blocks": 0,                      "Local Written Blocks": 0,                      "Temp Read Blocks": 0,                      "Temp Written Blocks": 0,                      "Workers": [                      ]                    }                  ]                }              ]            }          ]        }      ]    },    "Planning": {      "Shared Hit Blocks": 111,      "Shared Read Blocks": 0,      "Shared Dirtied Blocks": 0,      "Shared Written Blocks": 0,      "Local Hit Blocks": 0,      "Local Read Blocks": 0,      "Local Dirtied Blocks": 0,      "Local Written Blocks": 0,      "Temp Read Blocks": 0,      "Temp Written Blocks": 0    },    "Planning Time": 0.165,    "Triggers": [    ],    "Execution Time": 159.089  }]}
p_z={"day":1,"cutoff_date":"1992-01-03","started_at":"2025-09-29 14:50:01,3N","finished_at":"2025-09-29 14:50:02,3N","plan":[  {    "Plan": {      "Node Type": "Aggregate",      "Strategy": "Sorted",      "Partial Mode": "Finalize",      "Parallel Aware": False,      "Async Capable": "False",      "Startup Cost": 144867.61,      "Total Cost": 144878.91,      "Plan Rows": 6,      "Plan Width": 236,      "Actual Startup Time": 595.333,      "Actual Total Time": 595.902,      "Actual Rows": 2,      "Actual Loops": 1,      "Group Key": ["l_returnflag", "l_linestatus"],      "Shared Hit Blocks": 208,      "Shared Read Blocks": 112504,      "Shared Dirtied Blocks": 0,      "Shared Written Blocks": 0,      "Local Hit Blocks": 0,      "Local Read Blocks": 0,      "Local Dirtied Blocks": 0,      "Local Written Blocks": 0,      "Temp Read Blocks": 0,      "Temp Written Blocks": 0,      "Plans": [        {          "Node Type": "Gather Merge",          "Parent Relationship": "Outer",          "Parallel Aware": "False",          "Async Capable": "False",          "Startup Cost": 144867.61,          "Total Cost": 144878.36,          "Plan Rows": 12,          "Plan Width": 236,          "Actual Startup Time": 595.295,          "Actual Total Time": 595.868,          "Actual Rows": 6,          "Actual Loops": 1,          "Workers Planned": 2,          "Workers Launched": 2,          "Shared Hit Blocks": 208,          "Shared Read Blocks": 112504,          "Shared Dirtied Blocks": 0,          "Shared Written Blocks": 0,          "Local Hit Blocks": 0,          "Local Read Blocks": 0,          "Local Dirtied Blocks": 0,          "Local Written Blocks": 0,          "Temp Read Blocks": 0,          "Temp Written Blocks": 0,          "Plans": [            {              "Node Type": "Aggregate",              "Strategy": "Sorted",              "Partial Mode": "Partial",              "Parent Relationship": "Outer",              "Parallel Aware": "False",              "Async Capable": "False",              "Startup Cost": 143867.59,              "Total Cost": 143876.95,              "Plan Rows": 6,              "Plan Width": 236,              "Actual Startup Time": 592.593,              "Actual Total Time": 592.601,              "Actual Rows": 2,              "Actual Loops": 3,              "Group Key": ["l_returnflag", "l_linestatus"],              "Shared Hit Blocks": 208,              "Shared Read Blocks": 112504,              "Shared Dirtied Blocks": 0,              "Shared Written Blocks": 0,              "Local Hit Blocks": 0,              "Local Read Blocks": 0,              "Local Dirtied Blocks": 0,              "Local Written Blocks": 0,              "Temp Read Blocks": 0,              "Temp Written Blocks": 0,              "Workers": [              ],              "Plans": [                {                  "Node Type": "Sort",                  "Parent Relationship": "Outer",                  "Parallel Aware": "False",                  "Async Capable": "False",                  "Startup Cost": 143867.59,                  "Total Cost": 143868.20,                  "Plan Rows": 246,                  "Plan Width": 25,                  "Actual Startup Time": 592.578,                  "Actual Total Time": 592.578,                  "Actual Rows": 19,                  "Actual Loops": 3,                  "Sort Key": ["l_returnflag", "l_linestatus"],                  "Sort Method": "quicksort",                  "Sort Space Used": 26,                  "Sort Space Type": "Memory",                  "Shared Hit Blocks": 208,                  "Shared Read Blocks": 112504,                  "Shared Dirtied Blocks": 0,                  "Shared Written Blocks": 0,                  "Local Hit Blocks": 0,                  "Local Read Blocks": 0,                  "Local Dirtied Blocks": 0,                  "Local Written Blocks": 0,                  "Temp Read Blocks": 0,                  "Temp Written Blocks": 0,                  "Workers": [                    {                      "Worker Number": 0,                      "Sort Method": "quicksort",                      "Sort Space Used": 25,                      "Sort Space Type": "Memory"                    },                    {                      "Worker Number": 1,                      "Sort Method": "quicksort",                      "Sort Space Used": 25,                      "Sort Space Type": "Memory"                    }                  ],                  "Plans": [                    {                      "Node Type": "Seq Scan",                      "Parent Relationship": "Outer",                      "Parallel Aware": "true",                      "Async Capable": "False",                      "Relation Name": "lineitem",                      "Alias": "lineitem",                      "Startup Cost": 0.00,                      "Total Cost": 143857.82,                      "Plan Rows": 246,                      "Plan Width": 25,                      "Actual Startup Time": 25.821,                      "Actual Total Time": 592.493,                      "Actual Rows": 19,                      "Actual Loops": 3,                      "Filter": "(l_shipdate <= '1992-01-03'::date)",                      "Rows Removed by Filter": 2000386,                      "Shared Hit Blocks": 96,                      "Shared Read Blocks": 112504,                      "Shared Dirtied Blocks": 0,                      "Shared Written Blocks": 0,                      "Local Hit Blocks": 0,                      "Local Read Blocks": 0,                      "Local Dirtied Blocks": 0,                      "Local Written Blocks": 0,                      "Temp Read Blocks": 0,                      "Temp Written Blocks": 0,                      "Workers": [                      ]                    }                  ]                }              ]            }          ]        }      ]    },    "Planning": {      "Shared Hit Blocks": 102,      "Shared Read Blocks": 9,      "Shared Dirtied Blocks": 0,      "Shared Written Blocks": 0,      "Local Hit Blocks": 0,      "Local Read Blocks": 0,      "Local Dirtied Blocks": 0,      "Local Written Blocks": 0,      "Temp Read Blocks": 0,      "Temp Written Blocks": 0    },    "Planning Time": 1.420,    "Triggers": [    ],    "Execution Time": 595.961  }]}
#p_x and p_z are identical plans for testing purpose

# CLI Driver
def compare_plans(plan_a: dict, plan_b: dict) -> dict:
    """
    Compares two PostgreSQL EXPLAIN (ANALYZE, FORMAT JSON) plans and
    summarizes plan equality, runtime difference, and filter selectivity difference.
    """

    # --- Plan identity ---
    norm_a = normalizePlan(plan_a)
    norm_b = normalizePlan(plan_b)

    hash_a = planFingerprint(norm_a)
    hash_b = planFingerprint(norm_b)

    same_plan = matcher(hash_a, hash_b)

    # --- Runtime ---
    runtime_a = extractRuntime(plan_a)
    runtime_b = extractRuntime(plan_b)
    runtime_diff = abs(runtime_a - runtime_b)

    # --- Filter selectivity ---
    stats_a = extractFilterStats(plan_a)
    stats_b = extractFilterStats(plan_b)

    sel_a = stats_a.get("selectivity")
    sel_b = stats_b.get("selectivity")

    if sel_a is not None and sel_b is not None:
        selectivity_diff = abs(sel_a - sel_b)
    else:
        selectivity_diff = None

    return {
        "same_plan": same_plan,
        "runtime_a_ms": runtime_a,
        "runtime_b_ms": runtime_b,
        "runtime_diff_ms": runtime_diff,
        "selectivity_a": sel_a,
        "selectivity_b": sel_b,
        "selectivity_diff": selectivity_diff,
        "filters_a": stats_a.get("filters", []),
        "filters_b": stats_b.get("filters", []),
    }

def print_comparison(result: dict):
    print("\nPlan A vs Plan B")
    print("----------------")
    print(f"Same plan: {'YES' if result['same_plan'] else 'NO'}")
    print(f"Runtime difference: {result['runtime_diff_ms']:.3f} ms")

    if result["selectivity_diff"] is not None:
        print(f"Filter selectivity difference: {result['selectivity_diff']:.6e}")
    else:
        print("Filter selectivity difference: N/A")

    
# def run_menu():
#     plan3 = p_x["plan"]
#     simplified_3 = normalizePlan(plan3)
#     # fp3 = plan_fingerprint(simplified_3)
#     print("\n\n\n\n\n\n")
#     print("PLAN:")
#     pprint.pprint(plan3)
#     print("\n\n\n\n\n\n")
#     print("\nSIMPLIFIED PLAN:")
#     pprint.pprint(simplified_3)

    
#     runtime = extractRuntime(p_x["plan"])
#     print("Execution Runtime (ms):", runtime)

#     stats = extractFilterStats(p_x["plan"])
#     print("Filter stats:")
#     pprint.pprint(stats)


# if __name__ == "__main__":
#     run_menu()

if __name__ == "__main__":
    plan_a = p_x["plan"]
    plan_b = p_y["plan"]

    result = compare_plans(plan_a, plan_b)
    print_comparison(result)


    print("\n\nDetailed Plan A Stats:")
    runtime = extractRuntime(p_x["plan"])
    print("Execution Runtime (ms):", runtime)

    stats = extractFilterStats(p_x["plan"])
    print("Filter stats:")
    pprint.pprint(stats)

    print("\n\nDetailed Plan B Stats:")
    runtime = extractRuntime(p_y["plan"])
    print("Execution Runtime (ms):", runtime)

    stats = extractFilterStats(p_y["plan"])
    print("Filter stats:")
    pprint.pprint(stats)