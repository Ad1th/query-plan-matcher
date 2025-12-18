import json
import pprint
import hashlib


def simplifier(plan_json: dict) -> dict: #ensuring input is a dictionary
    #receive JSON
    #Identify Root operation
    #Decide the kind of operation
    #Decide how operation is executed (kind of algo)
    #extract relation name if scan, else null
    #remove unnecessary details
    #identify child operators
    #construct simplified tree of query plan
    #return simplified JSON

    # Step 1: unwrap Postgres EXPLAIN JSON if needed
    if "Plan" in plan_json:
        node = plan_json["Plan"]
    else:
        node = plan_json

    # Step 2: identify raw operator type
    node_type = node.get("Node Type")

    # Canonical fields
    op = None
    algo = None
    relation = None
    children = []

    # Step 3: map raw operator to canonical op and algo
    if node_type == "Seq Scan":
        op = "Scan"
        algo = "SeqScan"
        relation = node.get("Relation Name")

    elif node_type == "HashAggregate":
        op = "Aggregate"
        algo = "HashAggregate"

    elif node_type == "Sort":
        op = "Sort"
        algo = "InMemorySort"

    elif node_type == "Index Scan":
        op = "Scan"
        algo = "IndexScan"
        relation = node.get("Relation Name")


    else:
        raise ValueError(f"Unsupported node type: {node_type}")

    # Step 4: identify and process child operators
    if "Plans" in node:
        for child in node["Plans"]:
            children.append(simplifier(child))

    # Step 5: construct and return canonical node
    return {
        "op": op,
        "algo": algo,
        "relation": relation,
        "children": children
    }
    # print(node_type, op, algo, relation)
    

# plan_json = [
#   {
#     "Plan": {
#       "Node Type": "Sort",
#       "Plans": [
#         {
#           "Node Type": "HashAggregate",
#           "Plans": [
#             {
#               "Node Type": "Seq Scan",
#               "Relation Name": "lineitem"
#             }
#           ]
#         }
#       ]
#     }
#   }
# ]

# simplified = simplifier(plan_json[0])


# pprint.pprint(simplified)

def plan_fingerprint(simplified_plan: dict) -> str:
    #Accept plan by F1
    #Serialize plan in a consistent and deterministic order
    #Generate a hash of the serialized plan
    #return the hash as fingerprint of the plan


    # Step 1: deterministic serialization
    serialized = json.dumps(
        simplified_plan,
        sort_keys=True,
        separators=(",", ":")
    )
    # return serialized
    # Step 2: generate hash
    hash_object = hashlib.sha256(serialized.encode())
    fingerprint = hash_object.hexdigest()
    return fingerprint
   

# plan_fingerprint_value = plan_fingerprint(simplified)
# print("Plan Fingerprint:", plan_fingerprint_value)


def matcher(fp1: str, fp2: str) -> bool:
    #Accept fingerprint of current plan
    #Compare the 2 fingerprint values
    #if fingerprints are equal, classify plans as equivalent, else different
    #return equivalence result

    return fp1 == fp2




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
    print("Plans are equivalent?" , is_same)


if __name__ == "__main__":
    run_menu()


