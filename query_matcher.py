import json



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
    print(node_type, op, algo, relation)
    

plan_json = [
  {
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
  }
]

simplified = simplifier(plan_json[0])

import pprint
pprint.pprint(simplified)

def plan_fingerprint(simplified_plan: dict):
    #Accept plan by F1
    #Serialize plan in a consistent and deterministic order
    #Generate a hash of the serialized plan
    #return the hash as fingerprint of the plan
    pass

def matcher(fp1,fp2):
    #Accept fingerprint of current plan
    #Compare the 2 fingerprint values
    #if fingerprints are equal, classify plans as equivalent, else different
    #return equivalence result
    pass
