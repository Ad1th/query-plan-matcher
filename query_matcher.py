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
    pass

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
