import pandas as pd
import json
import argparse
from collections import OrderedDict

def build_condition(row):
    """
    Builds a JSON Schema if-then rule based on a row from the data dictionary.
    """
    condition = {
        "if": {
            "properties": {
                "dataUseModifiers": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    },  
                    "contains": { "const": row["DUO_Code"] }
                }
            },
            "required": ["dataUseModifiers"]
        },
        "then": {
            "properties": {
                "_accessRequirementIds": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    },
                    "contains": { "const": int(row["Access_Requirement_ID"]) }
                }
            }
        }
    }

    # Optional conditional fields
    additional_conditions = {}
    required_fields = ["dataUseModifiers"]

    if "Grant_Number" in row and pd.notna(row["Grant_Number"]):
        additional_conditions["grantNumber"] = { "type": "array", "items": { "type": "string" }, "contains": { "const": row["Grant_Number"] } }
        required_fields.append("grantNumber")

    if "Data_Type" in row and pd.notna(row["Data_Type"]):
        additional_conditions["dataType"] = { "type": "array", "items": { "type": "string" }, "contains": { "const": row["Data_Type"] } }
        required_fields.append("dataType")

    if "Activated_By_Attribute" in row and pd.notna(row["Activated_By_Attribute"]):
        additional_conditions[row["Activated_By_Attribute"]] = { "type": "array", "items": { "type": "string" }, "contains": { "const": "True" } }
        required_fields.append(row["Activated_By_Attribute"])

    if additional_conditions:
        condition["if"]["properties"].update(additional_conditions)
        condition["if"]["required"] = required_fields

    return condition


def generate_json_schema(csv_path, output_path, title, version, org_id, grant_id):
    df = pd.read_csv(csv_path)

    conditions = []
    for _, row in df.iterrows():
        if grant_id and row["Grant_Number"] != grant_id:
            continue
        condition = build_condition(row)
        conditions.append(condition)

    schema = OrderedDict({
        "$schema": "http://json-schema.org/draft-07/schema",
        "title": title,
        "$id": f"{org_id}-{grant_id}-AccessRequirementSchema-{version}",
        "description": "Auto-generated schema defining DUO-based access restrictions.",
        "allOf": conditions
    })

    with open(output_path, 'w') as f:
        json.dump(schema, f, indent=2)

    print(f"✅ JSON Schema written to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate DUO JSON Schema from Data Dictionary CSV")
    parser.add_argument("csv_path", help="Path to the data_dictionary.csv")
    parser.add_argument("output_path", help="Path to output directory for the JSON schema")
    parser.add_argument("-t", "--title", default="AccessRequirementSchema", help="Schema title")
    parser.add_argument("-v", "--version", default="v1.0.0", help="Schema version")
    parser.add_argument("-o", "--org_id", default="MC2", help="Organization ID for $id field")
    parser.add_argument("-g", "--grant_id", help="Grant number to select conditions for from reference table")

    args = parser.parse_args()

    output_path = "".join([args.output_path, "/", args.org_id, ".", "AccessRequirement-", f"{args.grant_id}-" if args.grant_id else "", args.version, "-schema.json"])
    generate_json_schema(args.csv_path, output_path, args.title, args.version, args.org_id, args.grant_id)
