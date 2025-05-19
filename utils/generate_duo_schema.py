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
                "duoCodes": {
                    "type": "array",
                    "contains": { "const": row["DUO_Code"] }
                }
            },
            "required": ["duoCodes"]
        },
        "then": {
            "properties": {
                "_accessRequirementIds": {
                    "type": "array",
                    "contains": { "const": int(row["Access_Requirement_ID"]) }
                }
            }
        }
    }

    # Optional conditional fields
    additional_conditions = {}
    required_fields = ["duoCodes"]

    if "Grant_Number" in row and pd.notna(row["Grant_Number"]):
        additional_conditions["grantNumber"] = { "const": row["Grant_Number"] }
        required_fields.append("grantNumber")

    if "Data_Type" in row and pd.notna(row["Data_Type"]):
        additional_conditions["dataType"] = { "const": row["Data_Type"] }
        required_fields.append("dataType")

    if additional_conditions:
        condition["if"]["properties"].update(additional_conditions)
        condition["if"]["required"] = required_fields

    return condition


def generate_json_schema(csv_path, output_path, title="DUO Access Schema", version="1.0.0", org_id="MC2-Custom"):
    df = pd.read_csv(csv_path)

    conditions = []
    for _, row in df.iterrows():
        condition = build_condition(row)
        conditions.append(condition)

    schema = OrderedDict({
        "$schema": "http://json-schema.org/draft-07/schema",
        "title": title,
        "$id": f"{org_id}-duoCodeAR-{version}",
        "description": "Auto-generated schema defining DUO-based access restrictions.",
        "allOf": conditions
    })

    with open(output_path, 'w') as f:
        json.dump(schema, f, indent=2)

    print(f"✅ JSON Schema written to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate DUO JSON Schema from Data Dictionary CSV")
    parser.add_argument("csv_path", help="Path to the data_dictionary.csv")
    parser.add_argument("output_path", help="Path to output JSON schema")
    parser.add_argument("--title", default="DUO Access Schema", help="Schema title")
    parser.add_argument("--version", default="1.0.0", help="Schema version")
    parser.add_argument("--org_id", default="MC2-Custom", help="Organization ID for $id field")

    args = parser.parse_args()
    generate_json_schema(args.csv_path, args.output_path, args.title, args.version, args.org_id)
