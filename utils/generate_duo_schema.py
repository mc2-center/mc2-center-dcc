"""
generate_duo_schema.py
This script generates a JSON schema defining access requirements based on a CSV file containing annotation-based access restrictions.

Usage:
python generate_duo_schema.py [CSV file path] [Output JSON schema file path] [Optional parameters]

author: orion.banks
"""

import pandas as pd
import json
import argparse
from collections import OrderedDict

def build_condition(row: pd.Series, col_names: list, multi_condition: bool) -> dict:
    """
    Builds a conditional schema segment based on a row from the CSV file.
    Args:
        row (pd.Series): A row from the DataFrame representing access restrictions.
        col_names (list): List of additional column names to consider for conditions.
        multi_condition (bool): Flag indicating if multiple conditions should be included.
    Returns:
        dict: A dictionary representing the conditional schema segment.
    """
    condition = {
        "if": {
            "properties": {
                "dataUseModifiers": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    },  
                    "contains": { "const": row["dataUseModifiers"] }
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
                    "contains": { "const": int(row["accessRequirementId"]) }
                }
            }
        }
    }

    # Optional conditional fields
    additional_conditions = {}
    required_fields = ["dataUseModifiers"]

    if multi_condition is not None:
        if "activatedByAttribute" in row and pd.notna(row["activatedByAttribute"]):
            additional_conditions[row["activatedByAttribute"]] = { "type": "array", "items": { "type": "string" }, "contains": { "const": row["activationValue"] } }
            required_fields.append(row["activatedByAttribute"])

        for col in col_names:
            if col in row and pd.notna(row[col]):
                additional_conditions[col] = { "type": "array", "items": { "type": "string" }, "contains": { "const": row[col] } }
                required_fields.append(col)

    if additional_conditions:
        condition["if"]["properties"].update(additional_conditions)
        condition["if"]["required"] = required_fields

    return condition


def generate_json_schema(csv_path, output_path, title, version, org_id, grant_id, multi_condition, study_id, grant_col, study_col, data_type, data_col, species_type, species_col, access_requirement):
    """
    Generates a JSON schema defining access requirements based on a CSV file.
    Args:
        csv_path (str): Path to the input CSV file.
        output_path (str): Path to the output JSON schema file.
        title (str): Title of the JSON schema.
        version (str): Version of the JSON schema.
        org_id (str): Organization ID for the $id field.
        grant_id (str): Grant number to filter conditions.
        multi_condition (bool): Flag to generate schema with multiple conditions.
        study_id (str): Study ID to filter conditions.
        grant_col (str): Column name for grant identifier.
        study_col (str): Column name for study identifier.
        data_type (str): Data type to filter conditions.
        data_col (str): Column name for data type identifier.
        species_type (str): Species type to filter conditions.
        species_col (str): Column name for species type identifier.
        access_requirement (str): Access requirement ID to filter conditions.
    Returns:
        None
    """
    df = pd.read_csv(csv_path, header=0, dtype=str)

    conditions = []
    base_conditions = ["dataUseModifiers", "accessRequirementId", "activatedByAttribute", "activationValue", "entityIdList"]
    col_names = df.columns.tolist()
    col_names = [col for col in col_names if col not in base_conditions]
    for _, row in df.iterrows():
        if access_requirement is not None and row["accessRequirementId"] != access_requirement:
            continue
        if grant_id != "Project" and row[grant_col] != grant_id:
            continue
        if study_id is not None and row[study_col] != study_id:
            continue
        if data_type is not None and row[data_col] != data_type:
            continue
        if species_type is not None and row[species_col] != species_type:
            continue
        condition = build_condition(row, col_names, multi_condition)
        conditions.append(condition)

    schema = OrderedDict({
        "$schema": "http://json-schema.org/draft-07/schema",
        "title": title,
        "$id": f"{org_id}-{grant_id}-{study_id + '-' if study_id is not None else ''}{data_type + '-' if data_type is not None else ''}{species_type + '-' if species_type is not None else ''}{'mc-' if multi_condition is not None else ''}AccessRequirementSchema-{access_requirement + '-' if access_requirement is not None else ''}{version}",
        "description": f"Auto-generated schema that defines access requirements for biomedical data. Organization: {org_id}, Grant number or Project designation: {grant_id}, Study ID: {study_id if study_id else 'N/A'}, Data Type: {data_type if data_type else 'N/A'}, Species Type: {species_type if species_type else 'N/A'}, Multi-condition: {'Yes' if multi_condition else 'No'}, Selected Access Requirement ID: {access_requirement if access_requirement else 'N/A'}",
        "allOf": conditions
    })

    with open(output_path, 'w') as f:
        json.dump(schema, f, indent=2)

    print(f"✅ JSON Schema written to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Access Requirement JSON Schema from Data Dictionary CSV")
    parser.add_argument("csv_path", help="Path to the data_dictionary.csv. See and example at https://github.com/Sage-Bionetworks/governanceDUO/blob/main/access_requirement_JSON/README.md")
    parser.add_argument("output_path", help="Path to output directory for the JSON schema")
    parser.add_argument("-t", "--title", default="AccessRequirementSchema", help="Schema title")
    parser.add_argument("-v", "--version", default="v1.0.0", help="Schema version")
    parser.add_argument("-o", "--org_id", default="DCC", help="Organization ID for $id field")
    parser.add_argument("-a", "--access_requirement", default=None, help="Access requirement ID to select conditions for from reference table. If nothing is provided, the JSON schema will include all applicable conditions listed in the input table.")
    parser.add_argument("-g", "--grant_id", help="Grant number to select conditions for from reference table. If nothing is provided, the JSON schema will include all conditions listed in the input table.", default="Project")
    parser.add_argument("-m", "--multi_condition", help="Boolean. Generate schema with multiple conditions defined in the CSV", action="store_true", default=None)
    parser.add_argument("-gc", "--grant_col", help="Name of the column in the DCC AR data dictionary that will contain the identifier for the grant", default="grantNumber")
    parser.add_argument("-s", "--study_id", help="Study ID to select conditions for from reference table. If nothing is provided, the JSON schema will include all applicable studies listed in the input table.", default=None)
    parser.add_argument("-sc", "--study_col", help="Name of the column in the DCC AR data dictionary that will contain the identifier for the study", default="studyKey")
    parser.add_argument("-d", "--data_type", help="Data type to select conditions for from reference table. If nothing is provided, the JSON schema will include all applicable data types listed in the input table.", default=None)
    parser.add_argument("-dc", "--data_col", help="Name of the column in the DCC AR data dictionary that will contain the identifier for the data type", default="dataType")
    parser.add_argument("-p", "--species_type", help="Species to select conditions for from reference table. If nothing is provided, the JSON schema will include all applicable species listed in the input table.", default=None)
    parser.add_argument("-pc", "--species_col", help="Name of the column in the DCC AR data dictionary that will contain the identifier for the species", default="speciesType")

    args = parser.parse_args()

    output_path = "".join([args.output_path, "/", args.org_id, ".", "AccessRequirement-", f"{args.grant_id}-" if args.grant_id else "Project-", f"{args.study_id}-" if args.study_id else "", f"{args.data_type}-" if args.data_type else "", f"{args.species_type}-" if args.species_type else "", "mc-" if args.multi_condition else "", f"{args.access_requirement}-" if args.access_requirement else "", args.version, "-schema.json"])
    generate_json_schema(args.csv_path, output_path, args.title, args.version, args.org_id, grant_id = args.grant_id, multi_condition=args.multi_condition, study_id = args.study_id, grant_col=args.grant_col, study_col=args.study_col, data_type = args.data_type, data_col=args.data_col, species_type = args.species_type, species_col=args.species_col, access_requirement=args.access_requirement)
