"""Cleanup of merged manifests shared by utils/merge_and_correct_manifests.py and union_qc.py.

author: orion.banks
"""

import json
import re

import pandas as pd

import model_contract
from cells import PENDING

UNKNOWN = "Unknown"


def _enums(prop: dict) -> list[list]:
    """Every enum list of a schema property: its own, its array items' and those of its anyOf/oneOf branches."""
    found = [prop["enum"]] if "enum" in prop else []
    if isinstance(prop.get("items"), dict):
        found += _enums(prop["items"])
    for branch in prop.get("anyOf", []) + prop.get("oneOf", []):
        found += _enums(branch)
    return found


def _fill_value(prop: dict | None) -> str:
    """The value for an empty cell: ``PENDING`` if the property allows it or has no enum, else ``Unknown`` if allowed."""
    enums = _enums(prop) if prop else []
    if not enums or any(PENDING in e for e in enums):
        return PENDING
    return UNKNOWN if any(UNKNOWN in e for e in enums) else PENDING


def fill_empty_cells(updated_database: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """Fill empty cells in the updated database with 'Not Applicable' or, per the component's JSON schema, 'Pending Annotation' or 'Unknown'."""

    cols_to_fill = [
        "Assay",
        "Tumor Type",
        "Tissue",
        "Publication Dataset Alias",
        "PublicationView Key",
        "Dataset Description",
        "Dataset Design",
        "Species",
        "Dataset File Formats",
        "Dataset Data Use Codes",
    ]

    current_cols_to_fill = [col for col in updated_database.columns if col in cols_to_fill]
    if data_type == "EducationalResource":  # a resource's publication link stays empty rather than a placeholder
        current_cols_to_fill = [col for col in current_cols_to_fill if col != "PublicationView Key"]

    labels = model_contract.label_map()
    properties = json.loads(model_contract.fetch(f"json_schemas/{data_type}.json").read_text()).get("properties", {})
    fill_values = {col: _fill_value(properties.get(labels.get(col))) for col in current_cols_to_fill}

    for _,row in updated_database.iterrows():
        for col in current_cols_to_fill:
            if pd.isna(row[col]) or row[col] == "" or re.match(r"^,+", row[col]):
                if data_type == "PublicationView" and row["Publication Accessibility"] == "Open Access":
                    value = "Not Applicable"
                else:
                    value = fill_values[col]
                updated_database.at[row.name, col] = value

    return updated_database


def trim_whitespace(database: pd.DataFrame) -> pd.DataFrame:
    """Remove leading and trailing whitespace from entries in curated columns."""
    
    cols_to_clean = [
        "Assay",
        "Tumor Type",
        "Tissue",
        "Species",
    ]
    
    clean_cols = [col for col in database.columns if col in cols_to_clean]
    
    for _,row in database.iterrows():
        for col in clean_cols:
            database.at[row.name, col] = row[col].strip()

    return database
