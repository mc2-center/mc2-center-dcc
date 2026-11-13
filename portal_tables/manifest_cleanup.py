"""Cleanup of merged manifests shared by utils/merge_and_correct_manifests.py and union_qc.py.

author: orion.banks
"""

import re

import pandas as pd


def fill_empty_cells(updated_database: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """Fill empty cells in the updated database with 'Pending Annotation' or 'Not Applicable'."""
    
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

    for _,row in updated_database.iterrows():
        for col in current_cols_to_fill:
            if pd.isna(row[col]) or row[col] == "" or re.match(r"^,+", row[col]):
                if data_type == "PublicationView" and row["Publication Accessibility"] == "Open Access":
                    value = "Not Applicable"
                else:
                    value = "Pending Annotation"
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
