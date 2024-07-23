"""Clean and prep MC2 database tables for backpopulation

This script will reorder and modify database table manifest columns
to match the respective View-type schema.
"""

import pandas as pd
import sys


def add_missing_info(
    pubs: pd.DataFrame
) -> pd.DataFrame:
    """Add missing information into table before syncing."""

    prefix = "https://doi.org/"

    pubs["Publication Doi"] = [
        "".join([prefix, doi])
        for doi in pubs["Publication Doi"]
    ]

    return pubs


def clean_table(df: pd.DataFrame, data) -> pd.DataFrame:
    """Clean up the table one final time."""

    # Reorder columns to match the table order.
    if data == "publication":
        col_order = [
            "Component",
            "PublicationView_id",
            "Publication Grant Number",
            "Publication Doi",
            "Publication Journal",
            "Pubmed Id",
            "Pubmed Url",
            "Publication Title",
            "Publication Year",
            "Publication Keywords",
            "Publication Authors",
            "Publication Abstract",
            "Publication Assay",
            "Publication Tumor Type",
            "Publication Tissue",
            "Publication Accessibility",
            "Publication Dataset Alias",
            "entityId",
        ]

    elif data == "dataset":
        col_order = [
            "Component",
            "DatasetView_id",
            "Dataset Pubmed Id",
            "Dataset Grant Number",
            "Dataset Name",
            "Dataset Alias",
            "Dataset Description",
            "Dataset Design",
            "Dataset Assay",
            "Dataset Species",
            "Dataset Tumor Type",
            "Dataset Tissue",
            "Dataset Url",
            "Dataset File Formats",
            "entityId",
        ]
    return df[col_order]


def main():
    """Main function."""
    input = sys.argv[1]
    output = sys.argv[2]
    data = sys.argv[3]
    clean = sys.argv[4]

    manifest = pd.read_csv(input, header=0).fillna("")

    if data == "publication" and clean == "doi":
        database = add_missing_info(manifest)
    else:
        database = manifest
    
    final_database = clean_table(database, data)
    
    print(f"📄 Saving copy of final table to: {output}...")
    
    final_database.to_csv(output, index=False)
    
    print("\n\nDONE ✅")


if __name__ == "__main__":
    main()
