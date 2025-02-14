"""
processing-splits.py

Runs the Python script `processing-splits.py` to process split files from the specified output folder. 
Adds missing columns required to match the schema, truncates any columns with 400+ words, and adds "Read more on Pubmed"

author: aditi.gopalan

"""

import os
import pandas as pd
import sys


def process_csv(file_path):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path, header=0)
    data = str(df.iloc[0,0])
    resource = data[:-4]

    # 1. Change column names
    col_mapping = [
        (f"{resource} Grant Number", "GrantView Key"),
        ("Publication TumorType", "Publication Tumor Type")
        ]
    
    for old_col, new_col in col_mapping:
        if old_col in df.columns:
            df = df.rename(columns={old_col: new_col})

    # 2. Add "PublicationView_id" as a column if not present, and fill it with values from "Pubmed Id" column
    if "PublicationView_id" not in df.columns and "Pubmed Id" in df.columns:
        df["PublicationView_id"] = df["Pubmed Id"]

    # 3. Add Study Key column
    if "Study Key" not in df.columns:
        df["Study Key"] = ""

    # 4. Drop 'Publication Theme Name' and 'Publication Consortium Name' columns
    columns_to_drop = ["Publication Theme Name", "Publication Consortium Name"]
    df = df.drop(columns=columns_to_drop, errors="ignore")

    # 5. Modify each column content as per the second script
    for column in df.columns:
        if column in df.columns:
            df[column] = df[column].apply(
                lambda x: (
                    x[:400] + "(Read more on Pubmed)"
                    if isinstance(x, str) and len(x) > 500
                    else x
                )
            )
    # 6. Reorder columns to match the table order
    if data == "PublicationView":
        col_order = [
            "Component",
            "PublicationView_id",
            "GrantView Key",
            "Study Key",
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
            "Publication Dataset Alias"
            ]

    elif data == "DatasetView":
        col_order = [
            "Component",
            "DatasetView_id",
            "GrantView Key",
            "Study Key",
            "PublicationView Key",
            "Dataset Name",
            "Dataset Alias",
            "Dataset Description",
            "Dataset Design",
            "Dataset Assay",
            "Dataset Species",
            "Dataset Tumor Type",
            "Dataset Tissue",
            "Dataset Url",
            "Dataset Doi",
            "Dataset File Formats",
            "Data Use Codes"
            ]

    elif data == "ToolView":
        col_order = [
            "Component",
            "ToolView_id",
            "GrantView Key",
            "Study Key",
            "DatasetView Key",
            "PublicationView Key",
            "Tool Name",
            "Tool Description",
            "Tool Homepage",
            "Tool Version",
            "Tool Operation",
            "Tool Input Data",
            "Tool Output Data",
            "Tool Input Format",
            "Tool Output Format",
            "Tool Function Note",
            "Tool Cmd",
            "Tool Type",
            "Tool Topic",
            "Tool Operating System",
            "Tool Language",
            "Tool License",
            "Tool Cost",
            "Tool Accessibility",
            "Tool Download Url",
            "Tool Download Type",
            "Tool Download Note",
            "Tool Download Version",
            "Tool Documentation Url",
            "Tool Documentation Type",
            "Tool Documentation Note",
            "Tool Link Url",
            "Tool Link Type",
            "Tool Link Note",
            "Tool Doi",
            "Tool Date Last Modified",
            "Tool Release Date",
            "Tool Package Dependencies",
            "Tool Package Dependencies Present",
            "Tool Compute Requirements",
            "Tool Entity Name",
            "Tool Entity Type",
            "Tool Entity Role"
            ]

    elif data == "EducationalResource":
        col_order = [
            "Component",
            "EducationalResource_id",
            "GrantView Key",
            "Study Key",
            "DatasetView Key",
            "PublicationView Key",
            "ToolView Key",
            "Resource Title",
            "Resource Link",
            "Resource Doi",
            "Resource Topic",
            "Resource Activity Type",
            "Resource Primary Format",
            "Resource Intended Use",
            "Resource Primary Audience",
            "Resource Educational Level",
            "Resource Description",
            "Resource Origin Institution",
            "Resource Language",
            "Resource Contributors",
            "Resource Secondary Topic",
            "Resource License",
            "Resource Use Requirements",
            "Resource Alias",
            "Resource Internal Identifier",
            "Resource Media Accessibility",
            "Resource Access Hazard",
            "Resource Dataset Alias",
            "Resource Tool Link"
        ]

    df = df[col_order]

    # Save the modified DataFrame back to the CSV file
    df.to_csv(file_path, index=False)


if __name__ == "__main__":
    # Get the folder path from command-line arguments, defaulting to the current working directory
    folder_path = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()

    # Iterate over all files in the specified directory with a .csv extension
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(folder_path, filename)

            # Process each CSV file
            process_csv(file_path)

    print("Processing completed.")
