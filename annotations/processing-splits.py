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
    df = pd.read_csv(file_path)

    # 1. Change column name from "Publication TumorType" to "Publication Tumor Type"
    if "Publication TumorType" in df.columns:
        df = df.rename(columns={"Publication TumorType": "Publication Tumor Type"})

    # 2. Add "PublicationView_id" as a column if not present, and fill it with values from "Pubmed Id" column
    if "PublicationView_id" not in df.columns and "Pubmed Id" in df.columns:
        df.insert(1,'PublicationView_id',df['Pubmed Id'].copy())

    # 3. Drop 'Publication Theme Name' and 'Publication Consortium Name' columns
    columns_to_drop = ["Publication Theme Name", "Publication Consortium Name"]
    df = df.drop(columns=columns_to_drop, errors="ignore")

    # 4. Modify each column content as per the second script
    for column in df.columns:
        if column in df.columns:
            df[column] = df[column].apply(
                lambda x: (
                    x[:400] + "(Read more on Pubmed)"
                    if isinstance(x, str) and len(x) > 500
                    else x
                )
            )
    # Re-order the Dataframe with columns in the same order as the union table
    if "Publication Dataset Alias" in df.columns and "Publication Accessibility" in df.columns:
        col_list = list(df.columns)
        x,y = col_list.index('Publication Dataset Alias'), col_list.index('Publication Accessibility')
        col_list[y], col_list[x] = col_list[x], col_list[y]
        df = df[col_list]

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
