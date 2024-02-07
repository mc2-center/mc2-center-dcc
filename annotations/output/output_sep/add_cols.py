import os
import pandas as pd
import sys

def process_csv(file_path, column_type):
    df = pd.read_csv(file_path)

    column_name_to_replace = f"{column_type} TumorType"
    new_column_name = f"{column_type} Tumor Type"
    if column_name_to_replace in df.columns:
        df = df.rename(columns={column_name_to_replace: new_column_name})

    new_column_view_id = f"{column_type} View_id"
    if new_column_view_id not in df.columns and 'Pubmed Id' in df.columns:
        df[new_column_view_id] = df['Pubmed Id']

    columns_to_drop = [f'{column_type} Theme Name', f'{column_type} Consortium Name']
    df = df.drop(columns=columns_to_drop, errors='ignore')

    for column in df.columns:
        if column in df.columns:
            df[column] = df[column].apply(lambda x: x[:400] + "(Read more on Pubmed)" if isinstance(x, str) and len(x) > 500 else x)

    df.to_csv(file_path, index=False)

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ["Publication", "Dataset", "Tool"]:
        print("Usage: python script.py [Publication | Dataset | Tool]")
        sys.exit(1)

    column_type = sys.argv[1]
    folder_path = os.getcwd()

    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(folder_path, filename)
            process_csv(file_path, column_type)

    print("Processing completed.")



