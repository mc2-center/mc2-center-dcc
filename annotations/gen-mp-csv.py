import os
import csv
import pandas as pd
import sys

def get_xlsx_files_in_folder(folder_path):
    xlsx_files = []
    for file in os.listdir(folder_path):
        if file.endswith("_dataset.csv"): # CHANGE HERE
            xlsx_files.append(os.path.join(folder_path, file))
    return xlsx_files

def extract_ca_number(file_path):
    file_name = os.path.basename(file_path)
    # Assuming the file name format is "CA****_publication.xlsx"
    ca_number = file_name.split('_')[0][2:]
    print(f"Extracted CA number: {ca_number}")
    return f"CA{ca_number}"

def get_folder_id_and_grant_id_from_csv(csv_file, ca_number):
    df = pd.read_csv(csv_file)
    
    column_name = 'folderIdDatasets'  # CHANGE HERE
    grant_id_column_name = 'grantId'  

    if column_name in df.columns and grant_id_column_name in df.columns:
        print(f"Successfully read CSV file: {csv_file}")
        
        match = df[df['grantNumber'] == ca_number]
        if not match.empty:
            folder_id = match[column_name].values[0]
            grant_id = match[grant_id_column_name].values[0]  
            print(f"Matching {column_name}: {folder_id}, Grant ID: {grant_id}")  
            return folder_id, grant_id
        else:
            print(f"No match found for {column_name}.")
            return None, None
    else:
        print(f"Error: '{column_name}' or '{grant_id_column_name}' column not found in the CSV file: {csv_file}")
        return None, None

def write_file_paths_to_csv(file_paths, output_file, csv_file):
    with open(output_file, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["File Paths", "target_id", "grantId"]) 

        for file_path in file_paths:
            ca_number = extract_ca_number(file_path)
            folder_id, grant_id = get_folder_id_and_grant_id_from_csv(csv_file, ca_number) 
            csv_writer.writerow([file_path, folder_id, grant_id])  

def main(folder_path, output_csv_file, csv_file):
    xlsx_files = get_xlsx_files_in_folder(folder_path)
    write_file_paths_to_csv(xlsx_files, output_csv_file, csv_file)

    print("CSV file with file paths, target IDs, and grant IDs generated.")  

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python script_name.py folder_path output_csv_file csv_file")
    else:
        folder_path = sys.argv[1]
        output_csv_file = sys.argv[2]
        csv_file = sys.argv[3]
        main(folder_path, output_csv_file, csv_file)
