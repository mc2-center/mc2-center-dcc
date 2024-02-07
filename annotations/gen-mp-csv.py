import os
import csv
import pandas as pd
import sys

def get_csv_files_in_folder(folder_path, file_suffix):
    csv_files = []
    for file in os.listdir(folder_path):
        if file.endswith(file_suffix):
            csv_files.append(os.path.join(folder_path, file))
    return csv_files

def extract_ca_number(file_path, file_suffix):
    file_name = os.path.basename(file_path)
    # Assuming the file name format is "CA****_publication.csv" or "CA****_dataset.csv"
    ca_number = file_name.split('_')[0][2:]
    print(f"Extracted CA number: {ca_number}")
    return f"CA{ca_number}"

def get_folder_id_and_grant_id_from_csv(csv_file, ca_number, folder_id_column_name, grant_id_column_name):
    df = pd.read_csv(csv_file)
    
    if folder_id_column_name in df.columns and grant_id_column_name in df.columns:
        print(f"Successfully read CSV file: {csv_file}")
        
        match = df[df['grantNumber'] == ca_number]
        if not match.empty:
            folder_id = match[folder_id_column_name].values[0]
            grant_id = match[grant_id_column_name].values[0]  
            print(f"Matching {folder_id_column_name}: {folder_id}, Grant ID: {grant_id}")  
            return folder_id, grant_id
        else:
            print(f"No match found for {folder_id_column_name}.")
            return None, None
    else:
        print(f"Error: '{folder_id_column_name}' or '{grant_id_column_name}' column not found in the CSV file: {csv_file}")
        return None, None

def write_file_paths_to_csv(file_paths, output_file, csv_file, folder_id_column_name, grant_id_column_name):
    with open(output_file, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["File Paths", folder_id_column_name, grant_id_column_name])

        for file_path in file_paths:
            ca_number = extract_ca_number(file_path, file_suffix)
            folder_id, grant_id = get_folder_id_and_grant_id_from_csv(csv_file, ca_number, folder_id_column_name, grant_id_column_name) 
            csv_writer.writerow([file_path, folder_id, grant_id])  

def main(folder_path, output_csv_file, csv_file, data_type):
    if data_type == "publication":
        file_suffix = "_publication.csv"
        folder_id_column_name = "folderIdPublication"
        grant_id_column_name = "grantIdPublication"
    elif data_type == "dataset":
        file_suffix = "_dataset.csv"
        folder_id_column_name = "folderIdDatasets"
        grant_id_column_name = "grantIdDatasets"
    else:
        print("Invalid data type. Please provide either 'publication' or 'dataset'.")
        return

    csv_files = get_csv_files_in_folder(folder_path, file_suffix)
    write_file_paths_to_csv(csv_files, output_csv_file, csv_file, folder_id_column_name, grant_id_column_name)

    print("CSV file with file paths, target IDs, and grant IDs generated.")  

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python script_name.py folder_path output_csv_file csv_file data_type")
    else:
        folder_path = sys.argv[1]
        output_csv_file = sys.argv[2]
        csv_file = sys.argv[3]
        data_type = sys.argv[4].lower()  # Convert to lowercase for case-insensitivity
        main(folder_path, output_csv_file, csv_file, data_type)

