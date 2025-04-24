'''
upload_validation.py
author: aditi.gopalan
Description: This script checks if manifests have been uploaded to Synapse 
'''

import synapseclient
import pandas as pd
import csv
import argparse
from datetime import datetime

def get_folder_type_argument():
    parser = argparse.ArgumentParser(description="Process Synapse folders and create a manifest.")
    parser.add_argument("csv_file_path", help="Path to the input CSV file.")
    parser.add_argument("folder_type", choices=["Publication", "Dataset", "Tool", "Grant"],
                        help="Specify folder type: 'Publication', 'Dataset', 'Tool', or 'Grant'")
    parser.add_argument(
        "-s",
        action="store_true",
        help="Boolean; if this flag is provided, validation report will not be uploaded",
    )
    args = parser.parse_args()
    return args.csv_file_path, args.folder_type, args.s

def get_folder_id_column(folder_type):
    if folder_type == "Publication":
        return "folderIdPublication"
    elif folder_type == "Dataset":
        return "folderIdDatasets"
    elif folder_type == "Tool":
        return "folderIdTools"
    elif folder_type == "Grant":
        return "folderIdGrant"
    

def get_project_name(syn, folder_id):
    try:
        folder = syn.get(folder_id)
        project = syn.get(folder.parentId)
        return project.name
    except Exception as e:
        print(f"Error getting information for {folder_id}: {e}")
        return 'ERROR'

def main():
    csv_file_path, folder_type, upload = get_folder_type_argument()
    
    syn = synapseclient.Synapse()
    syn.login()

    df = pd.read_csv(csv_file_path)
    results = []

    for index, row in df.iterrows():
        folder_synapse_id = str(row[get_folder_id_column(folder_type)])
        
        if pd.notna(folder_synapse_id) and folder_synapse_id.lower() != 'nan':
            project_name = get_project_name(syn, folder_synapse_id)
            if project_name != 'ERROR':
                try:
                    folder_children = syn.getChildren(folder_synapse_id)
                    most_recent_file = max(folder_children, key=lambda x: x['modifiedOn'])
                    modification_time = most_recent_file.get('modifiedOn', 'N/A')
                    results.append({'SynapseID': folder_synapse_id, 'ProjectName': project_name, 'ModifiedOn': modification_time})
                except Exception as e:
                    print(f"Error getting information for {folder_synapse_id}: {e}")
                    results.append({'SynapseID': folder_synapse_id, 'ProjectName': 'ERROR', 'ModifiedOn': 'ERROR'})
            else:
                print(f"Skipping row with invalid Synapse ID: {index}")
        else:
            print(f"Skipping row with missing Synapse ID: {index}")

    current_month = datetime.now().strftime("%Y%m")
    output_csv_file = f"upload_check_{current_month}.csv"
    
    with open(output_csv_file, 'w', newline='') as csvfile:
        fieldnames = ['SynapseID', 'ProjectName', 'ModifiedOn']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            writer.writerow(result)

    print(f"Results saved to {output_csv_file}")

    if upload is None:
        folder_id = "syn53770348"  # Replace with the actual Synapse folder ID

        file_entity = synapseclient.File(output_csv_file, parent=folder_id)
        syn.store(file_entity)

        print(f"Manifest uploaded to Synapse folder: {folder_id}")

if __name__ == "__main__":
    main()

 

