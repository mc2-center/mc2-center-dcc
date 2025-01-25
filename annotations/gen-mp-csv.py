"""
gen-mp-csv.py

This script will:
- query Synapse tables for grant and project folder information
- create a grant folder reference table and save it as a CSV
- extract grant numbers from manifests
- generate a CSV that contains manifest paths and target folder Synapse Ids, for use with upload-manifests.py

Note that Synapse Ids to query are hardcoded:
- grant_table = syn21918972 (Grants - Merged from CCKP database)
- folder_table = syn27210848 (All Files V2 from CCKP database)

author: aditi.gopalan
author: orion.banks
"""

import os
import csv
import pandas as pd
import sys
import synapseclient
from datetime import datetime


def query_synapse_for_folder_info(
    ref_path, data_type, folder_id_column_name, grant_id_column_name
):

    syn = synapseclient.login()

    grant_table = (
        syn.tableQuery(
            f'SELECT "grantNumber", "grantId" AS {grant_id_column_name} FROM syn21918972'
        )
        .asDataFrame()
        .fillna("")
    )

    folder_table = (
        syn.tableQuery(
            f"SELECT id AS {folder_id_column_name}, name, projectId AS {grant_id_column_name}  FROM syn27210848 WHERE name='{data_type}' AND parentId=projectId"
        )
        .asDataFrame()
        .fillna("")
    )

    grant_folder_reference = grant_table.merge(
        folder_table, how="left", on=grant_id_column_name
    )

    grant_folder_reference.to_csv(ref_path, index=False)

    return grant_folder_reference


def get_csv_files_in_folder(folder_path, file_suffix):
    csv_files = []
    for file in os.listdir(folder_path):
        if file.endswith(file_suffix):
            csv_files.append(os.path.join(folder_path, file))
    return csv_files


def extract_ca_number(file_path, file_suffix):
    file_name = os.path.basename(file_path)
    # Assuming the file name format is "CA****_publication.csv" or "CA****_dataset.csv"
    ca_number = file_name.split("_")[0][2:]
    print(f"\n\nExtracted CA number: {ca_number}")
    return f"CA{ca_number}"


def get_folder_id_and_grant_id_from_csv(
    grant_folder_reference, ca_number, folder_id_column_name, grant_id_column_name, mapping
):
    df = grant_folder_reference

    if folder_id_column_name in df.columns and grant_id_column_name in df.columns:
        
        match = df[df["grantNumber"] == ca_number]
        if ca_number in mapping.keys():
            print(f"Entry {ca_number} contains values from mapping dictionary")
            folder_id, grant_id = mapping[ca_number]
        
        elif not match.empty:
            folder_id = match[folder_id_column_name].values[0]
            grant_id = match[grant_id_column_name].values[0]
            
        else:
            print(f"No match found for {ca_number}.")
            return None, None
        
        print(
                f"Matching {folder_id_column_name}: {folder_id}, Grant ID: {grant_id}"
            )
        
        return folder_id, grant_id
    
    else:
        print(
            f"Error: '{folder_id_column_name}' or '{grant_id_column_name}' column not found in the CSV file: {grant_folder_reference}"
        )
        return None, None


def write_file_paths_to_csv(
    file_paths,
    output_file,
    file_suffix,
    grant_folder_reference,
    folder_id_column_name,
    grant_id_column_name,
    mapping
):

    ref_name = "".join(
        ["grant_folder_reference_", datetime.today().strftime("%Y-%m-%d"), ".csv"]
    )
    base_path = os.path.dirname(file_paths[0])
    ref_path = os.path.join(base_path, ref_name)
    grant_folder_reference = query_synapse_for_folder_info(
        ref_path, data_type, folder_id_column_name, grant_id_column_name
    )

    with open(output_file, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(["File Paths", folder_id_column_name, grant_id_column_name])

        for file_path in file_paths:
            ca_number = extract_ca_number(file_path, file_suffix)
            folder_id, grant_id = get_folder_id_and_grant_id_from_csv(
                grant_folder_reference,
                ca_number,
                folder_id_column_name,
                grant_id_column_name,
                mapping
            )
            csv_writer.writerow([file_path, folder_id, grant_id])


def main(folder_path, output_csv_file, data_type):
    grant_id_column_name = "grantId"

    if data_type == "publications":
        file_suffix = "_publication.csv"
        folder_id_column_name = "folderIdPublication"
        folder_map = {"CA209997": "syn32698150",
                      "CA209923": "syn43447063",
                      "CAfiliatedNon-GrantAssociated": "syn52963310",
                      "CA184898": "syn32698262"
                   }

    elif data_type == "datasets":
        file_suffix = "_dataset.csv"
        folder_id_column_name = "folderIdDatasets"
        folder_map = {"CA209997": "syn52744921",
                      "CA209923": "syn43447065",
                      "CAfiliatedNon-GrantAssociated": "syn52963225",
                      "CA184898": "syn34577441"
                   }

    elif data_type == "tools":
        file_suffix = "_tool.csv"
        folder_id_column_name = "folderIdTools"
        folder_map = {"CA209997": "syn32698153",
                      "CA209923": "syn43447067",
                      "CAfiliatedNon-GrantAssociated": "syn52963223",
                      "CA184898": "syn53478645"
                   }

    elif data_type == "education":
        file_suffix = "_education.csv"
        folder_id_column_name = "folderIdEducation"

    elif data_type == "grants":
        file_suffix = "_grant.csv"
        folder_id_column_name = "folderIdGrant"

    else:
        print(
            "Invalid data type. Please provide one of 'publications', 'datasets', 'tools', 'education', or 'grants'."
        )
        return
    
    eq_mapping = {"CA209997": (folder_map["CA209997"], "syn7315802"),
                  "CA209923": (folder_map["CA209923"], "syn43447051"),
                  "CAfiliatedNon-GrantAssociated": (folder_map["CAfiliatedNon-GrantAssociated"], "syn52963211"),
                   "CA184898": (folder_map["CA184898"], "syn9772917")
                  }

    file_paths = get_csv_files_in_folder(folder_path, file_suffix)
    manifest_for_upload = write_file_paths_to_csv(
        file_paths,
        output_csv_file,
        data_type,
        file_suffix,
        folder_id_column_name,
        grant_id_column_name,
        eq_mapping
    )

    print("CSV file with file paths, target IDs, and grant IDs generated.")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python script_name.py folder_path output_csv_file data_type")
    else:
        folder_path = sys.argv[1]
        output_csv_file = sys.argv[2]
        data_type = sys.argv[3].lower()  # Convert to lowercase for case-insensitivity
        main(folder_path, output_csv_file, data_type)
