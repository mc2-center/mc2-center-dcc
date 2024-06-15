""" 
schema_update.py 

This script takes the projectID (grantId) for respective manifests and gets the corresponding tableIds. 
Using the tableIds, it updates the schema for specific cols in the table

author: aditi.gopalan
author: thomas.yu
author: orion.banks
"""

import synapseclient
import pandas as pd
import sys

syn = synapseclient.login()

if len(sys.argv) != 3:
    print("Usage: python script_name.py input_file.csv entity_type")
    sys.exit(1)

input_file_path = sys.argv[1]
entity_type = sys.argv[2]

grantId_list = pd.read_csv(input_file_path)["grantId"].tolist()

table_ids_list = []

for grantId in grantId_list:
    try:
        table_id = [
            x
            for x in syn.getChildren(grantId, includeTypes=["table"])
            if x.get("name").startswith(entity_type.lower())
        ][0].get("id")
    except IndexError:
        table_id = ""

    table_ids_list.append(table_id)

print(len(table_ids_list))
print(table_ids_list)

# Updating schema for all the tables in the list
# Columns to modify
columns_to_modify = [
    f"{entity_type} Keywords",
    f"{entity_type} Abstract",
    f"{entity_type} Authors",
    f"{entity_type} Assay",
    f"{entity_type} Tumor Type",
    f"{entity_type} Tissue",
    f"{entity_type} Dataset Alias",
    f"{entity_type} Title",
    f"{entity_type} Download Type",
    f"{entity_type} Documentation Type",
    f"{entity_type} Grant Number",
    f"{entity_type} Doi",
    f"{entity_type} Journal",
    f"Pubmed Id",
    f"Pubmed Url",
    f"{entity_type} Name",
    f"{entity_type} Pubmed Id",
    f"{entity_type} Alias",
    f"{entity_type} Description",
    f"{entity_type} Design",
    f"{entity_type} Species",
    f"{entity_type} Url",
    f"{entity_type} File Formats",
    f"{entity_type} Year",
    f"{entity_type} Accessibility",
    f"{entity_type}View_id"
]

# Initialize counter
successful_table_modifications_count = 0

for my_table_synid in table_ids_list:
    try:
        my_table = syn.get(my_table_synid)

        # Get existing columns
        my_columns = list(syn.getColumns(my_table_synid))

        # Modify specified columns
        for column_name in columns_to_modify:
            # Find the column to modify
            column_to_modify = next(
                (col for col in my_columns if col.name == column_name), None
            )

            if column_to_modify:
                # Set columnType and maximumSize accordingly
                if column_name in [f"{entity_type} Abstract", f"{entity_type} Authors", f"{entity_type} Description", f"{entity_type} Design"]:
                    new_column = syn.store(
                        synapseclient.Column(
                            name=column_name, columnType="LARGETEXT"
                        )
                    )
                elif column_name in [f"{entity_type} Keywords", f"{entity_type} Title", f"{entity_type} Assay"]:
                    new_column = syn.store(
                        synapseclient.Column(
                            name=column_name, columnType="MEDIUMTEXT"
                        )
                    )

                elif column_name in [f"{entity_type} Name", f"{entity_type} Alias", f"{entity_type} Species", f"{entity_type} Tumor Type", f"{entity_type} Tissue", f"{entity_type} Dataset Alias", f"{entity_type} Url", f"{entity_type} File Formats"]:
                    new_column = syn.store(
                        synapseclient.Column(
                            name=column_name, columnType="STRING", maximumSize=500
                        )
                    )
                
                else:
                    new_column = syn.store(
                        synapseclient.Column(
                            name=column_name, columnType="STRING", maximumSize=100
                        )
                    )

                # Define the change
                changes = {
                    "concreteType": "org.sagebionetworks.repo.model.table.TableUpdateTransactionRequest",
                    "entityId": my_table_synid,
                    "changes": [
                        {
                            "concreteType": "org.sagebionetworks.repo.model.table.TableSchemaChangeRequest",
                            "entityId": my_table_synid,
                            "changes": [
                                {
                                    "oldColumnId": column_to_modify.id,
                                    "newColumnId": new_column.id,
                                }
                            ],
                        }
                    ],
                }

                # Wait for asynchronous update
                syn._waitForAsync(
                    uri=f"/entity/{my_table_synid}/table/transaction/async",
                    request=changes,
                )

        print(f"Columns modified successfully for table ID: {my_table_synid}")

        # Increment counter for successful table modification
        successful_table_modifications_count += 1

    except synapseclient.core.exceptions.SynapseHTTPError as e:
        # Log the error if needed
        print(f"Error modifying columns for table ID {my_table_synid}: {e}")
        continue  # Continue to the next table ID even if an error occurs
    except synapseclient.core.exceptions.SynapseFileNotFoundError as e:
        print(f"Error: {e}")
        print(f"Synapse ID causing the error: {my_table_synid}")
        continue  # Continue to the next table ID even if an error occurs

# Print the total number of tables successfully modified
print(
    f"Total number of tables successfully modified: {successful_table_modifications_count}"
)

# Remove the 'grantID' column from the input file
input_data = pd.read_csv(input_file_path)
input_data = input_data.drop(columns=["grantId"])
input_data.to_csv(input_file_path, index=False)

print("The 'grantID' column has been deleted from the input file.")
