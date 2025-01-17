""" 
replace_table_schema.py 

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

print(table_ids_list)
print(f"\n\nNumber of tables to modify: {len(table_ids_list)}")

columns_to_drop = [f"{entity_type} Grant Number", f"{entity_type} Pubmed Id"]

# Initialize counter
successful_table_modifications_count = 0

for my_table_synid in table_ids_list:
    try:
        my_table = syn.get(my_table_synid)

        # Get existing columns
        current_columns = list(syn.getColumns(my_table_synid))

        if entity_type == "Publication":
            columns_to_add = ["GrantView Key", "Study Key"]

            column_order = [
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
                "Publication Dataset Alias",
                "Id",
                "entityId",
            ]

        if entity_type == "Dataset":
            columns_to_add = [
                "GrantView Key",
                "Study Key",
                "PublicationView Key",
                "Data Use Codes",
            ]

            column_order = [
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
                "Publication Abstract",
                "Dataset File Formats",
                "Data Use Codes",
                "Id",
                "entityId",
            ]

        col_names = [n.name for n in current_columns]

        added_columns = [
            synapseclient.Column(name=x, columnType="STRING", maximumSize=100)
            for x in columns_to_add
            if x not in col_names
        ]

        dropped_columns = [y for y in current_columns if y.name in columns_to_drop]

        added_tup = list(zip(added_columns, ["add"] * len(added_columns)))
        dropped_tup = list(zip(dropped_columns, ["drop"] * len(dropped_columns)))

        column_tups = added_tup + dropped_tup
        
        print(f"Adding columns to {my_table_synid}:\n{[c.name for c in added_columns]}\n\n")
        print(f"Removing columns from {my_table_synid}:\n{[y.name for y in dropped_columns]}\n\n")

        # Modify specified columns
        for column, action in column_tups:
            if action == "drop":
                drop_column = column
                # Define the change
                changes = {
                    "concreteType": "org.sagebionetworks.repo.model.table.TableUpdateTransactionRequest",
                    "entityId": my_table_synid,
                    "changes": [
                        {
                            "concreteType": "org.sagebionetworks.repo.model.table.TableSchemaChangeRequest",
                            "entityId": my_table_synid,
                            "changes": [{"oldColumnId": drop_column.id}],
                        }
                    ],
                }

            if action == "add":
                new_column = syn.store(column)
                # Define the change
                changes = {
                    "concreteType": "org.sagebionetworks.repo.model.table.TableUpdateTransactionRequest",
                    "entityId": my_table_synid,
                    "changes": [
                        {
                            "concreteType": "org.sagebionetworks.repo.model.table.TableSchemaChangeRequest",
                            "entityId": my_table_synid,
                            "changes": [{"newColumnId": new_column.id}],
                        }
                    ],
                }
            
            syn._waitForAsync(uri=f"/entity/{my_table_synid}/table/transaction/async", request=changes)
        

        # Wait for asynchronous update
        

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

    updated_columns = list(syn.getColumns(my_table_synid))

    updated_ids = [col.id for col in updated_columns]

    ordered_ids = []

    for order_name in column_order:
        order_id = [col.id for col in updated_columns if col.name == order_name]
        ordered_ids = ordered_ids + order_id

    order_str = ", ".join(ordered_ids)
    print(f"Column Id order:\n{order_str}")
    lead_id = ordered_ids[0]

    order = {
        "concreteType": "org.sagebionetworks.repo.model.table.TableUpdateTransactionRequest",
        "entityId": my_table_synid,
        "changes": [
            {
                "concreteType": "org.sagebionetworks.repo.model.table.TableSchemaChangeRequest",
                "entityId": my_table_synid,
                "changes": [{"oldColumnId": lead_id, "newColumnId": lead_id}], #does not change columns, just triggers table update to apply order
                "orderedColumnIds": ordered_ids,
            }
        ],
    }

    # Wait for asynchronous update
    syn._waitForAsync(
        uri=f"/entity/{my_table_synid}/table/transaction/async", request=order
    )

# Print the total number of tables successfully modified
print(
    f"\nTotal number of tables successfully modified:\n{successful_table_modifications_count} of {len(table_ids_list)}"
)
