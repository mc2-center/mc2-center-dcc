import synapseclient
import pandas as pd
import csv

# Set up your Synapse credentials
syn = synapseclient.Synapse()
syn.login()

# Read the CSV file into a DataFrame
csv_file_path = "feb_filepaths.csv"
df = pd.read_csv(csv_file_path)

# List to store results
results = []

# Iterate through the rows and get the last modification time for each folder
for index, row in df.iterrows():
    folder_synapse_id = str(row['folderIdPublication'])
    
    try:
        # Get the list of children (files) for the folder
        folder_children = syn.getChildren(folder_synapse_id)
        
        # Find the most recently modified file within the folder
        most_recent_file = max(folder_children, key=lambda x: x['modifiedOn'])
        
        # Get the last modification time from the file metadata
        modification_time = most_recent_file.get('modifiedOn', 'N/A')

        # Append results to the list
        results.append({'SynapseID': folder_synapse_id, 'ModifiedOn': modification_time})
    except Exception as e:
        print(f"Error getting information for {folder_synapse_id}: {e}")
        # If there is an error, append an entry with 'ERROR' for ModifiedOn
        results.append({'SynapseID': folder_synapse_id, 'ModifiedOn': 'ERROR'})

# Save results to a new CSV file
output_csv_file = "upload_check.csv"
with open(output_csv_file, 'w', newline='') as csvfile:
    fieldnames = ['SynapseID', 'ModifiedOn']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for result in results:
        writer.writerow(result)

print(f"Results saved to {output_csv_file}")




