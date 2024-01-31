import os
import pandas as pd

# Get the current working directory
folder_path = os.getcwd()

# Loop through each file in the directory
for filename in os.listdir(folder_path):
    if filename.endswith(".csv"):
        # Construct the full file path
        file_path = os.path.join(folder_path, filename)

        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)

        # Modify the DataFrame using your provided code
        for column in df.columns:
            if column in df.columns:
                df[column] = df[column].apply(lambda x: x[:400] + "(Read more on Pubmed)" if isinstance(x, str) and len(x) > 500 else x)

        # Save the modified DataFrame back to the CSV file
        df.to_csv(file_path, index=False)

print("Modification complete.")
