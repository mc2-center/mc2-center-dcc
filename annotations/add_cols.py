import os
import pandas as pd


def process_csv(file_path):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file_path)

    # 1. Change column name from "Publication TumorType" to "Publication Tumor Type"
    if "Publication TumorType" in df.columns:
        df = df.rename(columns={"Publication TumorType": "Publication Tumor Type"})

    # 2. Add "PublicationView_id" as a column if not present, and fill it with values from "Pubmed Id" column
    if "PublicationView_id" not in df.columns and "Pubmed Id" in df.columns:
        df["PublicationView_id"] = df["Pubmed Id"]

    # 3. Drop 'Publication Theme Name' and 'Publication Consortium Name' columns
    columns_to_drop = ["Publication Theme Name", "Publication Consortium Name"]
    df = df.drop(columns=columns_to_drop, errors="ignore")

    # Save the modified DataFrame back to the CSV file
    df.to_csv(file_path, index=False)


if __name__ == "__main__":
    # Get the current working directory
    current_directory = os.getcwd()

    # Iterate over all files in the current directory with a .csv extension
    for filename in os.listdir(current_directory):
        if filename.endswith(".csv"):
            file_path = os.path.join(current_directory, filename)

            # Process each CSV file
            process_csv(file_path)

    print("Processing completed.")
