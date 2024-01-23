import os
import pandas as pd

def convert_xlsx_to_csv(folder_path):
    for filename in os.listdir(folder_path):
        if filename.endswith(".xlsx"):
            xlsx_path = os.path.join(folder_path, filename)
            csv_path = os.path.join(folder_path, os.path.splitext(filename)[0] + ".csv")
            
            # Load the Excel file into a DataFrame
            df = pd.read_excel(xlsx_path)
            
            # Save the DataFrame to a CSV file
            df.to_csv(csv_path, index=False)
            
            print(f"Converted: {xlsx_path} to {csv_path}")

if __name__ == "__main__":
    folder_path = "."  # Set the path to your folder here
    convert_xlsx_to_csv(folder_path)
