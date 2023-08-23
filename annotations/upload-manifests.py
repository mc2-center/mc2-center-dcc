import pandas as pd
import synapseclient
import multiprocessing
import subprocess

def login():
    """Login to Synapse"""
    syn = synapseclient.Synapse()
    syn.login()
    return syn

def submit_entry_worker(args):
    df, target_id = args
    print(f"Submitting file: {df} with target ID: {target_id}")
    # Construct the subprocess command
    command = [
        "schematic",
        "model",
        "submit",
        "--mp",
        df,
        "-d",
        target_id
    ]

    # Print the command
    print(" ".join(command))  
    subprocess.run(command)

def main():
    choice = input(
        "\n\nDid you validate the manifest using Schematic before running this script? Type 'y' for yes, 'n' for no"
    )
    if choice == 'y':
        syn = login()
        csv_file = 'input.csv'  # Replace with CSV file path
        df = pd.read_csv(csv_file)

        # Create a pool of worker processes
        num_processes = multiprocessing.cpu_count()  # Number of CPU cores
        pool = multiprocessing.Pool(processes=num_processes)

        #  Arguments for the submit_entry_worker function
        args_list = [(pd.read_csv(row['file_path'], index_col=False).fillna(""), row['target_id']) for _, row in df.iterrows()]

        # Submit entries using multiprocessing
        pool.map(submit_entry_worker, args_list)

        # Close the pool of worker processes
        pool.close()
        pool.join()

        '''
        
        #Un-parallelized code block

        # Iterate over each row in the CSV
        for index, row in df.iterrows():
            file_path = row['file_path']  
            target_id = row['target_id']  
            
            # Call the submission function with file_path and target_id
            df = pd.read_csv(file_path, index_col=False).fillna("")
            submit_entry(syn, df, target_id)
        

        '''

if __name__ == "__main__":
    main()

