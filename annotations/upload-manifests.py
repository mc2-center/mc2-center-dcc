import pandas as pd
import synapseclient
import multiprocessing
import subprocess
import sys

'''

This script accepts a CSV file containing manifest file paths and their corresponding Synapse IDs for target folders. 
It then performs a parallel validation check, after which it parallel uploads the manifests to Synapse

'''
def login():
    """Login to Synapse"""
    syn = synapseclient.Synapse()
    syn.login()
    return syn

def validate_entry_worker(fp):
    print(f"Validating file: {fp}")
    validate_command = [
        "schematic",
        "model",
        "-c",
        "/Users/agopalan/schematic/config.yml",  # Replace with link to your schematic config file
        "validate",
        "-dt",
        "PublicationView",
        "-mp",
        fp
    ]
    
    print(f"Running validation command: {' '.join(validate_command)}")
    try:
        subprocess.run(validate_command, check=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
        return fp  # Validation succeeded, return the filepath
    except subprocess.CalledProcessError:
        print(f"File with {fp} could not be validated and will not be submitted to Synapse.")
        return None  # Validation failed

def submit_entry_worker(args):
    fp, target_id = args
    print(f"Submitting file: {fp} with target ID: {target_id}")
    command = [
        "schematic",
        "model",
        "-c",
        "/Users/agopalan/schematic/config.yml",  # Replace with link to your schematic config file
        "submit",
        "-mp",
        fp,
        "-d",
        target_id,
        "-dl",
        "-mrt",
        "table_and_file",
        "-tm",
        "upsert"
    ]

    
    cmd_line = " ".join(command)
    print(cmd_line)
    subprocess.run(command, stdout=sys.stdout, stderr=subprocess.STDOUT)

def main():
    
    syn = login()
    csv_file = '/Users/agopalan/mc2-center-dcc/annotations/input.csv'  # Replace with CSV file path
    df = pd.read_csv(csv_file)

    num_processes = multiprocessing.cpu_count()  # Number of CPU cores
    pool = multiprocessing.Pool(processes=num_processes)

    validation_args_list = df['file_path'].tolist()

    validated_files = pool.map(validate_entry_worker, validation_args_list)

    pool.close()
    pool.join()

    validated_files = [fp for fp in validated_files if fp is not None]

    submit_pool = multiprocessing.Pool(processes=num_processes)

    submit_args_list = [(fp, df.loc[i, 'target_id']) for i, fp in enumerate(validated_files)]

    submit_pool.map(submit_entry_worker, submit_args_list)

    submit_pool.close()
    submit_pool.join()

if __name__ == "__main__":
    main()




