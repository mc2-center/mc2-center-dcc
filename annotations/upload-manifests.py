''' This script takes an input of manifest paths and their target ids and validates and uploads files in parallel using schematic '''
import pandas as pd
import synapseclient
import multiprocessing
import subprocess
import sys
import argparse
from functools import partial

def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-m",
                        type=str,
                        help="path to CSV containing file paths and target folder synIDs")
    parser.add_argument("-t",
                        type=str,
                        choices=["PublicationView", "DatasetView", "ToolView", "EducationalResource"],
                        help="type of manifest(s) being validated and uploaded")
    parser.add_argument("-c",
                        type=str,
                        help="path to a schematic config file")
    parser.add_argument("-v",
                        action="store_false",
                        help="Boolean; if this flag is provided, invalid manifests will be submitted")
    return parser.parse_args()

def login():
    """Login to Synapse"""
    syn = synapseclient.Synapse()
    syn.login()
    return syn

def validate_entry_worker(args, cf, mt, valid_only):
    print(f"Args received in validate_entry_worker: {args}")  # Add this line to print args
    fp, target_id = args  # Unpack the tuple
    print(f"Validating file: {fp} of type {mt}")
    validate_command = [
        "schematic",
        "model",
        "-c",
        cf,
        "validate",
        "-dt",
        mt,
        "-mp",
        fp
    ]

    print(f"Running validation command: {' '.join(validate_command)}")
    try:
        subprocess.run(validate_command, check=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
        return args  # Validation succeeded, return the tuple
    except subprocess.CalledProcessError:
        print(f"File with {fp} could not be validated.")  # Validation failed
        if valid_only:
            return None
        else:
            return args  # Return the tuple


def submit_entry_worker(args, cf):
    fp, target_id = args  # Unpack the tuple
    print(f"Submitting file: {fp} with target ID: {target_id}")
    command = [
        "schematic",
        "model",
        "-c",
        cf,
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
    
    #syn = login()
    args = get_args()
    csv_file = args.m if args.m else 'input.csv'
    config_file = args.c
    manifest_type = args.t
    submit_valid = args.v

    df = pd.read_csv(csv_file)
    pd.set_option('display.max_colwidth', 90)

    num_processes = multiprocessing.cpu_count()  # Number of CPU cores
    pool = multiprocessing.Pool(processes=num_processes)

    validation_args_list = list(df.to_records(index=False))

    validated_files = pool.map(partial(
        validate_entry_worker, cf=config_file, mt=manifest_type, valid_only=submit_valid), 
        validation_args_list)
    print("/n ####VALIDATED FILES##### /n", validated_files)

    pool.close()
    pool.join()

    validated_files = [tup for tup in validated_files if tup is not None]

    submit_pool = multiprocessing.Pool(processes=num_processes)

    submit_args_list = [tup for tup in validated_files]
    print(pd.DataFrame(submit_args_list))

    choice = input(
        "\n\nReview the printed list of arguments for errors in path and target matches. Type 'upload' to continue or 'end' if you see an error")

    if choice == 'upload':
        submit_pool.map(partial(
            submit_entry_worker, cf=config_file),
            submit_args_list)

        submit_pool.close()
        submit_pool.join()

    elif choice == 'end':
        print("\n\nManifests will NOT be uploaded. Exiting now.")
        exit

if __name__ == "__main__":
    main()











