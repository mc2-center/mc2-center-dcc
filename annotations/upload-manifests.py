"""
upload-manifests.py

This script accepts a CSV file containing manifest file paths and their corresponding Synapse IDs for target folders. 
It then performs a parallel validation check, after which it parallel uploads the manifests to Synapse

author: aditi.gopalan
author: orion.banks

"""

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
    parser.add_argument(
        "-m",
        type=str,
        help="path to CSV containing file paths and target folder synIDs",
    )
    parser.add_argument(
        "-t",
        type=str,
        choices=["PublicationView", "DatasetView", "ToolView", "EducationalResource", "GrantView"],
        help="type of manifest(s) being validated and uploaded",
    )
    parser.add_argument("-c", type=str, help="path to a schematic config file")
    parser.add_argument(
        "-v",
        action="store_false",
        help="Boolean; if this flag is provided, invalid manifests will be submitted",
    )
    parser.add_argument(
        "--skip",
        action="store_true",
        default=None,
        help="Boolean; if this flag is provided, validation will be skipped. Only use if your manifests have been previously validated.",
    )
    return parser.parse_args()


def login():
    """Login to Synapse"""
    syn = synapseclient.Synapse()
    syn.login()
    return syn


def validate_entry_worker(args, cf, mt, valid_only):
    print(
        f"Args received in validate_entry_worker: {args}"
    )  # Add this line to print args
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
        subprocess.run(
            validate_command, check=True, stdout=sys.stdout, stderr=subprocess.STDOUT
        )
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
        "-mrt",
        "table_and_file",
        "-tm",
        "upsert",
        "-tcn",
        "display_name"
    ]
    cmd_line = " ".join(command)

    print(cmd_line)

    subprocess.run(command, stdout=sys.stdout, stderr=subprocess.STDOUT)


def main():

    # syn = login()
    args = get_args()
    csv_file = args.m if args.m else "input.csv"
    config_file = args.c
    manifest_type = args.t
    submit_valid = args.v
    skip_validation = args.skip
    single = True

    df = pd.read_csv(csv_file)
    pd.set_option("display.max_colwidth", 90)

    num_processes = multiprocessing.cpu_count()  # Number of CPU cores
    
    validation_args_list = list(df.to_records(index=False))

    if skip_validation is not None:
        submit_args_list = validation_args_list

    if skip_validation is None:

        pool = multiprocessing.Pool(processes=num_processes)

        validated_files = pool.map(
            partial(
                validate_entry_worker,
                cf=config_file,
                mt=manifest_type,
                valid_only=submit_valid,
            ),
            validation_args_list,
        )
        print("/n ####VALIDATED FILES##### /n", validated_files)

        pool.close()
        pool.join()

        validated_files = [tup for tup in validated_files if tup is not None]
        submit_args_list = [tup for tup in validated_files]

    submit_pool = multiprocessing.Pool(processes=num_processes)
    
    print(pd.DataFrame(submit_args_list))

    choice = input(
        "\n\nReview the printed list of arguments for errors in path and target matches. Type 'upload' to continue or 'end' if you see an error\n\n"
    )

    if choice == "upload":
        
        if single:
            print("Single upload mode is active. Manifests will be uploaded one-by-one.")
            cf = config_file
            for fp, target_id in submit_args_list:
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
                "-mrt",
                "table_and_file",
                "-tm",
                "upsert",
                "-tcn",
                "display_name"
                ]

                cmd_line = " ".join(command)
                print(cmd_line)
                subprocess.run(command, stdout=sys.stdout, stderr=subprocess.STDOUT)

        else:
            submit_pool.map(partial(submit_entry_worker, cf=config_file), submit_args_list)

        submit_pool.close()
        submit_pool.join()

    elif choice == "end":
        print("\n\nManifests will NOT be uploaded. Exiting now.")
        exit


if __name__ == "__main__":
    main()
