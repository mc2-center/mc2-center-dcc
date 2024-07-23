"""sync_manifest.py

This script will generate a TSV-formatted manifest for Synapse upload
via syncToSynapse.

Folder contained in the directory path will be created in Synapse

TSV will contain columns:
'path' - path to file to be uploaded to Synapse
'parent' - Synapse ID of folder or Synapse project where the file will be stored

Usage:
python sync_manifest.py -m <path to TSV manifest>

author: orion.banks
"""

import synapseclient
import synapseutils
import argparse


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        type=str,
        help="path to directory to be uploaded to Synapse",
    )
    parser.add_argument(
        "-p",
        type=str,
        help="folder or project Synapse ID, where the files will be stored",
    )
    parser.add_argument(
        "-m",
        type=str,
        help="[Optional] local path at which the manifest TSV will be stored",
    )
    return parser.parse_args()


def main():

    syn = (
        synapseclient.login()
    )  # you can pass your username and password directly to this function

    args = get_args()

    local, target, manifest = args.d, args.p, args.m  # assign path to manifest file from command line input

    synapseutils.generate_sync_manifest(
        syn,
        directory_path=local,
        parent_id=target,
        manifest_path=manifest
        )


if __name__ == "__main__":
    main()
