"""upload_files.py

This script will upload a batch of files to Synapse, based on a TSV-formatted manifest,
provided at runtime.

TSV should contain columns:
'path' - path to file to be uploaded to Synapse
'parent' - Synapse ID of folder or Synapse project where the file will be stored

Usage:
python upload_files.py -m <path to TSV manifest>

author: orion.banks
"""

import synapseutils
import argparse

import importlib.util
import os
# removed when the mc2dcc package (CLI consolidation plan) lands
# portal_tables/utils.py, loaded by path: "utils" would collide with this package
_spec = importlib.util.spec_from_file_location(
    "portal_tables_utils",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "portal_tables", "utils.py"),
)
_portal_utils = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_portal_utils)
syn_login = _portal_utils.syn_login


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-m",
        type=str,
        help="path to manifest listing file paths and target folders in tsv format",
    )
    return parser.parse_args()


def main():

    syn = syn_login()

    args = get_args()

    manifest = args.m  # assign path to manifest file from command line input

    upload = synapseutils.syncToSynapse(syn, manifestFile=manifest)


if __name__ == "__main__":
    main()
