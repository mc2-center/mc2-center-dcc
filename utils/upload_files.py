"""upload_files.py
This script will upload a batch of files to Synapse, based on a TSV-formatted manifest,
provided at runtime.

Usage:
python upload_files.py -m <path to TSV manifest>

author: orion.banks
"""

import synapseclient
import synapseutils
import argparse

def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-m",
						type=str,
                        help="path to manifest listing file paths and target folders in tsv format")
    return parser.parse_args()

def main():
	
	syn = synapseclient.login() #you can pass your username and password directly to this function

	args = get_args()

	manifest = args.m #assign path to manifest file from command line input

	upload = synapseutils.syncToSynapse(syn, manifestFile=manifest)

if __name__ == "__main__":
    main()