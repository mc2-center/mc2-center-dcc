"""
Check if Synapse user account is certified. 
Prints output to terminal, unless '-f' is provided as a flag at run time

author: orion.banks
"""

import synapseclient
import argparse
import pandas as pd
from pathlib import Path


### Login to Synapse ###
def login():

    syn = synapseclient.Synapse()
    syn.login()

    return syn


def get_args():

    parser = argparse.ArgumentParser(description="Get synapse project folder ids")
    parser.add_argument(
        "-l", nargs="+", help="Synapse user IDs to check for certification state."
    )
    parser.add_argument(
        "-f",
        action="store_true",
        help="Boolean; if provided, store the output as a CSV.",
    )
    return parser.parse_args()


def get_status(syn, idList):

    df = pd.DataFrame(columns=["personId", "certState"])

    for id in idList:
        status = syn.is_certified(id)
        newRow = pd.DataFrame([[id, status]], columns=["personId", "certState"])
        df = pd.concat([df, newRow])

    return df


def main():

    syn = login()
    args = get_args()
    out = get_status(syn, args.l)

    if args.f:
        outPath = Path("output/cert_status.csv")
        outPath.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(outPath, index=True)

    else:
        print(out)


if __name__ == "__main__":
    main()
