"""
Check if Synapse user account is certified. 
Prints output to terminal, unless '-f' is provided as a flag at run time
"""

from synapseclient.models import UserProfile
import argparse
import pandas as pd
from pathlib import Path

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
        status = UserProfile(id=id).is_certified(synapse_client=syn)
        newRow = pd.DataFrame([[id, status]], columns=["personId", "certState"])
        df = pd.concat([df, newRow])

    return df


def main():

    syn = syn_login()
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
