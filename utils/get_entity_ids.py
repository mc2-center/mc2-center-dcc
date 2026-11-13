"""
Get synIDs for entities matching input name
"""

from synapseclient import operations
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
    parser.add_argument("-l", nargs="+", help="Synapse project IDs to query.")
    parser.add_argument(
        "-n", type=str, help="Entity name to search for in Synapse project."
    )
    return parser.parse_args()


def get_list(syn, name, projects):

    df = pd.DataFrame(columns=["projectId", "entityId"])

    for project in projects:
        entityId = operations.find_entity_id(name, project, synapse_client=syn)
        newRow = pd.DataFrame([[project, entityId]], columns=["projectId", "entityId"])
        df = pd.concat([df, newRow])

    df = df.reset_index(drop=True)
    return df


def main():

    syn = syn_login()
    args = get_args()
    out = get_list(syn, args.n, args.l)

    outPath = Path("output/project_entity.csv")
    outPath.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(outPath, index=True)


if __name__ == "__main__":
    main()
