"""Shared functions used by syncing scripts"""

import os
import argparse
from getpass import getpass
from datetime import datetime

import synapseclient
import pandas as pd


# Manifest and portal table synIDs of each resource type.
CONFIG = {
    "publication": {
        "manifest": "syn53478776",
        "portal_table": "syn21868591",
    },
    "dataset": {
        "manifest": "syn53478774",
        "portal_table": "syn21897968",
    },
    "tool": {
        "manifest": "syn53479671",
        "portal_table": "syn26127427",
    },
    "people": {"manifest": "syn38301033", "portal_table": "syn28073190"},
    "grant": {"manifest": "syn53259587", "portal_table": "syn21918972"},
    "education": {"manifest": "syn53651540", "portal_table": "syn51497305"}
}


def syn_login() -> synapseclient.Synapse:
    """Log into Synapse. If env variables not found, prompt user."""
    try:
        syn = synapseclient.login(silent=True)
    except synapseclient.core.exceptions.SynapseNoCredentialsError:
        print(
            ".synapseConfig not found; please manually provide your",
            "Synapse Personal Access Token (PAT). You can generate"
            "one at https://www.synapse.org/#!PersonalAccessTokens:0",
        )
        pat = getpass("Your Synapse PAT: ")
        syn = synapseclient.login(authToken=pat, silent=True)
    return syn


def get_args(resource: str) -> argparse.Namespace:
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser(description=f"Sync {resource} to the CCKP")
    parser.add_argument(
        "-m",
        "--manifest_id",
        type=str,
        default=CONFIG.get(resource).get("manifest"),
        help="Synapse ID of the manifest CSV file.",
    )
    parser.add_argument(
        "-t",
        "--portal_table_id",
        type=str,
        default=CONFIG.get(resource).get("portal_table"),
        help=(
            f"Sync to this specified table. (Default: "
            f"{CONFIG.get(resource).get('portal_table')})"
        ),
    )
    parser.add_argument(
        "-o",
        "--output_csv",
        type=str,
        default=f"./final_{resource}_table.csv",
        help="Filepath to output CSV.",
    )
    parser.add_argument("--dryrun", action="store_true")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Output all logs and interim tables.",
    )
    return parser.parse_args()


# TODO: check if we still need this function?
def sort_and_stringify_col(col: pd.Series) -> str:
    """Sort list col then join together as comma-separated string."""
    # Check column by looking at first row; if str, convert to list first.
    if isinstance(col.iloc[0], str):
        col = col.str.replace(", ", ",").str.split(",")
    return col.apply(lambda x: ",".join(map(str, sorted(x))))


def convert_to_stringlist(col: pd.Series) -> pd.Series:
    """Convert a string column to a list."""
    return col.str.replace(", ", ",").str.split(",")


def update_table(syn: synapseclient.Synapse, table_id: str, df: pd.DataFrame) -> None:
    """Update the portal table.

    Steps include:
        - creating a new table version
        - truncating the table
        - sync over rows from the latest manifest
    """

    today = datetime.today().strftime("%Y-%m-%d")
    print(f"Creating new table version with label: {today}...")
    syn.create_snapshot_version(table_id, label=today)

    print("Syncing table with latest data...\n")
    current_rows = syn.tableQuery(f"SELECT * FROM {table_id}")
    syn.delete(current_rows)
    new_rows = df.values.tolist()
    syn.store(synapseclient.Table(table_id, new_rows))
