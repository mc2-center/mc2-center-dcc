from getpass import getpass
from datetime import datetime

import synapseclient
import pandas as pd
import os


def syn_login() -> synapseclient.Synapse:
    """Log into Synapse. If env variables not found, prompt user.

    Returns:
        syn: Synapse object
    """
    try:
        syn = synapseclient.login(
            authToken=os.getenv('SYNAPSE_AUTH_TOKEN'),
            silent=True)
    except synapseclient.core.exceptions.SynapseNoCredentialsError:
        print(
            "Credentials not found; please manually provide your",
            "Synapse Personal Access Token (PAT). You can generate"
            "one at https://www.synapse.org/#!PersonalAccessTokens:0",
        )
        pat = getpass("Your Synapse PAT: ")
        syn = synapseclient.login(authToken=pat, silent=True)
    return syn


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
