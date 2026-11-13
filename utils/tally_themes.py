"""Tally Themes in Portal Tables

This script will get a count of themes across the grants and
consortiums in the CCKP. (Based on James' `nbs/portal_summary.Rmd`)
"""

import argparse
import os

from synapseclient.models import Table
import pandas as pd

import importlib.util
# removed when the mc2dcc package (CLI consolidation plan) lands
# portal_tables/utils.py, loaded by path: "utils" would collide with this package
_spec = importlib.util.spec_from_file_location(
    "portal_tables_utils",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "portal_tables", "utils.py"),
)
_portal_utils = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_portal_utils)
syn_login = _portal_utils.syn_login

# Source table IDs
GRANTS = "syn21918972"
THEMES = "syn35369899"
PUBS = "syn21868591"
DATASETS = "syn21897968"
TOOLS = "syn26127427"

# Destination table IDs
CONSORTIUM_CTS = "syn21641485"
CON_THEME_CTS = "syn21649281"
THEME_CTS = "syn21639584"


def _add_missing_themes(themes, df, label):
    """Helper function: add missing themes with count = 0."""
    missing_themes = themes[~themes.index.isin(df.index)].index.tolist()
    for theme in missing_themes:
        new_row = pd.DataFrame(
            [[0, label]], columns=["totalCount", "groupBy"], index=[theme]
        )
        df = pd.concat([df, new_row])
    return df


def _tally_portal_table(syn, table_id, colname, clause=False):
    """Helper function: tally themes in portal table."""
    query = f"SELECT {colname}, theme FROM {table_id}"
    if clause:
        query += " WHERE portalDisplay = true"
    return (
        Table(id=table_id)
        .query(query=query, include_row_id_and_row_version=False, synapse_client=syn)
        .explode("theme")
        .groupby("theme")
        .count()
        .rename(columns={colname: "totalCount"})
    )


def tally_by_consortium(grants):
    """Portal - Consortium Counts (syn21641485)"""
    # An empty list joins to "", so grants without a consortium are counted under "".
    grants["consortium"] = grants["consortium"].str.join(", ")
    return (
        grants[["grantId", "consortium"]]
        .groupby("consortium")
        .count()
        .rename(columns={"grantId": "totalCount"})
        .assign(groupBy="grants")
        .reset_index()
        .reindex(columns=["consortium", "groupBy", "totalCount"])
    )


def tally_by_theme_consortium(grants, themes):
    """Portal - Consortium-Theme Counts (syn21649281)"""
    res = (
        grants[["grantId", "consortium", "theme"]]
        .explode("theme")
        .groupby(["theme", "consortium"])
        .count()
        .rename(columns={"grantId": "totalCount"})
        .assign(groupBy="grants")
        .join(themes)
        .fillna("")
        .reset_index()
        .reindex(
            columns=["theme", "themeDescription", "consortium", "groupBy", "totalCount"]
        )
    )
    return res[~res["theme"].isin(["Computational Resource"])]


def tally_by_group(syn, themes):
    """Portal - Theme Counts (syn21639584)"""

    # get theme counts in publications
    theme_pubs = _tally_portal_table(syn, PUBS, "pubMedId").assign(
        groupBy="publications"
    )
    theme_pubs = _add_missing_themes(themes, theme_pubs, "publications")

    # get theme counts in datasets
    theme_datasets = _tally_portal_table(syn, DATASETS, "pubMedId").assign(
        groupBy="datasets"
    )
    theme_datasets = _add_missing_themes(themes, theme_datasets, "datasets")

    # get theme counts in tools
    theme_tools = _tally_portal_table(syn, TOOLS, "toolName", clause=True).assign(
        groupBy="tools"
    )
    theme_tools = _add_missing_themes(themes, theme_tools, "tools")

    # concat results together
    res = (
        pd.concat([theme_pubs, theme_datasets, theme_tools])
        .join(themes)
        .reset_index()
        .rename(columns={"index": "theme"})
        .sort_values(["groupBy", "theme"])
        .reindex(columns=["theme", "themeDescription", "groupBy", "totalCount"])
    )
    return res[~res["theme"].isin(["Computational Resource"])]


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser(description="Tally themes in portal tables.")
    parser.add_argument(
        "--dryrun",
        action="store_true",
        help="Compute and print the tallies and write local CSVs, but make no "
        "Synapse writes (the three destination tables are not truncated or updated).",
    )
    parser.add_argument(
        "--outdir",
        default=".",
        help="Directory for the local CSVs written by --dryrun. (Default: .)",
    )
    return parser.parse_args()


def main():
    """Main function."""
    args = get_args()
    syn = syn_login()

    # Table of theme names and their descriptions.
    themes = (
        Table(id=THEMES)
        .query(
            query=f"SELECT displayName, description FROM {THEMES}",
            include_row_id_and_row_version=False,
            synapse_client=syn,
        )
        .rename(columns={"displayName": "theme", "description": "themeDescription"})
        .set_index("theme")
    )
    grants = Table(id=GRANTS).query(
        query=f"SELECT grantId, grantNumber, consortium, theme FROM {GRANTS}",
        include_row_id_and_row_version=False,
        synapse_client=syn,
    )

    consortium_counts = tally_by_consortium(grants)
    theme_consortium_counts = tally_by_theme_consortium(grants, themes)
    theme_counts = tally_by_group(syn, themes)

    results = {
        CONSORTIUM_CTS: ("consortium_counts", consortium_counts),
        CON_THEME_CTS: ("theme_consortium_counts", theme_consortium_counts),
        THEME_CTS: ("theme_counts", theme_counts),
    }

    if args.dryrun:
        os.makedirs(args.outdir, exist_ok=True)
        for table_id, (name, df) in results.items():
            path = os.path.join(args.outdir, f"{name}.csv")
            df.to_csv(path, index=False)
            print(f"[dryrun] {name}: {len(df)} rows for {table_id} -> {path}")
        print("[dryrun] No Synapse tables were updated.")
        return

    for table_id, (_, df) in results.items():
        _portal_utils.update_table(syn, table_id, df)


if __name__ == "__main__":
    main()
