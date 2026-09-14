"""Tally Themes in Portal Tables

This script will get a count of themes across the grants and
consortiums in the CCKP. (Based on James' `nbs/portal_summary.Rmd`)
"""

import re

from synapseclient import Synapse
from synapseclient.models import Table
import pandas as pd

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
        .query(query=query, synapse_client=syn)
        .explode("theme")
        .groupby("theme")
        .count()
        .rename(columns={colname: "totalCount"})
    )


def tally_by_consortium(grants):
    """Portal - Consortium Counts (syn21641485)"""
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


def update_table(syn, table_id, updated_table):
    """Truncate table then add new rows."""
    table = Table(id=table_id)
    table.delete_rows(query=f"SELECT * FROM {table_id}", synapse_client=syn)

    # Strip embedded newlines/carriage returns from free-text cells. A literal
    # line break inside a quoted CSV field survives synapseclient's row
    # serialization (csv.writer with QUOTE_NONNUMERIC), but Synapse's
    # server-side bulk CSV ingestion splits large uploads by scanning for
    # newlines rather than parsing quotes, which corrupts the batch containing
    # that row into blank rows. See the same fix in portal_tables/utils.py
    # (row-version 2230/2231 duplicated blank rows in syn21897968).
    updated_table = updated_table.map(
        lambda v: re.sub(r"[\r\n]+", " ", v) if isinstance(v, str) else v
    )
    table.store_rows(values=updated_table, synapse_client=syn)


def main():
    """Main function."""
    syn = Synapse()
    syn.login(silent=True)

    # Table of theme names and their descriptions.
    themes = (
        Table(id=THEMES)
        .query(query=f"SELECT displayName, description FROM {THEMES}", synapse_client=syn)
        .rename(columns={"displayName": "theme", "description": "themeDescription"})
        .set_index("theme")
    )
    grants = Table(id=GRANTS).query(
        query=f"SELECT grantId, grantNumber, consortium, theme FROM {GRANTS}",
        synapse_client=syn,
    )

    consortium_counts = tally_by_consortium(grants)
    theme_consortium_counts = tally_by_theme_consortium(grants, themes)
    theme_counts = tally_by_group(syn, themes)

    update_table(syn, CONSORTIUM_CTS, consortium_counts)
    update_table(syn, CON_THEME_CTS, theme_consortium_counts)
    update_table(syn, THEME_CTS, theme_counts)


if __name__ == "__main__":
    main()
