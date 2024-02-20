"""Add Datasets to the Cancer Complexity Knowledge Portal (CCKP).

This script will sync over new datasets and its annotations to the
Datasets portal table. A Synapse Folder will also be created for each
new dataset in its respective grant Project.
"""

import argparse

# import re
from synapseclient import Table  # , Folder
import pandas as pd
import utils


def get_args():
    """Set up command-line interface and get arguments."""
    parser = argparse.ArgumentParser(description="Add new datasets to the CCKP")
    parser.add_argument(
        "-m",
        "--manifest_id",
        type=str,
        default="syn53478774",
        help="Synapse ID to the manifest CSV file.",
    )
    parser.add_argument(
        "-t",
        "--portal_table_id",
        type=str,
        default="syn21897968",
        help="Add datasets to this specified table. (Default: syn21897968)",
    )
    parser.add_argument(
        "-o",
        "--output_csv",
        type=str,
        default="./final_dataset_table.csv",
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


# def create_folder(syn, name, parent):
#     name = name.replace("/", "-")
#     folder = Folder(name, parent=parent)
#     folder = syn.store(folder)
#     return folder.id


def sort_and_stringify_col(col):
    """Sort list col then join together as comma-separated string."""
    # Check column by looking at first row; if str, convert to list first.
    if isinstance(col.iloc[0], str):
        col = col.str.replace(", ", ",").str.split(",")
    return col.apply(lambda x: ",".join(map(str, sorted(x))))


def convert_to_stringlist(col):
    """Convert a string column to a list."""
    return col.str.replace(", ", ",").str.split(",")


def add_missing_info(datasets, grants, pubs):
    """Add missing information into table before syncing.

    Returns:
        datasets: Data frame
    """
    datasets["link"] = [
        "".join(["[", d_id, "](", url, ")"]) if url else ""
        for d_id, url in zip(datasets["DatasetAlias"], datasets["DatasetUrl"])
    ]
    datasets["grantName"] = ""
    datasets["themes"] = ""
    datasets["consortia"] = ""
    datasets["pub"] = ""
    for _, row in datasets.iterrows():
        # if re.search(r"^syn\d+$", row["datasetAlias"]):
        #     folder_id = row["datasetAlias"]
        # else:
        #     grant_proj = grants[grants.grantNumber == row["datasetGrantNumber"][0]][
        #         "grantId"
        #     ].values[0]
        #     folder_id = ""
        #     folder_id = create_folder(syn, row["datasetAlias"], grant_proj)
        # datasets.at[_, "id"] = folder_id
        grant_names = []
        themes = set()
        consortia = set()
        for g in row["DatasetGrantNumber"].split(","):
            if g != "Affiliated/Non-Grant Associated":
                grant_names.append(
                    grants[grants.grantNumber == g]["grantName"].values[0]
                )
                themes.update(grants[grants.grantNumber == g]["theme"].values[0])
                consortia.update(
                    grants[grants.grantNumber == g]["consortium"].values[0]
                )
        datasets.at[_, "grantName"] = grant_names
        datasets.at[_, "themes"] = list(themes)
        datasets.at[_, "consortia"] = list(consortia)

        pub_titles = []
        for p in row["DatasetPubmedId"].split(","):
            p = p.strip()  # remove leading/trailing whitespace, if any
            try:
                pub_titles.append(
                    pubs[pubs.pubMedId == int(p)]["publicationTitle"]
                    .values[0]
                    .replace("\xa0", " ")
                )
            except (ValueError, IndexError):
                pass  # PMID not yet annotated or found in portal table
        datasets.at[_, "pub"] = pub_titles
    return datasets


def clean_table(datasets):
    """Clean up the table one final time."""

    # Convert string columns to string-list.
    for col in [
        "DatasetView_id",
        "DatasetFileFormats",
        "DatasetAssay",
        "DatasetSpecies",
        "DatasetTissue",
        "DatasetTumorType",
        "DatasetGrantNumber",
        "DatasetPubmedId",
    ]:
        datasets[col] = convert_to_stringlist(datasets[col])

    # Reorder columns to match the table order.
    col_order = [
        "DatasetView_id",
        "DatasetName",
        "DatasetAlias",
        "DatasetDescription",
        "DatasetDesign",
        "DatasetFileFormats",
        "DatasetAssay",
        "DatasetSpecies",
        "DatasetTissue",
        "DatasetTumorType",
        "themes",
        "consortia",
        "DatasetGrantNumber",
        "grantName",
        "DatasetPubmedId",
        "pub",
        "link",
    ]
    datasets = datasets[col_order].explode("DatasetView_id")  # FIXME?
    return datasets


def main():
    """Main function."""
    syn = utils.syn_login()
    args = get_args()

    if args.dryrun:
        print("\n❗❗❗ WARNING:", "dryrun is enabled (no updates will be done)\n")

    manifest = pd.read_csv(syn.get(args.manifest_id).path).fillna("")
    manifest.columns = manifest.columns.str.replace(" ", "")
    manifest["grantNumber"] = sort_and_stringify_col(manifest["DatasetGrantNumber"])
    if args.verbose:
        print("Preview of manifest CSV:\n" + "=" * 72)
        print(manifest)
        print()

    curr_datasets = syn.tableQuery(
        f"SELECT datasetAlias, grantNumber FROM {args.portal_table_id}"
    ).asDataFrame()
    try:
        curr_datasets["grantNumber"] = sort_and_stringify_col(
            curr_datasets["grantNumber"]
        )
    except IndexError:
        pass  # Destination table is empty.

    # Only add datasets not currently in the Datasets table, using
    # dataset alias + grant number to determine uniqueness. - FIXME?
    new_datasets = pd.merge(
        manifest,
        curr_datasets,
        how="left",
        left_on=["DatasetAlias", "DatasetGrantNumber"],
        right_on=["datasetAlias", "grantNumber"],
        indicator=True,
    ).query("_merge=='left_only'")
    if new_datasets.empty:
        print("🚫 No new datasets found!")
    else:
        print(f"🆕 {len(new_datasets)} new datasets found!\n")

        print("Processing dataset staging database...")
        grants = syn.tableQuery(
            "SELECT grantId, grantNumber, grantName, theme, consortium FROM syn21918972"
        ).asDataFrame()
        pubs = syn.tableQuery(
            "SELECT pubMedId, publicationTitle FROM syn21868591"
        ).asDataFrame()

        new_datasets = add_missing_info(new_datasets, grants, pubs)
        new_datasets = clean_table(new_datasets)
        if args.verbose:
            print("\nDataset(s) to be synced:\n" + "=" * 72)
            print(new_datasets)

        if not args.dryrun:
            schema = syn.get(args.portal_table_id)
            new_rows = new_datasets.values.tolist()
            syn.store(Table(schema, new_rows))

        print(f"\nSaving copy of final table to: {args.output_csv}...")
        new_datasets.to_csv(args.output_csv, index=False)
        print("\n\nDONE ✅")


if __name__ == "__main__":
    main()
