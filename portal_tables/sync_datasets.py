"""Add Datasets to the Cancer Complexity Knowledge Portal (CCKP).

This script will sync over new datasets and its annotations to the
Datasets portal table. A Synapse Folder will also be created for each
new dataset in its respective grant Project.
"""
import pandas as pd
import utils


def add_missing_info(
    datasets: pd.DataFrame, grants: pd.DataFrame, pubs: pd.DataFrame
) -> pd.DataFrame:
    """Add missing information into table before syncing."""
    datasets["link"] = [
        "".join(["[", d_id, "](", url, ")"]) if url else ""
        for d_id, url in zip(datasets["DatasetAlias"], datasets["DatasetUrl"])
    ]
    datasets["grantName"] = ""
    datasets["themes"] = ""
    datasets["consortia"] = ""
    datasets["pub"] = ""
    for _, row in datasets.iterrows():
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
            p = p.strip()  # Remove leading/trailing whitespace, if any
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


def clean_table(df: pd.DataFrame) -> pd.DataFrame:
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
        df[col] = utils.convert_to_stringlist(df[col])

    # We only need one synID for the portal table. See
    # https://github.com/mc2-center/mc2-center-dcc/pull/41#issuecomment-1955119623
    # for more context.
    df["DatasetView_id"] = df["DatasetView_id"].str[0]

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
    return df[col_order]


def main():
    """Main function."""
    syn = utils.syn_login()
    args = utils.get_args("dataset")

    if args.dryrun:
        print("\n❗❗❗ WARNING:", "dryrun is enabled (no updates will be done)\n")

    manifest = pd.read_csv(syn.get(args.manifest_id).path).fillna("")
    manifest.columns = manifest.columns.str.replace(" ", "")
    if args.verbose:
        print("🔍 Preview of manifest CSV:\n" + "=" * 72)
        print(manifest)
        print()

    print("Processing dataset staging database...")
    grants = syn.tableQuery(
        "SELECT grantId, grantNumber, grantName, theme, consortium FROM syn21918972"
    ).asDataFrame()
    pubs = syn.tableQuery(
        "SELECT pubMedId, publicationTitle FROM syn21868591"
    ).asDataFrame()

    database = add_missing_info(manifest, grants, pubs)
    final_database = clean_table(database)
    if args.verbose:
        print("\n🔍 Dataset(s) to be synced:\n" + "=" * 72)
        print(final_database)
        print()

    if not args.dryrun:
        utils.update_table(syn, args.portal_table_id, final_database)
        print()

    print(f"📄 Saving copy of final table to: {args.output_csv}...")
    final_database.to_csv(args.output_csv, index=False)
    print("\n\nDONE ✅")


if __name__ == "__main__":
    main()
