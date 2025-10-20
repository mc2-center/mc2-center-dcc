"""Add Datasets to the Cancer Complexity Knowledge Portal (CCKP).

This script will "sync" the dataset manifest table to the Dataset
portal table, by first truncating the table, then re-adding the rows.
"""

import pandas as pd
import re
import synapseclient
import utils

from synapseclient.models import Dataset

def add_missing_info(
    syn: synapseclient.Synapse, datasets: pd.DataFrame, grants: pd.DataFrame, pubs: pd.DataFrame
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
    datasets["version"] = ""
    for _, row in datasets.iterrows():
        grant_names = []
        themes = set()
        consortia = set()
        for g in row["GrantViewKey"].split(","):
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
        
        try:
            dataset = Dataset(id=row["DatasetAlias"]).get() if re.match(r'syn\d{,9}', row["DatasetAlias"]) is not None else None
        except synapseclient.core.exceptions.SynapseUnmetAccessRestrictions as e:
            print(f"Encountered error: {e}")
            pass
        version = dataset.version_number if dataset is not None and dataset.version_number is not None else 1
        datasets.at[_, "version"] = int(version)
        
        pub_titles = []
        pub_doi = []
        for p in row["PublicationViewKey"].split(","):
            p = p.strip()  # Remove leading/trailing whitespace, if any
            try:
                pub_titles.append(
                    pubs[pubs.pubMedId == int(p)]["publicationTitle"]
                    .values[0]
                    .replace("\xa0", " ")
                )
                pub_doi.append(
                    pubs[pubs.pubMedId == int(p)]["doi"]
                    .values[0]
                )
            except (ValueError, IndexError):
                pass  # PMID not yet annotated or found in portal table
        datasets.at[_, "pub"] = list(set(pub_titles))
        if not row["DatasetDoi"]:  # If dataset does not have a pre-curated DOI, add a publication DOI
            try:
                datasets.at[_, "DatasetDoi"] = pub_doi[0]  # Use first DOI identified
            except IndexError:
                datasets.at[_, "DatasetDoi"] = "DOI Not Available"
    return datasets


def clean_table(df: pd.DataFrame) -> pd.DataFrame:
    """Clean up the table one final time."""

    df = df.rename(columns={
        "GrantViewKey": "DatasetGrantNumber",
        "PublicationViewKey": "DatasetPubmedId"
    })

    # Convert string columns to string-list.
    cols = [
        "DatasetView_id",
        "DatasetFileFormats",
        "DatasetAssay",
        "DatasetSpecies",
        "DatasetTissue",
        "DatasetTumorType",
        "DatasetGrantNumber",
        "DatasetPubmedId",
        "iconTags"
    ]
    
    for col in cols:
        df[col] = utils.convert_to_stringlist(df[col])
    
    for _,row in df.iterrows():
        for col in cols:
           df.at[_, col] = list(set(row[col]))

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
        "themes",
        "DatasetTumorType",
        "consortia",
        "DatasetGrantNumber",
        "grantName",
        "DatasetPubmedId",
        "pub",
        "link",
        "DatasetDoi",
        "iconTags",
        "version"
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
        "SELECT doi, pubMedId, publicationTitle FROM syn21868591"
    ).asDataFrame()

    database = add_missing_info(syn, manifest, grants, pubs)
    final_database = clean_table(database)
    if args.verbose:
        print("\n🔍 Dataset(s) to be synced:\n" + "=" * 72)
        print(final_database)
        print()

    if not args.dryrun:
        utils.update_table(syn, args.portal_table_id, final_database)
        print()

    if not args.noprint:
        print(f"📄 Saving copy of final table to: {args.output_csv}...")
        final_database.to_csv(args.output_csv, index=False)
    print("\n\nDONE ✅")


if __name__ == "__main__":
    main()
