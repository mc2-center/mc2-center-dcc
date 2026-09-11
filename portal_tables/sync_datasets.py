"""Add Datasets to the Cancer Complexity Knowledge Portal (CCKP).

This script will "sync" the dataset manifest table to the Dataset
portal table, by first truncating the table, then re-adding the rows.
"""

import pandas as pd
import re
import synapseclient
import utils

def _get_croissant_versions(syn: synapseclient.Synapse) -> dict:
    """Return a dict mapping dataset synID -> latest Croissant dataset_version."""
    df = syn.tableQuery(
        "SELECT dataset, dataset_version FROM syn65903895"
    ).asDataFrame()
    return df.groupby("dataset")["dataset_version"].max().to_dict()


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
    datasets["pubYear"] = None
    datasets["version"] = ""
    datasets["sourceRepository"] = ""
    datasets["downloadType"] = ""
    datasets["downloadSynId"] = ""

    # Pre-fetch Croissant versions so the portal `version` column matches the
    # version the Croissant DAG actually processed, not just the latest snapshot.
    croissant_versions = _get_croissant_versions(syn)

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

        # Use the actual Synapse entity ID (DatasetView_id), not DatasetAlias,
        # since DatasetAlias is often an external accession (e.g. a GEO
        # GSE ID) rather than a syn ID for externally-hosted datasets.
        dataset_id = row["DatasetView_id"].split(",")[0]
        try:
            # Plain syn.get() handles any entity type (File, Folder, Dataset,
            # etc.), unlike the Dataset model's .get(), which errors out on
            # DatasetView_ids that reference non-Dataset entities.
            entity = syn.get(dataset_id, downloadFile=False) if re.match(r'syn\d{,9}', dataset_id) is not None else None
        except (synapseclient.core.exceptions.SynapseUnmetAccessRestrictions, synapseclient.core.exceptions.SynapseHTTPError) as e:
            print(f"Encountered error: {e}")
            entity = None
        # Prefer the Croissant-processed version so the portal button appears
        # correctly. Fall back to (versionNumber - 1) for datasets not yet
        # in the Croissant table.
        if dataset_id in croissant_versions:
            version = int(croissant_versions[dataset_id])
        else:
            # Not every entity type carries a version (e.g. Folder), so fall
            # back to the default rather than assuming the attribute exists.
            entity_version = getattr(entity, "versionNumber", None) if entity is not None else None
            version = int(entity_version) - 1 if entity_version is not None else 1
        datasets.at[_, "version"] = version
        
        pub_titles = []
        pub_doi = []
        pub_years = []
        for p in row["PublicationViewKey"].split(","):
            p = p.strip()  # Remove leading/trailing whitespace, if any
            if len(p) < 4:
                pmid_list = [e for elem in "".join(row["PublicationViewKey"].split(",")) for e in elem.split()]
                p = "".join(pmid_list[0:8])
                datasets.at[_, "PublicationViewKey"] = p
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
                pub_years.append(
                    int(pubs[pubs.pubMedId == int(p)]["publicationYear"].values[0])
                )
            except (ValueError, IndexError):
                pass  # PMID not yet annotated or found in portal table
        datasets.at[_, "pub"] = list(set(pub_titles))
        # Use the most recent linked publication's year as the dataset's
        # release date for sorting; leave as None if no publication matched.
        datasets.at[_, "pubYear"] = max(pub_years) if pub_years else None
        if not row["DatasetDoi"]:  # If dataset does not have a pre-curated DOI, add a publication DOI
            try:
                datasets.at[_, "DatasetDoi"] = pub_doi[0]  # Use first DOI identified
            except IndexError:
                datasets.at[_, "DatasetDoi"] = "DOI Not Available"
        d = row["DataUseCodes"].split(",")
        try:
            d = [utils.translate_duo(code.strip()) for code in d]
        except KeyError as e:
            continue
        d = ["Open Access available through GEO"] if "GSE" in row["DatasetAlias"] else d
        datasets.at[_, "DataUseCodes"] = ",".join(d)
        
        source_repo = utils.extract_map_repository(row["DatasetUrl"], row["DatasetAlias"])
        download_type, download_id = utils.identify_download_type(syn, row, source_repo)
        
        datasets.at[_, "sourceRepository"] = source_repo
        datasets.at[_, "downloadType"] = download_type
        datasets.at[_, "downloadSynId"] = download_id

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
        "iconTags",
        "DataUseCodes"
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

    # Rank rows by how many content fields are still unannotated, so that
    # complete entries sort ahead of incomplete ones within the same
    # publication year.
    content_cols = [
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
        "DatasetDoi",
        "iconTags",
        "DataUseCodes",
    ]

    def _is_incomplete(value) -> bool:
        """A field is incomplete if it's empty or only holds 'Pending Annotation'."""
        if isinstance(value, list):
            return not [v for v in value if v not in ("", "Pending Annotation")]
        return value in (None, "", "Pending Annotation") or pd.isna(value)

    df["_incompleteCount"] = df[content_cols].apply(
        lambda row: sum(_is_incomplete(v) for v in row), axis=1
    )

    # Order by publication release date (most recent first), with datasets
    # that have no linked publication placed at the end, then by completeness
    # (fewer unannotated fields first) to break ties.
    df = df.sort_values(
        by=["pubYear", "_incompleteCount"],
        ascending=[False, True],
        na_position="last",
    )

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
        "version",
        "DataUseCodes",
        "sourceRepository",
        "downloadType",
        "downloadSynId"     
    ]

    return df[col_order]


def main():
    """Main function."""
    syn = utils.syn_login()
    args = utils.get_args("dataset")

    if args.dryrun:
        print("\n❗❗❗ WARNING:", "dryrun is enabled (no updates will be done)\n")

    manifest = pd.read_csv(syn.get(args.manifest_id).path, dtype=str).fillna("")
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
        "SELECT doi, pubMedId, publicationTitle, publicationYear FROM syn21868591"
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
