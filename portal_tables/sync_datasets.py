"""Add Datasets to the Cancer Complexity Knowledge Portal (CCKP).

This script will "sync" the dataset manifest table to the Dataset
portal table, by first truncating the table, then re-adding the rows.
"""

import logging
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import re
import synapseclient
from synapseclient import Synapse, operations
from synapseclient.models import Table
from cells import is_placeholder, parse_list
import crosswalk
import model_contract
import utils

logger = logging.getLogger(__name__)

COMPONENT = "DatasetView"

# Manifest columns that are not model attributes but are passed through.
MANIFEST_PASSTHROUGH = ("iconTags",)

# Number of Synapse entity reads that run at the same time.
MAX_READERS = 4

def duo_code_to_id() -> dict[str, str]:
    """Short code (``RTN``) -> DUO ID (``DUO:0000029``).

    Built from the data-models duo.csv codes (``USE``, ``RST``) plus the
    official DUO shorthands (``US``, ``RS``), so both spellings map to one ID.
    """
    mapping = {code: duo_id for duo_id, code in model_contract.duo_id_to_code().items()}
    mapping.update(utils.DUO_SHORTHANDS)
    return mapping


PENDING_ACCESS_TEXT = "Access information was not provided for this dataset"
NO_ACCESS_TEXT = "No access information available"


def translate_access(code: str, code_to_id: dict) -> str | None:
    """Access text for one data use code, or None if the code has no mapping.

    Manifests store ``DUO:`` IDs, ``DUOPlus*`` and ``Pending Annotation``,
    which are looked up directly (an aliased ID such as ``DUO:00000044`` is
    resolved by ``translate_duo``). Short codes (still held by the live manifest
    until it is migrated) are first translated to their DUO ID. A code with
    no entry is logged as a warning and dropped.
    """
    code = code.strip()
    if is_placeholder(code):
        return PENDING_ACCESS_TEXT
    key = code_to_id.get(code, code)
    try:
        return utils.translate_duo(key)
    except KeyError:
        logger.warning("No access information mapped for data use code %r", code)
        return None


def _get_croissant_versions(syn: Synapse) -> dict:
    """Return a dict mapping dataset synID -> latest Croissant dataset_version."""
    croissant_table = utils.CONFIG["croissant"]["table"]
    df = Table(id=croissant_table).query(
        query=f"SELECT dataset, dataset_version FROM {croissant_table}",
        synapse_client=syn,
    )
    return df.groupby("dataset")["dataset_version"].max().to_dict()


def _fetch_entity(syn: Synapse, dataset_id: str):
    """Read one entity without its file; return None if Synapse refuses."""
    try:
        # operations.get() looks up the entity's actual type on Synapse
        # first and dispatches to the matching model (File, Folder,
        # Dataset, etc.), unlike the Dataset model's .get() directly,
        # which errors out on DatasetViewIds that reference non-Dataset
        # entities.
        return operations.get(
            dataset_id,
            file_options=operations.FileOptions(download_file=False),
            synapse_client=syn,
        )
    except (synapseclient.core.exceptions.SynapseUnmetAccessRestrictions, synapseclient.core.exceptions.SynapseHTTPError) as e:
        print(f"Encountered error: {e}")
        return None


def _fetch_entities(syn: Synapse, dataset_ids) -> dict:
    """Read each distinct syn ID once, in parallel; return dict ID -> entity or None."""
    ids = list(dict.fromkeys(i for i in dataset_ids if re.match(r'syn\d{,9}', i) is not None))
    with ThreadPoolExecutor(max_workers=MAX_READERS) as pool:
        return dict(zip(ids, pool.map(lambda i: _fetch_entity(syn, i), ids)))


def add_missing_info(
    syn: Synapse, datasets: pd.DataFrame, grants: pd.DataFrame, pubs: pd.DataFrame
) -> pd.DataFrame:
    """Add missing information into table before syncing."""
    code_to_id = duo_code_to_id()
    datasets["link"] = [
        "".join(["[", d_id, "](", url, ")"]) if url else ""
        for d_id, url in zip(datasets["DatasetAlias"], datasets["DatasetUrl"])
    ]

    datasets["grantName"] = ""
    datasets["theme"] = ""
    datasets["consortium"] = ""
    datasets["pub"] = ""
    datasets["pubYear"] = None
    datasets["version"] = ""
    datasets["sourceRepository"] = ""
    datasets["downloadType"] = ""
    datasets["downloadSynId"] = ""

    # Pre-fetch Croissant versions so the portal `version` column matches the
    # version the Croissant DAG actually processed, not just the latest snapshot.
    croissant_versions = _get_croissant_versions(syn)

    # Read all entities first, in parallel, instead of one call per row.
    entities = _fetch_entities(
        syn, [(parse_list(v) or [""])[0] for v in datasets["DatasetViewId"]]
    )

    for _, row in datasets.iterrows():
        grant_names = []
        themes = []
        consortia = []
        for g in parse_list(row["GrantViewKey"]):
            if g != "Affiliated/Non-Grant Associated":
                grant_names.append(
                    grants[grants.grantNumber == g]["grantName"].values[0]
                )
                themes.extend(grants[grants.grantNumber == g]["theme"].values[0])
                consortia.extend(
                    grants[grants.grantNumber == g]["consortium"].values[0]
                )
        datasets.at[_, "grantName"] = grant_names
        datasets.at[_, "theme"] = list(dict.fromkeys(themes))
        datasets.at[_, "consortium"] = list(dict.fromkeys(consortia))

        # Use the actual Synapse entity ID (DatasetViewId), not DatasetAlias,
        # since DatasetAlias is often an external accession (e.g. a GEO
        # GSE ID) rather than a syn ID for externally-hosted datasets.
        dataset_id = (parse_list(row["DatasetViewId"]) or [""])[0]
        entity = entities.get(dataset_id)
        # Prefer the Croissant-processed version so the portal button appears
        # correctly. Fall back to (version_number - 1) for datasets not yet
        # in the Croissant table.
        if dataset_id in croissant_versions:
            version = int(croissant_versions[dataset_id])
        else:
            # Not every entity type carries a version (e.g. Folder), so fall
            # back to the default rather than assuming the attribute exists.
            # operations.get() returns a dict bundle for some types (e.g.
            # RecordSet); the version is then under entity["entity"].
            if isinstance(entity, dict):
                entity_version = (entity.get("entity") or {}).get("versionNumber")
            else:
                entity_version = getattr(entity, "version_number", None) if entity is not None else None
            version = int(entity_version) - 1 if entity_version is not None else 1
        datasets.at[_, "version"] = version
        
        pub_titles = []
        pub_doi = []
        pub_years = []
        for p in parse_list(row["PublicationViewKey"]):
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
        datasets.at[_, "pub"] = list(dict.fromkeys(pub_titles))
        # Use the most recent linked publication's year as the dataset's
        # release date for sorting; leave as None if no publication matched.
        datasets.at[_, "pubYear"] = max(pub_years) if pub_years else None
        if not row["DatasetDoi"]:  # If dataset does not have a pre-curated DOI, add a publication DOI
            usable = [d for d in pub_doi if isinstance(d, str) and d.strip()]
            # First non-blank publication DOI (NaN/blank are skipped)
            datasets.at[_, "DatasetDoi"] = usable[0].strip() if usable else utils.DOI_NOT_AVAILABLE
        if str(datasets.at[_, "DatasetDoi"]).strip() == utils.NO_DOI_LISTED:
            datasets.at[_, "DatasetDoi"] = utils.DOI_NOT_AVAILABLE
        d = [t for code in parse_list(row["DatasetDataUseCodes"]) if (t := translate_access(code, code_to_id))]
        d = d or [NO_ACCESS_TEXT]
        d = ["Open Access available through GEO"] if "GSE" in row["DatasetAlias"] else d
        # Keep the texts as a list: several contain commas, so joining them
        # here and splitting again in clean_table would break them apart.
        datasets.at[_, "DatasetDataUseCodes"] = d

        source_repo = utils.extract_map_repository(row["DatasetUrl"], row["DatasetAlias"])
        download_type, download_id = utils.identify_download_type(
            syn, row, source_repo, entity=entity
        )
        
        datasets.at[_, "sourceRepository"] = source_repo
        datasets.at[_, "downloadType"] = download_type
        datasets.at[_, "downloadSynId"] = download_id

    return datasets


def clean_table(df: pd.DataFrame, live_types: dict) -> pd.DataFrame:
    """Clean up the table one final time and return it in portal shape."""

    # Convert string columns to string-list.
    cols = [
        "DatasetViewId",
        "DatasetFileFormats",
        "Assay",
        "Species",
        "Tissue",
        "TumorType",
        "GrantViewKey",
        "PublicationViewKey",
        "iconTags",
    ]

    for col in cols:
        df[col] = df[col].map(parse_list)

    # DatasetDataUseCodes already holds a list of access texts (which may
    # contain commas), so it is deduped below but never string-split.
    for _, row in df.iterrows():
        for col in cols + ["DatasetDataUseCodes"]:
            # Dedupe keeping first-seen order: set() order is arbitrary, which
            # made the portal's single DatasetViewId differ between runs.
            df.at[_, col] = list(dict.fromkeys(row[col]))

    # We only need one synID for the portal table. See
    # https://github.com/mc2-center/mc2-center-dcc/pull/41#issuecomment-1955119623
    # for more context.
    df["DatasetViewId"] = df["DatasetViewId"].str[0]

    # Rank rows by how many content fields are still unannotated, so that
    # complete entries sort ahead of incomplete ones within the same
    # publication year.
    content_cols = [
        "DatasetDescription",
        "DatasetDesign",
        "DatasetFileFormats",
        "Assay",
        "Species",
        "Tissue",
        "theme",
        "TumorType",
        "consortium",
        "GrantViewKey",
        "grantName",
        "PublicationViewKey",
        "pub",
        "DatasetDoi",
        "iconTags",
        "DatasetDataUseCodes",
    ]

    def _is_incomplete(value) -> bool:
        """A field is incomplete if it's empty or only holds 'Pending Annotation'."""
        return not parse_list(value) or is_placeholder(value)

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

    # Rename, order and type the columns to match the live portal table.
    return crosswalk.to_portal(df, COMPONENT, live_types)


def main():
    """Main function."""
    syn = utils.syn_login()
    args = utils.get_args("dataset")

    if args.dryrun:
        print("\n❗❗❗ WARNING:", "dryrun is enabled (no updates will be done)\n")

    manifest = utils.fill_blank(utils.read_sync_manifest(syn, args))
    manifest = model_contract.restore_display_names(manifest, COMPONENT, MANIFEST_PASSTHROUGH)
    manifest = model_contract.rename_legacy_headers(manifest, COMPONENT)
    manifest = model_contract.normalize_headers(
        manifest, to="keys", passthrough=MANIFEST_PASSTHROUGH
    )
    if args.verbose:
        print("🔍 Preview of manifest CSV:\n" + "=" * 72)
        print(manifest)
        print()

    print("Processing dataset staging database...")
    grants = Table(id=utils.portal_table("grant")).query(
        query=f"SELECT grantId, grantNumber, grantName, theme, consortium FROM {utils.portal_table('grant')}",
        synapse_client=syn,
    )
    pubs = Table(id=utils.portal_table("publication")).query(
        query=f"SELECT doi, pubMedId, publicationTitle, publicationYear FROM {utils.portal_table('publication')}",
        synapse_client=syn,
    )

    database = add_missing_info(syn, manifest, grants, pubs)
    live_types = crosswalk.check_crosswalk(syn, args.portal_table_id, COMPONENT)
    final_database = clean_table(database, live_types)
    if args.verbose:
        print("\n🔍 Dataset(s) to be synced:\n" + "=" * 72)
        print(final_database)
        print()

    if not args.dryrun:
        utils.update_table(syn, args.portal_table_id, final_database, component=COMPONENT)
        print()

    if not args.noprint:
        print(f"📄 Saving copy of final table to: {args.output_csv}...")
        final_database.to_csv(args.output_csv, index=False)
    print("\n\nDONE ✅")


if __name__ == "__main__":
    main()
