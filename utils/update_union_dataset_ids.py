"""update_union_dataset_ids.py

For each DatasetView grant manifest table in the UNION, replaces stale
Synapse entity IDs (Folder/File entities) in the DatasetView_id column
with the correct Dataset entity IDs from the DatasetCollection (syn77025587).

This fixes the upstream source of the stale IDs so that future UNION syncs
start from correct data. The fix in union_qc.py (PR #172) handles any
remaining stale submissions at merge time, but updating the source tables
prevents those stale IDs from persisting in the UNION indefinitely.

Background:
    create_entity_links.py creates proper Synapse Dataset entities for GEO
    accessions and updates the local manifest CSV. If that updated manifest
    was not (re-)uploaded to the grant's Synapse manifest table, the UNION
    retains the original Folder/File ID. Each sync then overwrites the
    correct Dataset entity ID in the portal with the stale UNION value,
    breaking portal links.

Usage:
    # Dry run first — reports changes, writes nothing to Synapse:
    python utils/update_union_dataset_ids.py --dryrun

    # Apply fixes:
    python utils/update_union_dataset_ids.py

    # Limit to specific table IDs (useful for targeted re-runs):
    python utils/update_union_dataset_ids.py --tables syn52820331 syn52820332

After running (non-dryrun), re-run Step 1 (merge_tables.py) on the next
sync cycle to refresh the UNION materialized view with the corrected data.

author: aditya.nath, orion.banks
"""

import argparse
import json
import synapseclient
import pandas as pd
from pathlib import Path

# Synapse ID of the DatasetCollection holding all proper Dataset entities.
_COLLECTION_SYNID = "syn77025587"
# Parent project of all Dataset entities (CCKP Indexed Datasets).
_CENTRAL_PROJECT_SYNID = "syn76986881"
# Entity view listing all grant-specific manifest table IDs.
_INDEX_SYNID = "syn52750482"
# Value of the 'name' column in the index view for DatasetView manifest tables.
_MANIFEST_TYPE = "datasetview_synapse_storage_manifest_table"


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Update stale DatasetView_id values in UNION grant manifest tables "
            "to use proper Synapse Dataset entity IDs."
        )
    )
    parser.add_argument(
        "--dryrun",
        action="store_true",
        help="Report changes without writing anything to Synapse.",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=None,
        metavar="SYN_ID",
        help=(
            "Limit to specific grant manifest table IDs. "
            "If omitted, all tables in the UNION index are checked."
        ),
    )
    return parser.parse_args()


def get_dataset_entity_map(syn: synapseclient.Synapse) -> dict[str, str]:
    """Return {GEO accession name: Dataset entity synID} from CCKP Indexed Datasets.

    Pages through all Dataset entity children of the central project so the
    result is complete regardless of how many entities exist.
    """
    result: dict[str, str] = {}
    token = None
    while True:
        params: dict = {"parentId": _CENTRAL_PROJECT_SYNID, "includeTypes": ["dataset"]}
        if token:
            params["nextPageToken"] = token
        resp = syn.restPOST("/entity/children", body=json.dumps(params))
        for child in resp.get("page", []):
            result[child["name"]] = child["id"]
        token = resp.get("nextPageToken")
        if not token:
            break
    return result


def get_manifest_table_ids(syn: synapseclient.Synapse) -> list[str]:
    """Return all DatasetView grant manifest table synIDs from the UNION index."""
    df = syn.tableQuery(
        f"SELECT id FROM {_INDEX_SYNID} WHERE name='{_MANIFEST_TYPE}'"
    ).asDataFrame()
    return df.iloc[:, 0].tolist()


def fix_table(
    syn: synapseclient.Synapse,
    table_id: str,
    entity_map: dict[str, str],
    dryrun: bool,
) -> list[dict]:
    """Fix stale DatasetView_id values in one grant manifest table.

    Queries the table, identifies rows where the Dataset Alias has a known
    Dataset entity ID that differs from the stored DatasetView_id, and updates
    those rows in place. ROW_ID/ROW_VERSION metadata from the query result is
    preserved in the DataFrame index so that syn.store() issues updates rather
    than inserts.

    Returns a list of change records: {table, alias, old_id, new_id}.
    """
    try:
        results = syn.tableQuery(f"SELECT * FROM {table_id}")
        df = results.asDataFrame()
    except Exception as e:
        print(f"  WARNING: could not query {table_id}: {e}")
        return []

    if "DatasetView_id" not in df.columns or "Dataset Alias" not in df.columns:
        return []

    changes: list[dict] = []
    for idx, row in df.iterrows():
        alias = str(row.get("Dataset Alias", "")).strip()
        current_id = str(row.get("DatasetView_id", "")).strip()
        if alias in entity_map:
            correct_id = entity_map[alias]
            if current_id != correct_id:
                changes.append(
                    {
                        "table": table_id,
                        "alias": alias,
                        "old_id": current_id,
                        "new_id": correct_id,
                    }
                )
                if not dryrun:
                    df.at[idx, "DatasetView_id"] = correct_id

    if changes and not dryrun:
        try:
            table = synapseclient.Table(table_id, df)
            syn.store(table)
        except Exception as e:
            print(f"  ERROR storing {table_id}: {e}")
            # Return changes without marking them as applied
            for c in changes:
                c["error"] = str(e)

    return changes


def main() -> None:
    args = get_args()
    syn = synapseclient.login()

    if args.dryrun:
        print("DRY RUN — no changes will be written to Synapse.\n")

    print("Building Dataset entity map from CCKP Indexed Datasets...")
    entity_map = get_dataset_entity_map(syn)
    print(f"  {len(entity_map)} Dataset entities found.\n")

    if args.tables:
        table_ids = args.tables
        print(f"Checking {len(table_ids)} specified table(s).\n")
    else:
        print("Fetching DatasetView grant manifest table IDs from UNION index...")
        table_ids = get_manifest_table_ids(syn)
        print(f"  {len(table_ids)} grant manifest tables to check.\n")

    all_changes: list[dict] = []
    for i, table_id in enumerate(table_ids, 1):
        print(f"[{i}/{len(table_ids)}] {table_id} ... ", end="", flush=True)
        changes = fix_table(syn, table_id, entity_map, args.dryrun)
        if changes:
            print(f"{len(changes)} row(s) {'would be updated' if args.dryrun else 'updated'}.")
            all_changes.extend(changes)
        else:
            print("no changes.")

    verb = "would be updated" if args.dryrun else "updated"
    print(f"\nTotal rows {verb}: {len(all_changes)}")

    if all_changes:
        suffix = "dryrun" if args.dryrun else "applied"
        report_path = Path(f"output/update_union_dataset_ids_{suffix}.csv")
        report_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(all_changes).to_csv(report_path, index=False)
        print(f"Change report saved to: {report_path}")

    if not args.dryrun and all_changes:
        print(
            "\nDone. Re-run Step 1 (merge_tables.py) on the next sync cycle to "
            "refresh the UNION materialized view with the corrected data."
        )


if __name__ == "__main__":
    main()
