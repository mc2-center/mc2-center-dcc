"""
Create/store UNION tables from grant-specific metadata tables in 
Synapse.

Inputs:
- synID for source tableview, queried for table synIDs to combine into UNION table
- synID for the project in which to store the UNION table
- manifest type contained in source tables

Outputs:
- a MaterializedView table, containing all entries from
the tables provided as input
- if table already exists, scope will be updated and table will be regenerated in-place

author: orion.banks
"""

from synapseclient import operations
from synapseclient.models import MaterializedView, RecordSet, SchemaStorageStrategy, Table
import argparse
from datetime import datetime
import logging
import os
import pandas as pd
import model_contract
import recordsets
import utils

GRANTS_TABLE = "syn21918972"
FOLDER_MAP = {
    "PublicationView": "publications",
    "DatasetView": "datasets",
    "ToolView": "tools",
    "EducationalResource": "education",
}

logger = logging.getLogger(__name__)


def get_args():

    parser = argparse.ArgumentParser(
        description="Create UNION tables from metadata stored in Synapse project tables"
    )
    parser.add_argument(
        "-s", type=str, help="Synapse ID of entityview with table information to query."
    )
    parser.add_argument(
        "-t", type=str, help="Synapse ID of target project to store merged table."
    )
    parser.add_argument(
        "-n",
        type=str,
        choices=["PublicationView", "DatasetView", "ToolView", "EducationalResource"],
        help="Name of metadata component being merged into table.",
    )
    parser.add_argument(
        "-rec",
        "--target_record_set",
        action="store_true",
        default=None,
        help="Boolean. If provided, the program will search for Synapse RecordSets of the requested data type in MC2 Center projects"
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="RecordSet mode: directory for <Component>_UNION.csv (default: output/).",
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help="RecordSet mode: LIVE WRITE. Store the union as a Synapse table in -t.",
    )
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help="RecordSet mode: let --publish replace the table with an empty union.",
    )
    parser.add_argument(
        "--grant-projects",
        nargs="+",
        default=None,
        help=argparse.SUPPRESS,  # override the Grants table with project IDs
    )
    return parser.parse_args()


def get_table_ids(syn, source_id, table_type, column_name):

    table_id_df = Table(id=source_id).query(
        query=f"SELECT {column_name} FROM {source_id} WHERE name='{table_type}'",
        synapse_client=syn,
    )

    return table_id_df[column_name].to_list()


def build_query(table_ids):
    
    return " UNION ".join([f"SELECT * FROM {id}" for id in table_ids])


def get_record_sets(syn, record_type, folder_name, source = "syn21918972", org = "MC2Center"):

    project_ids = Table(id=source).query(query=f"SELECT grantId FROM {source}", synapse_client=syn)

    record_df = pd.DataFrame(project_ids)
    record_df["recordId"] = ""
    record_name = "_".join([org, record_type])

    # TODO(D12): mirror tables below still feed the UNION; see plan before removing.
    for _,row in record_df.iterrows():
        folder_id = operations.find_entity_id(name=folder_name, parent=row["grantId"], synapse_client=syn)
        if folder_id is None:
            logger.warning("Skipping %s: folder '%s' not found", row["grantId"], folder_name)
            continue
        record_id = operations.find_entity_id(name=record_name, parent=folder_id, synapse_client=syn)
        if record_id is None:
            logger.warning("Skipping %s: RecordSet '%s' not found in folder %s", row["grantId"], record_name, folder_id)
            continue
        record_set = RecordSet(id=record_id).get(synapse_client=syn)
        record_set_df = pd.read_csv(record_set.path, header=0)  # apply sorting, column naming, extraction, etc. as needed
        record_set_table = Table(name=f"{record_name}_table", parent_id=row["grantId"]).store(synapse_client=syn)
        record_set_table.store_rows(values=record_set_df, schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA, synapse_client=syn)
        record_df.at[_,"recordId"] = record_set_table.id

    # grants that were skipped have no recordId; keep them out of the UNION query
    return record_df[record_df["recordId"] != ""].reset_index(drop=True)


def list_grant_projects(syn, source=GRANTS_TABLE):
    df = Table(id=source).query(query=f"SELECT grantId FROM {source}", synapse_client=syn)
    return [g for g in df["grantId"].tolist() if g]


def _join_cell(value):
    return ", ".join(str(v) for v in value) if isinstance(value, list) else value


def _join_lists(df, key_col):
    """", "-join list cells, warning (column and key) when an item has a comma."""
    keys = df[key_col] if key_col in df.columns else None
    for col in df.columns:
        for i, value in enumerate(df[col]):
            if isinstance(value, list) and any("," in str(v) for v in value):
                key = keys.iloc[i] if keys is not None else f"row {i}"
                logger.warning(
                    "List item containing a comma in column '%s' (key %s) will split "
                    "when the ', '-joined cell is parsed downstream", col, key,
                )
        df[col] = df[col].map(_join_cell)
    return df


def build_union(syn, component, grant_ids):
    """Read each grant's RecordSet and union them. Returns (frame, stats).

    ``frame`` has display-name headers in template order; list cells are
    ", "-joined. ``stats`` holds grants scanned/found/missing, rows and duplicate keys.
    """
    folder_name = FOLDER_MAP[component]
    frames, missing = [], []
    for grant in grant_ids:
        folder_id = operations.find_entity_id(name=folder_name, parent=grant, synapse_client=syn)
        if folder_id is None:
            logger.warning("Skipping %s: folder '%s' not found", grant, folder_name)
            missing.append(grant)
            continue
        rs_id = recordsets.find_recordset(syn, folder_id, component)
        if rs_id is None:
            logger.warning("Skipping %s: RecordSet for %s not found in %s", grant, component, folder_id)
            missing.append(grant)
            continue
        frames.append(recordsets.read_recordset(syn, rs_id, list_cols=recordsets.list_columns(component)))

    headers = model_contract.template_headers(component)
    if frames:
        union = pd.concat(frames, ignore_index=True, sort=False).fillna("")
    else:
        union = pd.DataFrame()
    union = model_contract.normalize_headers(union, to="display")
    union = _join_lists(union, f"{component}_id")
    for col in headers:
        if col not in union.columns:
            union[col] = ""
    # A Grid import drops Component (not a schema property); restore it.
    if "Component" in headers:
        union["Component"] = component
    extra = [c for c in union.columns if c not in headers]
    if extra:
        logger.warning("Columns not in the %s template, kept at the end: %s", component, extra)
    union = union[headers + extra].reset_index(drop=True)

    key = f"{component}_id"
    dupes = 0
    if len(union) and key in union.columns:
        keyed = union[union[key] != ""][key]
        dupes = int(keyed.duplicated(keep="first").sum())
    stats = {
        "grants_scanned": len(grant_ids),
        "recordsets_found": len(frames),
        "missing": len(missing),
        "rows": len(union),
        "duplicate_keys": dupes,
    }
    return union, stats


def union_schema(component):
    """Fixed Synapse schema from the template headers (free-text columns get LARGETEXT)."""
    from synapseclient.models import Column, ColumnType

    to_display = {label: display for display, label in model_contract.label_map().items()}
    # joined list cells can exceed STRING(1000), so they are LARGETEXT too
    free = set(model_contract.free_text_columns(component)) | {
        to_display.get(c, c) for c in recordsets.list_columns(component)
    }
    return [
        Column(name=h, column_type=ColumnType.LARGETEXT)
        if h in free
        else Column(name=h, column_type=ColumnType.STRING, maximum_size=1000)
        for h in model_contract.template_headers(component)
    ]


def _column_shape(c):
    """(name, type, maximum_size) of a column; maximum_size only applies to STRING."""
    ctype = str(getattr(c.column_type, "value", c.column_type))
    return (c.name, ctype, getattr(c, "maximum_size", None) if ctype == "STRING" else None)


def publish_union(syn, component, union, target):
    """LIVE WRITE: store ``union`` as table ``<component>_UNION`` in ``target``.

    An existing table (its columns, including STRING maximum_size, must match the
    fixed schema) is replaced: snapshot it, delete its rows, store the union, and
    put the snapshot's rows back if ``store_rows`` fails, so a failed re-publish
    does not leave the table empty. (Mirrors utils.update_table's
    snapshot/delete/store/restore; that function is not reused because it
    requires a non-empty frame, rewrites newlines in cells, and checks the
    portal crosswalk, none of which apply to a union table.)
    """
    name = f"{component}_UNION"
    schema = union_schema(component)
    values = union[model_contract.template_headers(component)]  # extra (non-template) columns are not in the schema
    existing_id = operations.find_entity_id(name=name, parent=target, synapse_client=syn)
    if existing_id is None:
        table = Table(name=name, parent_id=target, columns=schema).store(synapse_client=syn)
        table.store_rows(values=values, synapse_client=syn)
        return table.id

    existing = Table(id=existing_id).get(include_columns=True, synapse_client=syn)
    have = [_column_shape(c) for c in existing.columns.values()]
    want = [_column_shape(c) for c in schema]
    if have != want:
        raise ValueError(
            f"Existing table {existing_id} ({name}) has columns that differ from the "
            f"fixed {component} schema; refusing to publish.\n  existing: {have}\n  expected: {want}"
        )
    existing.snapshot(label=f"pre-union-{datetime.now():%Y-%m-%d-%H-%M-%S}", synapse_client=syn)
    previous = existing.query(
        query=f"SELECT * FROM {existing_id}", include_row_id_and_row_version=False, synapse_client=syn
    )
    existing.delete_rows(query=f"SELECT ROW_ID, ROW_VERSION FROM {existing_id}", synapse_client=syn)
    try:
        existing.store_rows(values=values, synapse_client=syn)
    except Exception:
        logger.error("Storing the union in %s failed; restoring the previous %d rows", existing_id, len(previous))
        existing.store_rows(values=previous, synapse_client=syn)
        raise
    return existing.id


def run_recordset_mode(syn, component, output_dir, grant_ids=None, target=None, publish=False,
                       allow_empty=False):
    if grant_ids is None:
        grant_ids = list_grant_projects(syn)
    union, stats = build_union(syn, component, grant_ids)
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"{component}_UNION.csv")
    union.to_csv(path, index=False)
    print(
        f"{component} union: grants scanned={stats['grants_scanned']}, "
        f"RecordSets found={stats['recordsets_found']}, missing={stats['missing']}, "
        f"rows={stats['rows']}, duplicate keys={stats['duplicate_keys']}\nWrote {path}"
    )
    if publish:
        # No RecordSets found (or a bad grant list) gives an empty union; never let
        # that silently empty the published table.
        if union.empty and not allow_empty:
            raise SystemExit(
                f"{component} union is empty ({stats['recordsets_found']} RecordSets found); "
                "not publishing. Use --allow-empty to replace the table anyway."
            )
        print(f"Published table {publish_union(syn, component, union, target)}")
    return union, stats


def main():

    args = get_args()
    if args.publish and not (args.target_record_set and args.t):
        raise SystemExit("--publish requires --target_record_set and -t")
    if args.target_record_set and not args.n:
        raise SystemExit("--target_record_set requires -n")
    syn = utils.syn_login()
    if args.target_record_set:
        run_recordset_mode(syn, args.n, args.output_dir, args.grant_projects, args.t, args.publish,
                           args.allow_empty)
        return
    source, target, table_type, record_sets = args.s, args.t, args.n, args.target_record_set

    folder_map = {
        "PublicationView" : "publications",
        "DatasetView" : "datasets",
        "ToolView" : "tools",
        "EducationalResource" : "education"
    }

    if table_type in [
        "PublicationView",
        "DatasetView",
        "ToolView",
        "EducationalResource"
    ]:

        label = f"{table_type}_UNION"
        view_type = f"{table_type.lower()}_synapse_storage_manifest_table"
        record_type =  f"{table_type}_RecordSet"
        folder_name = folder_map[table_type]

    else:
        print(
            f"{table_type} is not a valid table type. Please select a different component."
        )
    
    if record_sets is not None:
        record_df = get_record_sets(syn, record_type, folder_name)
        table_ids_from_view = record_df["recordId"].to_list()
    else:
        table_ids_from_view = get_table_ids(syn, source, view_type, "id")
    
    print(table_ids_from_view)

    table_query = build_query(table_ids_from_view)

    table = MaterializedView(name=label, parent_id=target, defining_sql=table_query)

    table.store(synapse_client=syn)


if __name__ == "__main__":
    main()
