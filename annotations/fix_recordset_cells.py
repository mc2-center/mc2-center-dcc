"""
fix_recordset_cells.py

Change single cells of rows in grant RecordSets from a CSV, without code.
The script builds one upload file per grant and then runs
``upload_recordsets.py`` on it, so the normal upload rules apply (write log,
contributor-owned cells, read-back check).

``--changes`` is a CSV with these columns, one cell per line:
``component``, ``grant``, ``key``, ``column``, ``new_value``.

- ``component``: ``PublicationView``, ``DatasetView``, ``ToolView`` or
  ``EducationalResource``. One run takes one component.
- ``grant``: one grant number, for example ``CA209891``. A row that lists
  several grants lives in the RecordSet of each grant. To fix such a row,
  add one line for each grant.
- ``key``: the key value of the row, for example a PMID (PublicationView) or
  a Synapse ID (DatasetView, ToolView).
- ``column``: the display name or the RecordSet key name of the column.
- ``new_value``: the full new cell value. Write a list cell as a comma
  separated list, or as a JSON array (``["A, B", "C"]``) when an item holds a
  comma. The script changes whole cells only. The upload file holds each list
  cell as a JSON array, so an unchanged item that holds a comma stays one item.
  A line is refused when a list item (new, or unchanged in a row that the
  script writes) holds a quote or a backslash: ``upload_recordsets.py`` would
  drop the whole row.

Steps: find the grant project in the staging Grants table, find its component
folder and RecordSet, read the latest version. When the grant has no RecordSet
of the component, or its RecordSet has no row with the key, the script looks in the
RecordSets of all grants. If exactly one holds the key, the script uses it
(``cells.csv`` then has ``holding_grant`` and ``found_by_search``). If several hold it,
the line is refused: add one line for each holding grant. Check every line. A line is
refused when its grant, folder or RecordSet is not found, its key is not a row,
its column is unknown, its column is the key column (use
``rekey_recordset_rows.py``), two lines set one cell to different values, or
the value fails the JSON Schema of the column. If any line is refused, the
script writes ``refused.csv`` and nothing else, and it exits with 1.

Then, for each grant, the script writes an upload file with the key column and
the changed columns only (display-name headers). A cell of those columns that
the changes file does not set keeps its current value, because a blank cell
would clear the stored value. The script writes also ``paths.csv`` (columns
``File Paths``, the folder column of the component and ``grantId``) and
``cells.csv`` (grant, recordset, key, column, current, new) and
``overwrite.csv`` (grantId, key, column). Then it calls ``upload_recordsets.py``
with ``--overwrite-csv``: exactly the cells in the changes file may replace a
value that a curator edited in Curator (a cell that a curator owns is kept by
the standard upload). Every other curator-owned cell still wins. The replaced
values are in ``<grant>/overwritten.csv`` of the upload report. Dry run is the default; ``--execute`` writes to
Synapse.

Files are in ``<report-dir>/<run-id>/fix_cells/``; the upload reports are in
``<report-dir>/<run-id>/``.

Example:
    python fix_recordset_cells.py --changes fixes.csv --report-dir reports
    python fix_recordset_cells.py --changes fixes.csv --report-dir reports --execute

author: orion.banks
"""

import argparse
import concurrent.futures
import importlib.util
import logging
import os
import sys
from pathlib import Path

import jsonschema
import pandas as pd

# removed when the mc2dcc package (CLI consolidation plan) lands
HERE = Path(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, str(HERE / ".." / "portal_tables"))
import crosswalk  # folder_id_column: as gen-mp-csv.py writes it; upload_recordsets.py reads the one folderId* column
import merge_tables
import model_contract
import recordsets
import schema_validation
import utils

logger = logging.getLogger(__name__)

COMPONENTS = ("PublicationView", "DatasetView", "ToolView", "EducationalResource")
MAX_READERS = 4  # RecordSets read at the same time in the search
RUN_PREFIX = "dcc-fixcells-"
CHANGE_COLUMNS = ["component", "grant", "key", "column", "new_value"]


def get_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.strip().split("\n")[0])
    parser.add_argument("--changes", required=True, help=f"CSV with columns {', '.join(CHANGE_COLUMNS)}")
    recordsets.add_log_args(parser)
    parser.add_argument("--report-dir", required=True, help="directory for the files and reports")
    parser.add_argument("--execute", action="store_true", help="write to Synapse; the default is a dry run")
    parser.add_argument(
        "--run-id", default=recordsets.new_run_id(RUN_PREFIX),
        help=f"default: {RUN_PREFIX!r} plus a timestamp",
    )
    return parser.parse_args(argv)


def load_changes(path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = [c for c in CHANGE_COLUMNS if c not in df.columns]
    if missing:
        raise SystemExit(f"{path} lacks column(s) {missing}")
    df = df[CHANGE_COLUMNS].copy()
    for c in ("component", "grant", "key", "column"):
        df[c] = df[c].str.strip()
    return df


def grant_projects(syn) -> dict[str, str]:
    """``{grantNumber: grant project ID}`` from the staging Grants table."""
    from synapseclient.models import query
    table = utils.portal_table("grant")
    df = query(f"SELECT grantNumber, grantId FROM {table}", synapse_client=syn)
    return dict(zip(df["grantNumber"], df["grantId"]))


def find_folder(syn, project: str, component: str):
    from synapseclient.operations import find_entity_id
    return find_entity_id(name=merge_tables.folder_name(project, component), parent=project, synapse_client=syn)


def resolve_column(name: str, columns) -> str | None:
    """The RecordSet column for a display name or a key name; None when unknown."""
    if name in columns:
        return name
    try:
        label = model_contract.normalize_headers(pd.DataFrame(columns=[name]), to="keys").columns[0]
    except ValueError:
        return None
    return label if label in columns else None


def schema_problem(component: str, column: str, value: str) -> str | None:
    """Why the schema rejects ``value`` for ``column``; None when it passes."""
    schema = schema_validation.load_schema(component)
    parsed = recordsets.parse_upload(pd.DataFrame({column: [value]}), recordsets.list_columns(component))
    record = schema_validation._instance({column: parsed[column].iloc[0]}, schema["properties"])
    cls = jsonschema.validators.validator_for(schema)
    validator = cls(schema, format_checker=schema_validation._format_checker())
    errors = [e for e in validator.iter_errors(record) if e.path and str(e.path[0]) == column]
    return errors[0].message if errors else None


def join_list(value):
    """A list cell as text for the report."""
    return ", ".join(value) if isinstance(value, list) else value


def upload_cell(column: str, value: str, list_cols) -> str:
    """``value`` as the upload file holds it: a list column as a JSON array.

    The text is read as ``parse_upload`` reads it (JSON array, or the comma split rules).
    """
    if column not in list_cols:
        return value
    return recordsets.cell_to_csv(recordsets.parse_upload(pd.DataFrame({column: [value]}), list_cols)[column].iloc[0])


def plan(syn, changes: pd.DataFrame, component: str):
    """Check the changes. Returns ``(grants, refused)``.

    ``grants`` is ``{grant: {"project", "folder", "recordset", "key", "frame", "cells"}}``
    where ``frame`` is the upload frame (key column and changed columns, key names)
    and ``cells`` the report rows. ``refused`` is a list of changes with a ``reason``.
    A line whose grant has no RecordSet, or whose RecordSet has no row with the key,
    is looked up in the RecordSets of all grants (each is read once). The grant that
    holds the key is then the grant of the cells; ``holding_grant`` and
    ``found_by_search`` are in the cell report.
    """
    refused, state = [], {}
    list_cols = recordsets.list_columns(component)
    projects = grant_projects(syn)
    loaded = {}  # grant -> dict, or a str with the reason why it has no usable RecordSet

    def refuse(row, why):
        refused.append({**row, "reason": why})

    def load(grant):
        if grant not in loaded:
            project = projects[grant]
            try:
                folder = find_folder(syn, project, component)
                rs_id = recordsets.find_recordset(syn, folder, component) if folder else None
                if rs_id is None:
                    loaded[grant] = f"no {component} folder or RecordSet in {project}"
                else:
                    _, key_cols = recordsets.recordset_info(syn, rs_id)
                    if len(key_cols) != 1:
                        loaded[grant] = f"{rs_id} upsert key is not exactly one column: {key_cols}"
                    else:
                        # the latest version can be replaced in place: read it from Synapse, never through the cache
                        current = recordsets.read_recordset(syn, rs_id, list_cols=list_cols)
                        current = current.drop_duplicates(subset=key_cols[0], keep="last").set_index(key_cols[0])
                        loaded[grant] = {"project": project, "folder": folder, "recordset": rs_id,
                                         "key": key_cols[0], "current": current}
            except Exception as e:  # one unreadable grant must not stop the run with a traceback
                loaded[grant] = f"grant {grant} ({project}) could not be read: {type(e).__name__}: {e}"
        return loaded[grant]

    def holders(key, skip):
        """The grants other than ``skip`` whose RecordSet has a row ``key``."""
        found = []
        pending = [g for g in projects if g != skip and g not in loaded]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_READERS) as pool:
            list(pool.map(load, pending))  # load never raises; each grant is submitted once
        for g in projects:
            info = load(g) if g != skip else None
            if isinstance(info, dict) and key in info["current"].index:
                found.append(g)
        return found

    for r in changes.to_dict("records"):
        grant, found_by_search = r["grant"], False
        if grant not in projects:
            refuse(r, "grant is not in the staging Grants table")
            continue
        info = load(grant)
        if isinstance(info, dict) and r["key"] in info["current"].index:
            target = grant
        else:
            held = holders(r["key"], skip=grant)
            if len(held) == 1:
                target, found_by_search = held[0], True
            elif held:
                refuse(r, f"key is a row of the RecordSets of several grants: {', '.join(held)}; "
                          "add one line for each grant")
                continue
            elif isinstance(info, str):
                refuse(r, f"{info}, and the key is not a row of any {component} RecordSet")
                continue
            else:
                refuse(r, f"key is not a row of the RecordSet, or of any other {component} RecordSet")
                continue
        info = load(target)
        current, key_col = info["current"], info["key"]
        column = resolve_column(r["column"], set(current.columns) | {key_col})
        st = state.setdefault(target, {"edits": {}, "cells": [], "lines": []})
        edits = st["edits"]
        value = upload_cell(column, r["new_value"], list_cols) if column else r["new_value"]
        if column is None:
            refuse(r, "unknown column")
        elif column == key_col:
            refuse(r, "key column; use rekey_recordset_rows.py")
        elif (r["key"], column) in edits and edits[(r["key"], column)] != value:
            refuse(r, "the cell is set twice to different values")
        elif (why := schema_problem(component, column, r["new_value"])):
            refuse(r, f"value fails the schema: {why}")
        elif recordsets.has_unsafe_item(recordsets.parse_cell(value)) and column in list_cols:
            refuse(r, "a list item holds a quote or a backslash; upload_recordsets.py would drop the row")
        else:
            st["lines"].append(r)
            if (r["key"], column) not in edits:
                cell = {"grant": grant, "recordset": info["recordset"], "key": r["key"], "column": column,
                        "current": join_list(current.at[r["key"], column]), "new": r["new_value"],
                        "holding_grant": target, "found_by_search": found_by_search}
                st["cells"].append(cell)
                if found_by_search:
                    print(f"found by search: key {r['key']} of grant {grant} is in the {component} "
                          f"RecordSet {info['recordset']} of grant {target}")
            edits[(r["key"], column)] = value

    grants = {}
    for target, st in state.items():
        edits = st["edits"]
        if not edits:
            continue
        info = load(target)
        current = info["current"]
        columns = list(dict.fromkeys(c for _, c in edits))
        keys = list(dict.fromkeys(k for k, _ in edits))
        frame = current.loc[keys, columns].copy().astype(object)  # a blank upload cell would clear the stored value
        for c in columns:
            frame[c] = frame[c].map(recordsets.cell_to_csv)  # a list cell as a JSON array keeps a comma item whole
        for (k, c), v in edits.items():
            frame.at[k, c] = v
        unsafe_keys = {k for k in keys for c in columns if (k, c) not in edits
                       and recordsets.has_unsafe_item(current.at[k, c])}
        if unsafe_keys:
            for line in st["lines"]:
                if line["key"] in unsafe_keys:
                    refuse(line, "a list item of an unchanged cell of this row holds a quote or a backslash; "
                                 "upload_recordsets.py would drop the row")
                else:  # listed, not left out silently: the grant's upload file is not written
                    refuse(line, "another row of this grant was refused (unchanged cell with a quote or a backslash)")
            continue
        grants[target] = {"project": info["project"], "folder": info["folder"], "recordset": info["recordset"],
                          "key": info["key"], "frame": frame.reset_index(), "cells": st["cells"]}
    return grants, refused


def run_upload(argv, syn):
    """Run ``upload_recordsets.py`` in-process, as ``upload_workflow.py`` does."""
    spec = importlib.util.spec_from_file_location("upload_recordsets_cmd", HERE / "upload_recordsets.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.main(argv, syn=syn)


def main(argv=None, syn=None):
    args = get_args(argv)
    changes = load_changes(args.changes)
    bad = sorted(set(changes["component"]) - set(COMPONENTS))
    if bad or changes["component"].nunique() != 1:
        raise SystemExit(f"one run needs one component of {list(COMPONENTS)}; found {sorted(set(changes['component']))}")
    component = changes["component"].iloc[0]
    syn = syn or utils.syn_login()
    out = Path(args.report_dir) / args.run_id / "fix_cells"
    out.mkdir(parents=True, exist_ok=True)

    grants, refused = plan(syn, changes, component)
    if refused:
        pd.DataFrame(refused).to_csv(out / "refused.csv", index=False)
        print(f"{len(refused)} change(s) refused, nothing written (see {out / 'refused.csv'}):")
        print(pd.DataFrame(refused)[["grant", "key", "column", "reason"]].to_string(index=False))
        return 1

    paths, cells = [], []
    for grant, g in grants.items():
        path = out / "files" / f"{grant}.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        model_contract.normalize_headers(g["frame"], to="display").to_csv(path, index=False)
        paths.append({"File Paths": str(path), crosswalk.COMPONENT_FOLDERS[component]["folder_id_column"]: g["folder"], "grantId": g["project"]})
        cells += g["cells"]
    pd.DataFrame(paths).to_csv(out / "paths.csv", index=False)
    cells_df = pd.DataFrame(cells)
    if "found_by_search" in cells_df and not cells_df["found_by_search"].any():  # the report is as before when no line needed a search
        cells_df = cells_df.drop(columns=["holding_grant", "found_by_search"])
    cells_df.to_csv(out / "cells.csv", index=False)
    # Every line of the changes file is a deliberate overwrite, also of a curator-owned cell.
    pd.DataFrame([{"grantId": g["project"], "key": c["key"], "column": c["column"]}
                  for g in grants.values() for c in g["cells"]]).to_csv(out / "overwrite.csv", index=False)
    print(cells_df.to_string(index=False))
    print(f"{len(cells_df)} cell(s) in {len(grants)} grant(s); files in {out}")

    upload_argv = ["--paths-csv", str(out / "paths.csv"), "--component", component,
                   "--write-log-table", args.write_log_table, "--removals-table", args.removals_table,
                   "--overwrite-csv", str(out / "overwrite.csv"),
                   "--report-dir", args.report_dir, "--run-id", args.run_id]
    if args.execute:
        upload_argv.append("--execute")
    return run_upload(upload_argv, syn) or 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    sys.exit(main())
