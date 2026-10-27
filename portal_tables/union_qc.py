"""
union_qc.py

I6: builds, validates and (on request) uploads the merged manifests the sync
scripts read (publications syn53478776, datasets syn53478774, tools syn53479671,
education syn53651540), in 14.x form.

Regenerate mode (``--regenerate``, a dry run unless ``--upload``), per component:
  1. builds the per-type union: from the grants' RecordSets (``merge_tables.
     build_union``) or, for testing, from a local directory of per-grant
     ``converted.csv`` files (``--input-dir``, e.g. the I5a migration dry run);
  2. merges rows that share the alias key (``qc_model/qc_attribute_mapping.csv``:
     ``first`` keeps the first row's value, the join rows collect the distinct
     items from every row, comma-joined; this is the "merged" in merged manifest);
  3. applies the curated PublicationView Key corrections (D-Q35,
     ``output/worklists/i8_1_pmid_keys.csv``), then fills empty cells with
     "Pending Annotation"/"Not Applicable" and trims whitespace
     (``manifest_cleanup``, shared with ``utils/merge_and_correct_manifests.py``;
     ``--no-fill`` skips the fill);
  4. carries reference-only columns (``iconTags``) over from the current
     manifest by alias key;
  5. validates with jsonschema against the 14.x JSON Schemas
     (``--schema-version``). Invalid rows are kept and reported (D-Q39:
     migrate and flag, never drop);
  6. writes ``<Component>_merged_manifest.csv`` (14.x display headers in template
     order), ``validation.csv``, ``validation_counts.csv``, ``key_corrections.csv``
     and ``report.txt`` to ``<output-dir>/<Component>/``.
``--upload`` stores the manifest as a new version of the existing manifest file
(LIVE WRITE, refused with ``--input-dir``).

Legacy mode (no ``--regenerate``): downloads UNION tables, merges, compares them
with the current manifests (``_updated.csv``) and validates, as before, but with
jsonschema instead of ``schematic``. Nothing is trimmed: invalid rows are listed
in ``<Component>_validation.csv``.

author: orion.banks
"""

from synapseclient import Synapse, operations
from synapseclient.models import File, Table
import argparse
import logging
import os
import pandas as pd
from pathlib import Path
import re
import manifest_cleanup
import merge_tables
import model_contract
import recordsets
import schema_validation
import utils

logger = logging.getLogger(__name__)

COMPONENTS = list(merge_tables.FOLDER_MAP)
RESOURCE = {
    "PublicationView": "publication",
    "DatasetView": "dataset",
    "ToolView": "tool",
    "EducationalResource": "education",
}
DEFAULT_PMID_KEYS = "output/worklists/i8_1_pmid_keys.csv"
DEFAULT_CARRY = ("iconTags",)
# The live publication and dataset manifests hold one row per alias (grants merged);
# the live tool manifest repeats a tool name once per grant, so tools stay unmerged.
DEFAULT_MERGE = ("PublicationView", "DatasetView")
# Manifest columns that are not schema properties (not validated).
NON_SCHEMA = ("Component", "entityId", "iconTags")
ID_CONFLICT_COLUMNS = ["alias", "ids", "grants", "rows"]
CORRECTION_COLUMNS = ["key", "column", "current_value", "proposed_value", "status"]


def get_args(argv=None):

    parser = argparse.ArgumentParser(
        description="Regenerate and validate the merged manifests the sync scripts read"
    )
    parser.add_argument(
        "--regenerate",
        action="store_true",
        help="Regenerate the merged manifest(s) in 14.x form. Dry run unless --upload.",
    )
    parser.add_argument(
        "--components",
        nargs="+",
        choices=COMPONENTS,
        default=COMPONENTS,
        help="Regenerate mode: components to process (default: all four).",
    )
    parser.add_argument(
        "--input-dir",
        default=None,
        help="Regenerate mode: read per-grant converted.csv files under this directory "
        "(<dir>/<Component>/**/converted.csv, else <dir>/**/converted.csv) "
        "instead of the grants' RecordSets.",
    )
    parser.add_argument("--grant-projects", nargs="+", default=None, help="Regenerate mode: grant project IDs (default: the Grants table).")
    parser.add_argument("--output-dir", default="output/i6", help="Regenerate mode: where reports and manifests go.")
    parser.add_argument(
        "--schema-version",
        default=None,
        help="data-models tag of the JSON Schemas to validate against (default: model_contract.MODEL_REF).",
    )
    parser.add_argument(
        "--mapping",
        default=None,
        help="Merge mapping CSV (default: qc_model/qc_attribute_mapping.csv from data-models). Legacy mode: path to mapping CSV.",
    )
    parser.add_argument(
        "--pmid-keys",
        default=None,
        help=f"Regenerate mode: curated PublicationView Key corrections (default {DEFAULT_PMID_KEYS} if it exists).",
    )
    parser.add_argument(
        "--reference-csv",
        default=None,
        help="Regenerate mode: current merged manifest to carry iconTags from (a CSV; default: read the live manifest).",
    )
    parser.add_argument(
        "--carry-columns",
        nargs="*",
        default=list(DEFAULT_CARRY),
        help="Regenerate mode: reference-only columns carried over by alias key.",
    )
    parser.add_argument(
        "--merge-components",
        nargs="*",
        choices=COMPONENTS,
        default=list(DEFAULT_MERGE),
        help="Regenerate mode: components whose rows are merged by alias key (default: publications and datasets).",
    )
    parser.add_argument(
        "--no-fill",
        action="store_true",
        help="Regenerate mode: do not fill empty Assay/Tumor Type/Tissue/... cells with "
        "'Pending Annotation' or 'Not Applicable' (the merge_and_correct_manifests step the portal text depends on).",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Regenerate mode: LIVE WRITE. Store each manifest as a new version of the current manifest file. "
        "Refused while any merged alias holds different ids in different grants (id_conflicts.csv).",
    )
    # legacy mode
    parser.add_argument("-l", nargs="+", help="Legacy mode: Synapse table IDs to query.")
    parser.add_argument("-p", help="Legacy mode: path to mapping CSV")
    parser.add_argument(
        "-tp", required=False, default=None, help="Legacy mode: path to a manifest CSV to validate (and merge with -m)."
    )
    parser.add_argument(
        "-m",
        action="store_true",
        default=None,
        help="""Boolean; if flag is provided,
        manifest rows will be merged by model-specific key.""",
    )
    parser.add_argument(
        "-s",
        action="store_true",
        help="""Boolean; if flag is provided,
        an extended column set will be used to
        identify updated entries.""",
    )
    parser.add_argument(
        "-db",
        action="store_true",
        default=None,
        help="""Boolean; if flag is provided,
        column labels will be added to updated output to indicate table source.""",
    )
    return parser.parse_args(argv)


def get_tables(syn: Synapse, tableIdList: list[str], mergeFlag: bool) -> list[tuple[pd.DataFrame | str, str]]:

    tables = []  # set up lists to store info
    names = []

    for tableId in tableIdList:  # pull table from Synapse
        table = Table(id=tableId).query(
            query=f"SELECT * FROM {tableId}",
            include_row_id_and_row_version=False,
            synapse_client=syn,
        )
        table = utils.fill_blank(table)
        name = table["Component"].iat[1]  # grab name of data type from table (second row, as before)
        manifestPath = Path(f"output/{name}/{name}.csv")  # build path to store table as CSV
        manifestPath.parent.mkdir(parents=True, exist_ok=True)  # create folder to store CSVs
        table.to_csv(manifestPath, index=False, lineterminator="\n")  # convert df to CSV
        
        if mergeFlag:  # if merging store the table for the next function
            tables.append(table)
        else:  # if not merging, store the file path for the next function
            tables.append(manifestPath)
        names.append(name)  # store the name for next functions

    return list(zip(tables, names))


def combine_rows(args: list[tuple[pd.DataFrame | str, str]], mapping: pd.DataFrame) -> list[tuple[Path, str]]:

    newTables, newNames = zip(*args)  # unpack the input

    groups = []
    names = []
    
    for table, name in zip(newTables, newNames):
        table = table.astype(str)  # make everything strings so they can be joined as needed
        mappingDict = {}
        
        for _, row in mapping.iterrows():
            if row["component"] == name:
                mappingDict[row["attribute"]] = row["mapping"]
                if row["tag"] == "aliasColumn":
                    aliasColumn = row["attribute"]
        
        for k, v in mappingDict.items():
            if v == '",".join':
                mappingDict[k] = ",".join
        
        mergedTable = (  # group rows by designated identifier and map attributes
            table.groupby(aliasColumn, as_index=False).agg(mappingDict).reset_index()
        )
        mergedTable = mergedTable.iloc[:, 1:]  # remove unnecessary "id" column
        
        mergePath = Path(f"output/{name}/{name}_merged.csv")
        mergePath.parent.mkdir(parents=True, exist_ok=True)
        mergedTable.to_csv(mergePath, index=False)

        groups.append(mergePath)
        names.append(name)

    return list(zip(groups, names))


def get_ref_tables(syn: Synapse, args: list[tuple[Path, str]]) -> list[tuple[Path, Path, str]]:

    tables, names = zip(*args)

    ref_paths = []
    table_paths = []
    ref_names = []

    for table, name in zip(tables, names):
        shortName = re.split(r'(?=[A-Z])', name)[1].lower()
        if shortName == "educational":
            shortName = "education"

        ref = utils.get_manifest(shortName)
        ref_table = operations.get(
            ref,
            file_options=operations.FileOptions(download_location=f"output/{name}"),
            synapse_client=syn,
        )
        ref_paths.append(ref_table.path)
        table_paths.append(table)
        ref_names.append(name)

    return list(zip(ref_paths, table_paths, ref_names))


def compare_and_subset_tables(args: list[tuple[Path, Path, str]], mapping: pd.DataFrame, strict: bool, debug: bool) -> list[tuple[Path, str]]:

    current, updated, names = zip(*args)

    updatePaths = []
    updateNames = []

    filter = "strict" if strict else "all"

    for ref, new, name in zip(current, updated, names):
        cols = []
        for _, row in mapping.iterrows():
            if row["component"] == name:
                if row["filter"] in set([filter, "all"]):
                    cols.append(row["attribute"])
                if row["tag"] == "aliasColumn":
                    key = row["attribute"]

        current_table = pd.read_csv(ref, header=0).sort_values(by=key).fillna("")
        new_table = pd.read_csv(new, header=0).sort_values(by=key).fillna("")
        
        if debug:
            current_table["Source"] = "Database"
            new_table["Source"] = "Updated"

        tables = [current_table, new_table]

        updated = pd.concat(tables, ignore_index=True).reset_index(drop=True).astype(str)

        updated.drop_duplicates(subset=cols, keep=False, ignore_index=True, inplace=True)

        if debug:
            updated.sort_values(by=[key, "Source"], inplace=True)

        updatePath = Path(f"output/{name}/{name}_updated.csv")
        updatePath.parent.mkdir(parents=True, exist_ok=True)

        updated.to_csv(updatePath, index=False)

        updatePaths.append(updatePath)
        updateNames.append(name)

    return list(zip(updatePaths, updateNames))


# --- validation (jsonschema, no schematic) -------------------------------------


def validate_manifest(manifest: pd.DataFrame, component: str, ref: str | None = None) -> pd.DataFrame:
    """jsonschema failures of a manifest (display headers): ``key, field, validator, message``.

    Validates what a sync would read: list columns are split from their joined cells
    (``recordsets.parse_upload``), non-schema columns (Component, entityId, iconTags)
    are left out. Rows are never removed or changed.
    """
    frame = manifest.drop(columns=[c for c in NON_SCHEMA if c in manifest.columns])
    frame = model_contract.rename_legacy_headers(frame, component)
    frame = model_contract.normalize_headers(frame, to="keys")
    frame = recordsets.parse_upload(frame, recordsets.list_columns(component))
    return schema_validation.validate_frame(frame, component, ref, key=f"{component}Id")


def write_validation(validation: pd.DataFrame, out_dir: Path, prefix: str = "") -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    validation.to_csv(out_dir / f"{prefix}validation.csv", index=False)
    schema_validation.validation_counts(validation).to_csv(out_dir / f"{prefix}validation_counts.csv", index=False)


def validate_tables(args: list[tuple[Path, str]], ref: str | None = None) -> list[tuple[str, pd.DataFrame]]:
    """Legacy mode: validate each manifest CSV, write ``<Component>_validation.csv``. Nothing is trimmed."""

    results = []
    for path, name in args:
        print(f"\nValidating manifest at: {str(path)}...")
        manifest = pd.read_csv(path, header=0, dtype=str, keep_default_na=False)
        validation = validate_manifest(manifest, name, ref)
        outPath = Path(f"output/{name}/{name}_validation.csv")
        outPath.parent.mkdir(parents=True, exist_ok=True)
        validation.to_csv(outPath, index=False)
        print(f"{len(validation)} validation failures in {validation['key'].nunique()} rows; written to {outPath}")
        results.append((name, validation))

    return results


# --- regeneration (I6) ----------------------------------------------------------


def load_mapping(path=None, ref: str | None = None) -> pd.DataFrame:
    """``qc_attribute_mapping.csv`` with 13.1.0 attribute names renamed to 14.x.

    The data-models file still says ``Data Use Codes`` (DM-14); the rename makes it
    ``Dataset Data Use Codes`` so the column is merged either way.
    """
    path = path or model_contract.fetch("qc_model/qc_attribute_mapping.csv", ref=ref)
    mapping = pd.read_csv(path, header=0, dtype=str, keep_default_na=False)
    mapping["attribute"] = [
        model_contract.RENAMES_13_TO_14.get(c, {}).get(a, a)
        for c, a in zip(mapping["component"], mapping["attribute"])
    ]
    return mapping


def alias_column(mapping: pd.DataFrame, component: str) -> str | None:
    rows = mapping[(mapping["component"] == component) & (mapping["tag"] == "aliasColumn")]
    return rows["attribute"].iat[0] if len(rows) else None


def merge_by_alias(union: pd.DataFrame, component: str, mapping: pd.DataFrame):
    """Merge rows that share the alias key, as ``combine_rows`` did for the live manifests.

    The first row's value is kept ("first"); the join columns collect the distinct
    items of every row, comma-joined, in first-seen order. Rows are ordered by alias;
    rows with a blank alias are never merged and follow at the end. The id column
    (``idColumn``) must agree: a group whose rows hold more than one distinct id is
    still comma-joined in the output but is listed in ``stats["id_conflicts"]``
    (``alias, ids, grants, rows``), and the caller must not publish it. Returns
    ``(merged, stats)``.
    """
    alias = alias_column(mapping, component)
    rows = mapping[mapping["component"] == component]
    id_rows = rows[rows["tag"] == "idColumn"]["attribute"]
    id_col = id_rows.iat[0] if len(id_rows) else None
    grant_rows = rows[rows["tag"] == "grantColumn"]["attribute"]
    grant_col = grant_rows.iat[0] if len(grant_rows) else None
    join_cols = [a for a, m in zip(rows["attribute"], rows["mapping"]) if m == '",".join' and a in union.columns]
    stats = {"rows_in": len(union), "alias": alias, "merged_groups": 0, "blank_alias": 0,
             "id_conflicts": []}
    if alias is None or alias not in union.columns or union.empty:
        stats["rows_out"] = len(union)
        return union, stats
    blank = union[alias] == ""
    merged = []
    for _, group in union[~blank].groupby(alias, sort=True):
        row = group.iloc[0].to_dict()
        for col in join_cols:
            row[col] = ",".join(dict.fromkeys(i for v in group[col] for i in utils.split_cell(v)))
        stats["merged_groups"] += int(len(group) > 1)
        if id_col in union.columns and len(group) > 1:
            ids = list(dict.fromkeys(i for v in group[id_col] for i in utils.split_cell(v)))
            if len(ids) > 1:
                grants = list(dict.fromkeys(i for v in group[grant_col] for i in utils.split_cell(v))) if grant_col in union.columns else []
                stats["id_conflicts"].append({"alias": row[alias], "ids": ",".join(ids), "grants": ",".join(grants),
                                              "rows": len(group)})
        merged.append(row)
    stats["blank_alias"] = int(blank.sum())
    out = pd.concat([pd.DataFrame(merged, columns=union.columns), union[blank]], ignore_index=True)
    stats["rows_out"] = len(out)
    return out, stats


def apply_key_corrections(manifest: pd.DataFrame, component: str, path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply the curated key fixes (``i8_1_pmid_keys.csv``, D-Q35) to the manifest.

    A worklist row names the manifest row (``row_key``, possibly a repeated id such as
    ``syn1,syn1``), a column (class label), the mangled ``current_value`` and the
    ``proposed_value``. The cell is changed only when it still holds ``current_value``.
    Returns ``(manifest, report)``; status is applied, already_correct, cell_differs,
    not_found or no_column.
    """
    report = []
    if path is None or not os.path.exists(path):
        return manifest, pd.DataFrame(columns=CORRECTION_COLUMNS)
    fixes = pd.read_csv(path, dtype=str, keep_default_na=False)
    fixes = fixes[fixes["source_id"] == utils.CONFIG[RESOURCE[component]]["manifest"]]
    to_display = {label: display for display, label in model_contract.label_map().items()}
    key_col = f"{component}_id"
    manifest = manifest.copy()
    for fix in fixes.to_dict("records"):
        column = to_display.get(fix["column"], fix["column"])
        wanted = set(utils.split_cell(fix["row_key"]))
        record = {"key": fix["row_key"], "column": column, "current_value": fix["current_value"],
                  "proposed_value": fix["proposed_value"]}
        if column not in manifest.columns or key_col not in manifest.columns:
            report.append({**record, "status": "no_column"})
            continue
        hits = [i for i, v in manifest[key_col].items() if set(utils.split_cell(v)) == wanted]
        if not hits:
            report.append({**record, "status": "not_found"})
        for i in hits:
            cell = manifest.at[i, column]
            if cell == fix["current_value"]:
                manifest.at[i, column] = fix["proposed_value"]
                report.append({**record, "status": "applied"})
            elif cell == fix["proposed_value"]:
                report.append({**record, "status": "already_correct"})
            else:
                report.append({**record, "current_value": cell, "status": "cell_differs"})
    return manifest, pd.DataFrame(report, columns=CORRECTION_COLUMNS)


def carry_columns(manifest: pd.DataFrame, reference: pd.DataFrame, alias: str, columns) -> tuple[pd.DataFrame, dict]:
    """Add reference-only columns (``iconTags``) from the current manifest, matched on the alias key.

    The reference may have space-stripped headers (the live dataset manifest), so
    headers compare without spaces. The first non-empty value per alias is used;
    manifest rows without one get "". Returns ``(manifest, {column: rows_without_value})``.
    """
    manifest = manifest.copy()
    ref = reference.rename(columns=lambda c: c.replace(" ", ""))
    stats = {}
    key = alias.replace(" ", "")
    for col in columns:
        if key not in ref.columns or col not in ref.columns:
            logger.warning("Reference manifest has no %r or %r column; %s not carried", key, col, col)
            continue
        lookup = {}
        for k, v in zip(ref[key].fillna(""), ref[col].fillna("")):
            if v and k not in lookup:
                lookup[k] = v
        manifest[col] = manifest[alias].map(lookup).fillna("")
        stats[col] = int((manifest[col] == "").sum())
    return manifest, stats


def regenerate_component(component, union, mapping, out_root, ref=None, pmid_keys=None, reference=None,
                         carry=DEFAULT_CARRY, union_stats=None, merge=True, fill=True):
    """Merge, correct, carry, validate and write one component's manifest. Returns a result dict."""
    out_dir = Path(out_root) / component
    out_dir.mkdir(parents=True, exist_ok=True)
    if merge:
        manifest, merge_stats = merge_by_alias(union, component, mapping)
    else:
        manifest, merge_stats = union, {"rows_in": len(union), "rows_out": len(union), "alias": alias_column(mapping, component),
                                        "merged_groups": 0, "blank_alias": 0, "id_conflicts": []}
    manifest, corrections = apply_key_corrections(manifest, component, pmid_keys)
    if fill:  # as utils/merge_and_correct_manifests.py does: fill empty cells, then trim whitespace
        manifest = manifest_cleanup.trim_whitespace(manifest_cleanup.fill_empty_cells(manifest.copy(), component))
    carried = {}
    if reference is not None and carry and merge_stats["alias"]:
        manifest, carried = carry_columns(manifest, reference, merge_stats["alias"], carry)
    validation = validate_manifest(manifest, component, ref)
    path = out_dir / f"{component}_merged_manifest.csv"
    manifest.to_csv(path, index=False, lineterminator="\n")
    write_validation(validation, out_dir)
    corrections.to_csv(out_dir / "key_corrections.csv", index=False)
    conflicts = pd.DataFrame(merge_stats["id_conflicts"], columns=ID_CONFLICT_COLUMNS)
    conflicts.to_csv(out_dir / "id_conflicts.csv", index=False)
    lines = [
        f"{component}: union rows={merge_stats['rows_in']}, manifest rows={merge_stats['rows_out']} "
        f"(alias {merge_stats['alias']!r}, {'merged' if merge else 'not merged'}: {merge_stats['merged_groups']} merged groups, {merge_stats['blank_alias']} blank-alias rows kept)",
        f"validation (schema {ref or model_contract.MODEL_REF}): {len(validation)} failures in "
        f"{validation['key'].nunique()} rows; all rows kept (D-Q39)",
        f"id conflicts (same alias, different ids; --upload is refused while any): {len(conflicts)}"
        + (f", see {out_dir / 'id_conflicts.csv'}" if len(conflicts) else ""),
        f"key corrections: {corrections['status'].value_counts().to_dict() if len(corrections) else 'none'}",
        f"carried columns, rows left blank: {carried or 'none'}",
        f"manifest: {path}",
    ]
    (out_dir / "report.txt").write_text("\n".join(lines) + "\n")
    return {"component": component, "path": path, "manifest": manifest, "validation": validation,
            "corrections": corrections, "id_conflicts": conflicts, "lines": lines, "union_stats": union_stats}


def upload_manifest(syn, component: str, path, comment: str) -> str:
    """LIVE WRITE: store ``path`` as a new version of the component's current manifest file."""
    manifest_id = utils.CONFIG[RESOURCE[component]]["manifest"]
    entity = File(id=manifest_id, download_file=False).get(synapse_client=syn)
    entity.path = str(path)
    entity.version_comment = comment
    entity = entity.store(synapse_client=syn)
    return f"{entity.id} version {entity.version_number}"


def regenerate_main(args, syn=None) -> int:
    if args.upload and args.input_dir:
        raise SystemExit("--upload is refused with --input-dir: local converted.csv files are test input, not production data")
    pmid_keys = args.pmid_keys or (DEFAULT_PMID_KEYS if os.path.exists(DEFAULT_PMID_KEYS) else None)
    mapping = load_mapping(args.mapping, args.schema_version)
    failures = 0
    results = []
    for component in args.components:
        if args.input_dir:
            union, stats = merge_tables.build_union_from_dir(component, args.input_dir)
        else:
            syn = syn or utils.syn_login()
            grants = args.grant_projects or merge_tables.list_grant_projects(syn)
            union, stats = merge_tables.build_union(syn, component, grants)
        print(f"{component} union: {stats}")
        if union.empty:
            raise SystemExit(f"{component} union is empty; nothing to regenerate")
        reference = None
        if args.carry_columns:
            if args.reference_csv:
                reference = pd.read_csv(args.reference_csv, dtype=str, keep_default_na=False)
            else:
                syn = syn or utils.syn_login()
                reference = utils.fill_blank(model_contract.read_manifest(syn, utils.CONFIG[RESOURCE[component]]["manifest"]))
        result = regenerate_component(component, union, mapping, args.output_dir, args.schema_version,
                                      pmid_keys, reference, tuple(args.carry_columns), stats,
                                      merge=component in args.merge_components, fill=not args.no_fill)
        print("\n".join(result["lines"]))
        failures += len(result["validation"])
        results.append(result)
    conflicted = [r["component"] for r in results if len(r["id_conflicts"])]
    if args.upload and conflicted:
        raise SystemExit(f"--upload refused: {', '.join(conflicted)} has rows with the same alias but different ids "
                         f"(id_conflicts.csv under {args.output_dir}); resolve them in the grant tables and regenerate")
    for result in results:
        if args.upload:
            syn = syn or utils.syn_login()
            print("LIVE WRITE: uploaded", upload_manifest(syn, result["component"], result["path"],
                  f"I6 regenerated from the {args.schema_version or model_contract.MODEL_REF} union"))
        else:
            print("Dry run: nothing uploaded (use --upload).")
    return 2 if failures else 0


def main(argv=None):

    args = get_args(argv)
    if args.regenerate:
        raise SystemExit(regenerate_main(args))

    inputList, attributeMap, inputManifest, merge, strict, debug = (
        args.l, args.p or args.mapping, args.tp, args.m, args.s, args.db
    )
    if attributeMap is None:
        raise SystemExit("Legacy mode needs -p (mapping CSV)")
    syn = utils.syn_login()
    mapping = pd.read_csv(attributeMap, header=0)

    if inputManifest is None:
        compare_and_subset = True
        print("Accessing requested tables...")
        newTables = get_tables(syn, inputList, merge)
        print("\nTable(s) downloaded from Synapse and stored as CSVs in output folder!")
    else:
        tables, newNames = [], []
        table = pd.read_csv(inputManifest, header=0)
        name = table.loc[:, "Component"].iat[1]
        if merge:
            tables.append(table)
            compare_and_subset = True
        else:
            tables.append(inputManifest)
            compare_and_subset = False
        newNames.append(name)
        newTables = list(zip(tables, newNames))
        print(f"\nReading provided table at {inputManifest} of type {name}.")

    if merge:
        print("\nMerging rows with matching identifier...")
        newTables = combine_rows(newTables, mapping)
        print("\nMatching rows merged!")
        print("\nMerged table(s) converted to CSV and stored in local output folder!")
        print("\nSubsequent operations will be performed on merged manifest(s).")
        if compare_and_subset:
            print("\nIdentifying new and updated resource records from to database entries...\n")
            refTables = get_ref_tables(syn, newTables)
            updatedTables = compare_and_subset_tables(refTables, mapping, strict, debug)
            print("\nNew and updated resource records found!")
    else:
        print("\nSubsequent operations will be performed on un-merged manifest(s).")
        print("\nDatabase comparison and filtering is disabled.")
        updatedTables = newTables

    validate_tables(updatedTables, args.schema_version)
    print("\nValidation reports stored in local output folder! No rows were trimmed.")


if __name__ == "__main__":
    main()
