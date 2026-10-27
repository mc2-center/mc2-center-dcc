"""
migrate_admin_manifests_14.py

Migrates one admin manifest of CCKP - MC2 Admin (syn28742843) to a shared RecordSet:
Grant ``syn53259587`` (GrantView), Project ``syn59074382`` (ProjectView) and
PersonView ``syn38301033``. Each becomes ``MC2Center_<Component>_RecordSet`` in the
folder ``utils.ADMIN_RECORDSET_FOLDER`` (``admin_recordsets``) of the admin project;
there is one RecordSet per component and no per-grant split. After the migration the
grant, project and people syncs read the RecordSets (``utils.MANIFEST_SOURCE``,
``--manifest-source recordset``).

This is the sibling of ``migrate_grant_tables_14.py`` (the grant-level tables) and
reuses its machinery: value normalization (``portal_tables/value_maps.py``), local
validation against the 14.x schema, the write path (``recordsets.write_logged``:
pending/written/done rows in the write log, lock and move checks, Grid only), the
``created`` / ``verified`` rows, read-back verification, resume state and the
comparison against the source. What differs: one source table per component, the
admin headers (``model_contract.LEGACY_ADMIN_HEADERS`` and ``RENAMES_13_TO_14``), and
no duplicate resolution (duplicate keys are reported, and the component is not written).

Conversion, per component:
* headers are renamed to 14.x class labels; columns that are not schema columns
  (``Component``, ``Id``, ``entityId``) are dropped; the schema columns the source
  lacks (``InstitutionKey``, ``PersonViewKey``, ``ProjectShortName``) stay empty;
* values are normalized by the grant migration's rules (``enum_case``,
  ``nonpreferred``, ``override`` ...); enum columns that have no category in
  ``all_valid_values.csv`` get a case-insensitive match against the schema enum;
* list cells (PersonView is a table of list columns) of columns that the schema
  types as a string are joined with ``", "`` (``list_to_string``); ``ConsortiumKey``
  and ``GrantViewKey`` hold one or more keys, split and rejoined per item;
* ``ConsortiumKey`` should hold ``program.*`` IDs. No rule maps a display name
  (``CSBC``) to its ID (the Consortium table has no ``consortiumId`` yet, plan D-1), so
  every other value is kept and reported as unmapped; ``--overrides`` (column,
  current_value, proposed_value) supplies a curated mapping per item;
* a date column whose value is not ``YYYY-MM-DD`` is kept and reported as unmapped;
* a source without a key column (PersonView has no ``PersonView_id``) or with duplicate
  or blank keys is reported (``duplicates.csv``) and fails: converted.csv is written, nothing
  is loaded into Synapse.

Dry run is the default and writes only reports to ``<out-dir>/<run-id>/<component>/``
(converted.csv, changes.csv, unmapped.csv, validation.csv, validation_counts.csv,
duplicates.csv, summary.txt) and ``comparison.txt`` (row counts and keys against the
source table). ``--execute`` needs ``--write-log-table`` and writes only through Grid
as the grant migration does; it never overwrites an existing RecordSet.

Exit status: 0 ok; 1 failed (also keys missing or duplicated) or the comparison differs;
2 everything ran but there are unmapped values or validation failures.

author: orion.banks
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import migrate_grant_tables_14 as grants  # noqa: E402  (also puts portal_tables on sys.path)

import model_contract  # noqa: E402
import recordsets  # noqa: E402
import utils  # noqa: E402
import value_maps  # noqa: E402

logger = logging.getLogger(__name__)

COMPONENTS = list(utils.ADMIN_COMPONENTS.values())
RESOURCE = {component: resource for resource, component in utils.ADMIN_COMPONENTS.items()}
KEYS = {c: grants.KEYS[c] for c in COMPONENTS}
SOURCE_KEY_HEADER = {c: f"{c}_id" for c in COMPONENTS}  # the source table's key column, where it has one
ADMIN_LABEL = "CCKP - MC2 Admin"
# String columns that hold one or more keys; split and rejoined per item.
KEY_LIST_COLUMNS = frozenset({"ConsortiumKey", "GrantViewKey"})
CONSORTIUM_PREFIX = "program."
DUPLICATE_COLUMNS = ["key", "rows"]


def get_args(argv=None):
    parser = argparse.ArgumentParser(description="Migrate an admin manifest (Grant, Project, PersonView) to a RecordSet")
    parser.add_argument("--component", required=True, choices=COMPONENTS)
    parser.add_argument("--source", help="Synapse ID of the source table (default: utils.CONFIG manifest of the component)")
    parser.add_argument("--schema-version", default="14.0.0",
                        help="registered MC2Center-<Component>-<version> schema to bind (default: %(default)s)")
    parser.add_argument("--write-log-table", help="Synapse ID of the write-log table (required with --execute)")
    parser.add_argument("--out-dir", help="default: output/migration_14/admin/<component>")
    parser.add_argument("--overrides", help="CSV of curated corrections: column, current_value, proposed_value")
    parser.add_argument("--execute", action="store_true", help="write to Synapse; the default is a dry run")
    parser.add_argument("--run-id", default=datetime.now().strftime("%Y%m%dT%H%M%S"), help="default: a timestamp")
    parser.add_argument("--redo", action="store_true", help="repeat although done (dry run only)")
    parser.add_argument("--cache-dir", default=str(recordsets.DEFAULT_CACHE_DIR),
                        help="on-disk cache of downloaded RecordSet versions; default: %(default)s")
    args = parser.parse_args(argv)
    if args.execute and not args.write_log_table:
        parser.error("--execute needs --write-log-table")
    if args.execute and args.redo:
        parser.error("--redo is for dry runs; an --execute never overwrites a migrated RecordSet")
    args.source = args.source or utils.CONFIG[RESOURCE[args.component]]["manifest"]
    args.out_dir = args.out_dir or f"output/migration_14/admin/{args.component}"
    return args


# --- conversion (no Synapse) --------------------------------------------------


def admin_renames(component: str) -> dict[str, str]:
    """Live admin headers and 13.1.0 names -> 14.x display names (no ``<Resource> Grant Number`` rule:
    ``Grant Number`` is a GrantView attribute)."""
    renames = dict(model_contract.LEGACY_ADMIN_HEADERS.get(component, {}))
    renames.update(model_contract.RENAMES_13_TO_14.get(component, {}))
    return renames


def to_labels(source: pd.DataFrame, component: str) -> pd.DataFrame:
    """Text cells (lists kept) with 14.x class-label headers; unknown headers raise ``grants.ConversionError``."""
    df = pd.DataFrame({c: grants._as_text(source[c]) for c in source.columns})
    renames = admin_renames(component)
    df = df.rename(columns={c: renames[c] for c in df.columns if c in renames and renames[c] not in df.columns})
    labels = model_contract.label_map()
    unknown = [c for c in df.columns if c not in model_contract.PASSTHROUGH and c not in labels]
    if unknown:
        raise grants.ConversionError(f"unknown headers: {unknown}")
    return model_contract.normalize_headers(df, to="keys")


def build_normalizer(component: str, overrides=None, ref=None) -> "value_maps.Normalizer":
    """The grant migration's normalizer plus a case-insensitive map for each schema enum that has no
    ``all_valid_values.csv`` category (``PortalDisplay``: ``True`` -> ``TRUE``)."""
    normalizer = value_maps.Normalizer.for_component(component, overrides, ref=ref)
    for column, prop in grants.load_schema(component, ref)["properties"].items():
        enum = prop.get("enum") or prop.get("items", {}).get("enum")
        if enum and column not in normalizer.enum_maps:
            normalizer.enum_maps[column] = value_maps.EnumMap(enum)
    return normalizer


def mint_person_key(name) -> str:
    """``person.<name-slug>``: NFKD-normalised without accents, lowercase, each run of non-alphanumeric
    characters one ``-``, no leading or trailing ``-``. Blank when ``name`` has no letter or digit."""
    if name is None or (not isinstance(name, str) and pd.isna(name)):
        return ""
    text = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode().lower()
    slug = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return f"person.{slug}" if slug else ""


def _joined(items) -> str:
    return ", ".join(str(i) for i in items)


def convert_admin_table(source: pd.DataFrame, component: str, normalizer: "value_maps.Normalizer", ref=None):
    """``(converted, changes, unmapped, dropped, duplicates, key_missing)`` for one admin table.

    ``converted`` has the schema columns the source holds (keys stripped; blank when the source has no
    key column, ``key_missing``). ``changes`` and ``unmapped`` identify a row by its key (``row N`` without
    one). ``duplicates`` lists keys that occur more than once (``key, rows``) or are blank; nothing is
    resolved. Raises ``grants.ConversionError`` for unknown headers.
    """
    key = KEYS[component]
    df = to_labels(source, component).reset_index(drop=True)
    schema = recordsets.schema_columns(component)
    dropped = [c for c in df.columns if c not in schema]
    df = df[[c for c in df.columns if c in schema]].copy().astype(object)
    key_missing = key not in df.columns
    changes: list[dict] = []
    unmapped: list[dict] = []
    if key_missing and component == "PersonView" and "Name" in df.columns:
        # The PersonView table has no key. Mint it from Name (owner decision, 2026-10-27). The minted key is
        # stored in the RecordSet and is never re-derived from Name later: a renamed person keeps the key.
        df.insert(0, key, [mint_person_key(n) for n in df["Name"]])
        key_missing = False
        changes += [{"key": k, "column": key, "from": "", "to": k, "rule": "minted_key"} for k in df[key] if k]
    elif key_missing:
        df.insert(0, key, [f"row {i + 1}" for i in range(len(df))])
    else:
        df[key] = [str(k).strip() for k in df[key]]
    array_cols = recordsets.list_columns(component)

    # List cells in a column the schema types as a string: joined (key-list columns are normalized per item first).
    for column in df.columns:
        if column == key or column in array_cols:
            continue
        cells = []
        for k, cell in zip(df[key], df[column]):
            if isinstance(cell, list):
                items = [str(i).strip() for i in cell if str(i).strip()]
                changes.append({"key": k, "column": column, "from": json.dumps(cell, ensure_ascii=False),
                                "to": _joined(items), "rule": "list_to_string"})
                cell = items if column in KEY_LIST_COLUMNS else _joined(items)
            cells.append(cell)
        df[column] = cells

    keyed = [c for c in KEY_LIST_COLUMNS if c in df.columns]
    saved = set(normalizer.list_cols)
    normalizer.list_cols |= set(keyed)
    try:
        out, norm_changes, norm_unmapped = normalizer.normalize_frame(df, key)
    finally:
        normalizer.list_cols = saved
    changes += norm_changes.to_dict("records")
    unmapped += norm_unmapped.to_dict("records")

    consortia = utils.load_consortium_keys()
    for column in keyed:  # one or more keys, rejoined; a ConsortiumKey name becomes its program.* ID
        cells = []
        for k, items in zip(out[key], out[column]):
            items = items if isinstance(items, list) else utils.split_cell(items)
            if column == "ConsortiumKey":
                mapped = []
                for item in items:
                    if item in consortia:
                        changes.append({"key": k, "column": column, "from": item, "to": consortia[item], "rule": "consortium_key"})
                        item = consortia[item]
                    elif not item.startswith(CONSORTIUM_PREFIX):
                        unmapped.append({"key": k, "column": column, "value": item})
                    mapped.append(item)
                items = mapped
            cells.append(_joined(items))
        out[column] = cells

    keys = [str(k).strip() for k in out[key]]
    counts = pd.Series(keys).value_counts()
    rows = {k: ", ".join(str(i + 1) for i, x in enumerate(keys) if x == k) for k in counts[counts > 1].index}
    duplicates = pd.DataFrame([{"key": k, "rows": r} for k, r in rows.items()], columns=DUPLICATE_COLUMNS)
    if "" in keys and not key_missing:
        duplicates = pd.concat([duplicates, pd.DataFrame([{"key": "", "rows": ", ".join(
            str(i + 1) for i, x in enumerate(keys) if x == "")}])], ignore_index=True)
    return (out, pd.DataFrame(changes, columns=value_maps.CHANGE_COLUMNS),
            pd.DataFrame(unmapped, columns=value_maps.UNMAPPED_COLUMNS), dropped, duplicates, key_missing)


def validate_admin(converted: pd.DataFrame, component: str, key_missing: bool, ref=None) -> pd.DataFrame:
    """Local validation failures; without a source key every row also fails ``required`` for the key."""
    validation = grants.validate_frame(converted, component, ref)
    if key_missing:
        key = KEYS[component]
        validation = validation[validation["field"] != key]
        missing = pd.DataFrame({"key": converted[key], "field": key, "validator": "required",
                                "message": f"'{key}' is a required property (the source table has no key column)"})
        validation = pd.concat([missing, validation], ignore_index=True)
    return validation


# --- one component ------------------------------------------------------------


def read_source(syn, table_id: str) -> pd.DataFrame:
    return grants.read_source(syn, table_id)


def source_keys_of(source: pd.DataFrame, component: str) -> list[str] | None:
    header = SOURCE_KEY_HEADER[component]
    if header not in source.columns:
        return None
    return [str(v).strip() for v in grants._as_text(source[header])]


def process_component(syn, entry, args, normalizer, write_log, report_dir: Path, list_cols, set_like):
    """Convert (and with ``--execute`` migrate) the admin table; ``(summary, converted, source_keys)``.

    ``converted`` is None when conversion failed; ``source_keys`` are the source table's keys (``row N``
    labels when it has no key column).
    """
    component, key = args.component, KEYS[args.component]
    summary = {"grant": entry["grant"], "table": entry["table"], "status": "", "message": ""}
    report_dir.mkdir(parents=True, exist_ok=True)
    source = read_source(syn, entry["table"])
    summary["source_rows"] = len(source)
    source_keys = source_keys_of(source, component)
    try:
        converted, changes, unmapped, dropped, duplicates, key_missing = convert_admin_table(
            source, component, normalizer, args.model_ref)
    except grants.ConversionError as err:
        summary.update(status="failed", message=str(err))
        return summary, None, source_keys or []
    minted = source_keys is None and not key_missing
    if source_keys is None:
        source_keys = list(converted[key])  # minted keys (or row labels): there are no source keys to compare
    validation = validate_admin(converted, component, key_missing, args.model_ref)
    if key_missing:
        converted = converted.assign(**{key: ""})
    converted_path = report_dir / "converted.csv"
    grants._write(converted, converted_path)
    grants._write(changes, report_dir / "changes.csv")
    grants._write(unmapped, report_dir / "unmapped.csv")
    grants._write(validation, report_dir / "validation.csv")
    grants._write(grants.validation_counts(validation), report_dir / "validation_counts.csv")
    if len(duplicates):
        grants._write(duplicates, report_dir / "duplicates.csv")
    no_source = sorted(recordsets.schema_columns(component) - set(converted.columns))
    summary.update(
        rows=len(converted), changes=len(changes), unmapped=len(unmapped), validation_failures=len(validation),
        dropped=dropped, no_source_columns=no_source, key_missing=key_missing, keys_minted=minted,
    )
    if key_missing:
        summary.update(status="failed", message=(
            f"the source table has no {SOURCE_KEY_HEADER[component]} column, so {key} cannot be filled "
            "(no rule gives it a value); converted.csv has blank keys and nothing is written"))
        return summary, converted, source_keys
    if len(duplicates):
        summary.update(status="failed", message=(
            f"{len(duplicates)} duplicate or blank key(s) (see duplicates.csv); none is resolved and nothing is written"))
        return summary, converted, source_keys
    if converted.empty:
        summary.update(status="empty_source", message="no rows; no RecordSet created, nothing written")
        return summary, converted, source_keys
    if not args.execute:
        summary.update(status="dryrun", message="dry run: nothing written")
        return summary, converted, source_keys
    grants.migrate_recordset(syn, entry, args, write_log, converted, converted_path, report_dir, summary, list_cols, set_like)
    return summary, converted, source_keys


def options_hash(args) -> str:
    """What changes the conversion: a done component is skipped on rerun only if this matches."""
    effective = {
        "component": args.component, "source": args.source, "schema_version": args.schema_version,
        "overrides": Path(args.overrides).read_bytes().hex() if args.overrides else "",
        "model_ref": getattr(args, "model_ref", None) or model_contract.MODEL_REF,
    }
    return hashlib.sha256(json.dumps(effective, sort_keys=True).encode()).hexdigest()


def comparison_text(args, summary: dict, source_keys: list[str], converted_keys: list[str], has_source_keys: bool):
    """``(ok, text)``: row counts and keys of the converted frame against the source table."""
    lines = [f"{args.component} comparison, run {args.run_id} ({'execute' if args.execute else 'dry run'})"]
    if summary.get("keys_minted"):  # no source keys: the rows must be all there, each with its own minted key
        rows = int(summary.get("source_rows", 0))
        unique = len(set(converted_keys)) == len(converted_keys) and all(converted_keys)
        ok = len(converted_keys) == rows and unique
        lines += [f"{args.component}: source table rows={rows}; converted rows={len(converted_keys)}; minted keys "
                  f"unique and non-blank={unique}", f"  {'EQUAL' if ok else 'DIFFERENT'}",
                  f"  the source table has no {SOURCE_KEY_HEADER[args.component]} column: keys are minted, not compared"]
        if args.execute:
            lines.append(f"  RecordSet {summary.get('recordset', '-')}: {summary.get('status')}")
        return ok, "\n".join(lines)
    ok, more = grants.compare_keys("converted frame vs source table", source_keys, converted_keys, "source table")
    lines += more
    if not has_source_keys:
        lines.append(f"  the source table has no {SOURCE_KEY_HEADER[args.component]} column: only the row count is "
                     "compared (keys are row labels)")
    if args.execute:
        lines.append(f"  RecordSet {summary.get('recordset', '-')}: {summary.get('status')}")
    return ok, "\n".join(lines)


def main(argv=None, syn=None):
    args = get_args(argv)
    syn = syn or utils.syn_login()
    component, key = args.component, KEYS[args.component]
    write_log = recordsets.SynapseTableWriteLog(syn, args.write_log_table) if args.write_log_table else None
    list_cols = recordsets.list_columns(component)
    set_like = recordsets.set_like_columns(component)
    overrides = value_maps.load_overrides(args.overrides) if args.overrides else None
    args.model_ref = grants.resolve_model_ref(args)
    normalizer = build_normalizer(component, overrides, args.model_ref)
    out_dir = Path(args.out_dir)
    run_dir = out_dir / args.run_id
    state_path = out_dir / "state.json"
    state = grants.load_state(state_path)
    mode = "execute" if args.execute else "dryrun"
    options = options_hash(args)
    entry = {
        "grant": component, "project": utils.ADMIN_PROJECT, "table": args.source,
        "folder_name": utils.ADMIN_RECORDSET_FOLDER,
        "description": f"{component} admin manifest of the MC2 Center ({ADMIN_LABEL}), migrated from table {args.source}.",
    }
    print(f"Run {args.run_id}: {'EXECUTE' if args.execute else 'dry run'}, {component}, source {args.source}, "
          f"target {utils.admin_recordset_name(component)} in folder {utils.ADMIN_RECORDSET_FOLDER} of {utils.ADMIN_PROJECT}")
    report_dir = run_dir / component
    previous = state["grants"].get(component, {})
    skip = (not args.redo and previous.get("mode") == mode and previous.get("status") in grants.DONE[mode]
            and previous.get("options") == options and Path(previous.get("converted", "")).exists())
    if skip:
        summary = dict(previous["summary"])
        summary["message"] = f"already {previous['status']} in run {previous['run_id']}; skipped"
        converted = recordsets._load_csv(previous["converted"], list_cols)
        source_keys = previous.get("source_keys", [])
        frames = {component: grants._frames_from(Path(previous["converted"]).parent)}
        report_dir.mkdir(parents=True, exist_ok=True)
    else:
        try:
            summary, converted, source_keys = process_component(
                syn, entry, args, normalizer, write_log, report_dir, list_cols, set_like)
        except Exception as err:
            logger.exception("%s failed", component)
            summary, converted, source_keys = (
                {"grant": component, "table": args.source, "status": "failed", "message": repr(err)}, None, [])
        frames = {component: grants._frames_from(report_dir)} if converted is not None else {}
        state["grants"][component] = {
            "mode": mode, "options": options, "status": summary["status"], "run_id": args.run_id, "summary": summary,
            "converted": str(report_dir / "converted.csv") if converted is not None else "",
            "source_keys": source_keys if converted is not None else [],
        }
        grants.save_state(state_path, state)
    (report_dir / "summary.txt").write_text(grants.summary_line(summary) + "\n")
    print(grants.summary_line(summary))

    run_dir.mkdir(parents=True, exist_ok=True)
    if converted is not None:
        has_source_keys = not summary.get("key_missing")
        converted_keys = [f"row {i + 1}" for i in range(len(converted))] if not has_source_keys else [
            str(k).strip() for k in converted[key]]
        compare_ok, text = comparison_text(args, summary, source_keys, converted_keys, has_source_keys)
    else:
        compare_ok, text = True, "nothing to compare: conversion failed"
    (run_dir / "comparison.txt").write_text(text + "\n")
    pd.DataFrame([{k: v for k, v in summary.items() if not k.startswith("_")}]).to_csv(run_dir / "summary.csv", index=False)
    report = grants.run_report([summary], frames)
    (run_dir / "run_summary.txt").write_text(report + "\n")
    if summary.get("key_missing") and frames.get(component, {}).get("converted") is not None:
        labelled = frames[component]["converted"]  # reports name a row without a key as "row N"
        frames[component]["converted"] = labelled.assign(**{key: [f"row {i + 1}" for i in range(len(labelled))]})
    if frames.get(component, {}).get("converted") is not None:  # duplicate keys: the worklist looks up the first row
        frames[component]["converted"] = frames[component]["converted"].drop_duplicates(key)
    grants.validation_worklist(frames, key).to_csv(run_dir / "validation_worklist.csv", index=False)
    print(text)
    print(report)
    if summary["status"] in grants.NEEDS_ATTENTION or not compare_ok:
        return 1
    unmapped_total = int(summary.get("unmapped", 0))
    invalid_total = int(summary.get("validation_failures", 0)) + int(summary.get("invalid_rows", 0))
    if unmapped_total or invalid_total:
        print(f"{unmapped_total} unmapped value(s), {invalid_total} validation failure(s): exit 2")
        return 2
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    sys.exit(main())
