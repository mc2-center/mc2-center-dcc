"""
migrate_grant_tables_14.py

I5a: migrates one component's grant-level manifest tables
(``<type>_synapse_storage_manifest_table``, listed in the view
``MC2_All_Project_Tables`` syn52750482) to shared RecordSets
(``MC2Center_<Component>_RecordSet`` in the grant's data-type folder).

Per grant table: read it, rename the 13.1.0 headers (``RENAMES_13_TO_14``),
normalize them to class labels, normalize the values (``portal_tables/
value_maps.py``: enums, DUO IDs, SPDX licenses, DOIs, curated ``--overrides``),
keep the schema columns plus the key, check the keys, and validate locally with
jsonschema against the 14.0.x schema. A grant with unknown headers or blank or
duplicate keys fails; the others continue.

Dry run is the default and writes only reports, to
``<out-dir>/<run-id>/<grant>/``: converted.csv, changes.csv (key, column, from,
to, rule), unmapped.csv, validation.csv (key, field, validator, message),
validation_counts.csv and summary.txt. Use ``--execute`` to write; it needs
``--write-log-table``. Per grant it finds or creates the data-type folder; skips
a RecordSet that already exists and has a pipeline write in the write log
(``already_migrated``); fails one that exists without a logged write ("exists but
not migrated; investigate"), and never overwrites. Otherwise it creates the
RecordSet and curation task (``create_grid=False``), loads the rows only through
a Grid session (``recordsets.write_logged``, the write path of
``upload_recordsets``: ``pending`` row, lock and move checks, ``written`` per
export, ``done``, recovery or abort on failure), reads the exported version back
and compares every key and cell with converted.csv, and collects Synapse's
validation results (synapse_validation.csv). That logged write is the
pipeline-owned baseline (D-Q33).

``<out-dir>/state.json`` holds the status per grant; a rerun skips grants that
finished in the same mode. ``--redo <grant>`` repeats one in a dry run.

After all grants, comparison.txt compares the legacy union (the converted
frames) with the RecordSet union (``--execute``, ``merge_tables.build_union``)
or, in a dry run, with the legacy MaterializedView (``--union-id``, a full run
only) or the per-table source row counts and keys.

Exit status: 0 all ok; 1 a grant failed (failed, locked, retry,
verification_mismatch) or the comparison differs; 2 everything ran but there
are unmapped values or validation failures (local, or Synapse's after a write).

author: orion.banks
"""

import argparse
import json
import logging
import os
import re
import sys
import urllib.error
from datetime import datetime
from functools import lru_cache
from pathlib import Path

import jsonschema
import pandas as pd
from synapseclient import operations
from synapseclient.extensions.curator import create_record_based_metadata_task
from synapseclient.models import Folder, Table

# removed when the mc2dcc package (CLI consolidation plan) lands
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "portal_tables"))
import merge_tables
import model_contract
import recordsets
import utils
import value_maps

logger = logging.getLogger(__name__)

VIEW_ID = "syn52750482"  # MC2_All_Project_Tables
UNION_PARENT = "syn28742843"  # CCKP - MC2 Admin: holds the legacy <Component>_UNION MaterializedViews
# Projects that hold grant-level tables but are not in the Grants table, labelled in reports.
UNLISTED_LABELS = {"syn52963211": "Affiliated/Non-Grant Associated"}  # MC2 Center Affiliate Resources Project
COMPONENTS = ["PublicationView", "DatasetView", "ToolView", "EducationalResource"]
KEYS = {c: f"{c}Id" for c in COMPONENTS}  # class-label upsert key of each component
FOLDER_MAP = merge_tables.FOLDER_MAP
# utils.CONFIG resource of the curated merged manifest of each component.
REFERENCE_RESOURCE = {"PublicationView": "publication", "DatasetView": "dataset", "ToolView": "tool",
                      "EducationalResource": "education"}
RESOLUTION_COLUMNS = ["key", "kept_row", "dropped_row", "reason"]

# Statuses that make the exit code 1.
NEEDS_ATTENTION = frozenset({"failed", "locked", "retry", "verification_mismatch"})
# Statuses a rerun in the same mode skips.
DONE = {"dryrun": {"dryrun"}, "execute": {"migrated", "already_migrated"}}

PROCESS_START = pd.Timestamp.now(tz="UTC")  # a pending row of our runId older than this is not ours

VALIDATION_COLUMNS = ["key", "field", "validator", "message"]


def get_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.strip().split("\n")[0])
    parser.add_argument("--component", required=True, choices=COMPONENTS)
    parser.add_argument("--grants", nargs="+", help="grant numbers to migrate (default: every grant)")
    parser.add_argument("--limit", type=int, help="stop after N grants")
    parser.add_argument(
        "--schema-version", default="14.0.0",
        help="registered MC2Center-<Component>-<version> schema to bind (default: %(default)s)",
    )
    parser.add_argument("--write-log-table", help="Synapse ID of the write-log table (required with --execute)")
    parser.add_argument("--out-dir", help="default: output/migration_14/<component>")
    parser.add_argument(
        "--overrides", help="CSV of curated corrections: column, current_value, proposed_value"
    )
    parser.add_argument(
        "--include-unlisted", action=argparse.BooleanOptionalAction, default=True,
        help="also migrate projects that hold a grant-level table but are not in the Grants table, "
        "labelled by project name (default: on)",
    )
    parser.add_argument("--execute", action="store_true", help="write to Synapse; the default is a dry run")
    parser.add_argument("--run-id", default=datetime.now().strftime("%Y%m%dT%H%M%S"), help="default: a timestamp")
    parser.add_argument("--redo", nargs="+", default=[], help="grant numbers to repeat although done (dry run only)")
    parser.add_argument(
        "--union-id",
        help="dry run: the legacy <Component>_UNION MaterializedView to compare with (full runs only); "
        f"default: look it up by name in {UNION_PARENT}",
    )
    parser.add_argument(
        "--cache-dir", default=str(recordsets.DEFAULT_CACHE_DIR),
        help="on-disk cache of downloaded RecordSet versions; default: %(default)s",
    )
    args = parser.parse_args(argv)
    if args.execute and not args.write_log_table:
        parser.error("--execute needs --write-log-table")
    if args.execute and args.redo:
        parser.error("--redo is for dry runs; an --execute never overwrites a migrated RecordSet")
    args.out_dir = args.out_dir or f"output/migration_14/{args.component}"
    return args


# --- Synapse reads ------------------------------------------------------------


def list_grant_tables(syn, component: str) -> pd.DataFrame:
    """``id, name`` of every ``<component>_synapse_storage_manifest_table`` in the view."""
    name = f"{component.lower()}_synapse_storage_manifest_table"
    return Table(id=VIEW_ID).query(
        query=f"SELECT id, name FROM {VIEW_ID} WHERE name = '{name}'",
        include_row_id_and_row_version=False,
        synapse_client=syn,
    )


def grants_map(syn) -> dict[str, str]:
    """``{grant project ID: grant number}`` from the Grants portal table."""
    df = Table(id=merge_tables.GRANTS_TABLE).query(
        query=f"SELECT grantId, grantNumber FROM {merge_tables.GRANTS_TABLE}",
        include_row_id_and_row_version=False,
        synapse_client=syn,
    )
    return {g: n for g, n in zip(df["grantId"], df["grantNumber"]) if g}


def project_of(syn, entity_id: str) -> tuple[str | None, str | None]:
    """``(ID, name)`` of the project an entity sits in."""
    for item in syn.restGET(f"/entity/{entity_id}/path")["path"]:
        if str(item.get("type", "")).endswith("Project"):
            return item["id"], item.get("name")
    return None, None


def read_source(syn, table_id: str) -> pd.DataFrame:
    return Table(id=table_id).query(
        query=f"SELECT * FROM {table_id}", include_row_id_and_row_version=False, synapse_client=syn
    )


def safe_name(label: str) -> str:
    """A grant label as a directory name."""
    return re.sub(r"[^\w.-]+", "_", label).strip("_") or "grant"


def select_grant_tables(syn, args) -> tuple[list[dict], list[str]]:
    """``(tables, skipped)``: tables to migrate (``grant``, ``project``, ``table``) and a note per skipped one.

    ``grant`` is the grant number; a project that is not in the Grants table is
    a grant labelled by its project name (``UNLISTED_LABELS``) unless
    ``--no-include-unlisted``.
    """
    grants = grants_map(syn)
    by_label = {n: g for g, n in grants.items()}
    by_label.update({label: project for project, label in UNLISTED_LABELS.items()})
    wanted = None
    if args.grants:
        unknown = [g for g in args.grants if g not in by_label]
        if unknown:
            raise SystemExit(f"Grant number(s) not in the Grants table: {unknown}")
        wanted = {by_label[g] for g in args.grants}
    tables, skipped, seen = [], [], set()
    for table_id in list_grant_tables(syn, args.component)["id"]:
        if args.limit is not None and len(tables) >= args.limit:
            break
        if wanted is not None and wanted <= seen:
            break
        project, name = project_of(syn, table_id)
        seen.add(project)
        unlisted = project not in grants
        if unlisted and not args.include_unlisted:
            skipped.append(f"{table_id}: project {project} is not in the Grants table")
            continue
        if wanted is not None and project not in wanted:
            continue
        label = UNLISTED_LABELS.get(project) or name or project if unlisted else str(grants[project])
        tables.append({"grant": label, "project": project, "table": table_id, "unlisted": unlisted})
    return tables, skipped


# --- conversion (no Synapse) --------------------------------------------------


class ConversionError(Exception):
    """A grant table that cannot be converted; the message says why."""


def _as_text(series: pd.Series) -> pd.Series:
    """String cells; an integral float (a numeric column holding blanks) loses its ``.0``."""
    if pd.api.types.is_float_dtype(series):
        return series.map(lambda v: "" if pd.isna(v) else (str(int(v)) if float(v).is_integer() else str(v)))
    return series.map(lambda v: "" if v is None or (not isinstance(v, (str, list)) and pd.isna(v)) else v)


def legacy_renames(component: str) -> dict[str, str]:
    """13.1.0 -> 14.0.0 display headers: ``RENAMES_13_TO_14`` plus the grant-number and
    tumor-type spellings older manifests used."""
    renames = dict(model_contract.LEGACY_EXTRA_RENAMES)
    renames[model_contract.grant_column_legacy_name(component)] = "GrantView Key"
    renames.update(model_contract.RENAMES_13_TO_14.get(component, {}))
    return renames


def _to_labels(source: pd.DataFrame, component: str, strict: bool = True) -> pd.DataFrame:
    """Text cells with 14.0.x class-label headers.

    Raises :class:`ConversionError` for unknown headers (``strict``; otherwise
    they are dropped) or a missing key column.
    """
    df = pd.DataFrame({c: _as_text(source[c]) for c in source.columns})
    renames = legacy_renames(component)
    df = df.rename(columns={c: renames[c] for c in df.columns if c in renames and renames[c] not in df.columns})
    labels = model_contract.label_map()
    unknown = [c for c in df.columns if c not in model_contract.PASSTHROUGH and c not in labels]
    if unknown:
        if strict:
            raise ConversionError(f"unknown headers: {unknown}")
        df = df.drop(columns=unknown)
    df = model_contract.normalize_headers(df, to="keys")
    if KEYS[component] not in df.columns:
        raise ConversionError(f"key column {KEYS[component]} is missing")
    return df


def read_reference(syn, component: str) -> pd.DataFrame:
    """The curated merged manifest of the component (read-only)."""
    return model_contract.read_manifest(syn, utils.CONFIG[REFERENCE_RESOURCE[component]]["manifest"])


def reference_frame(source: pd.DataFrame, component: str, normalizer) -> pd.DataFrame | None:
    """The merged manifest converted like a grant table (schema columns, normalized values), or None."""
    key = KEYS[component]
    try:
        df = _to_labels(source, component, strict=False)
    except ConversionError as err:
        logger.warning("Merged manifest for %s is not usable as a reference: %s", component, err)
        return None
    schema = recordsets.schema_columns(component)
    df = df[[c for c in df.columns if c in schema]]
    df = df[df[key].map(lambda v: str(v).strip() != "")]
    out, _, _ = normalizer.normalize_frame(df, key)
    out[key] = out[key].map(lambda v: str(v).strip())
    return out.reset_index(drop=True)


def _blank(value) -> bool:
    return value == "" or value == [] or value is None


def _id_value(value) -> tuple[int, int]:
    try:
        return 1, int(str(value).strip())
    except ValueError:
        return 0, 0


def resolve_duplicates(df: pd.DataFrame, legacy_id, component: str, normalizer, reference):
    """Keep one row of each duplicated key: ``(frame, resolutions)``.

    The kept row is the one equal to the merged manifest's row of that key on
    the columns both have (after the same conversion); if neither row or both
    match, or the key is not in the manifest, the row with the fewest blank
    cells; on a tie the higher legacy ``Id``. ``reference`` is a callable
    returning the converted merged manifest (or None), called once if needed.
    ``resolutions`` has ``key, kept_row, dropped_row, reason`` (source row numbers).
    """
    key = KEYS[component]
    keys = df[key].map(lambda v: str(v).strip())
    dup = keys.duplicated(keep=False) & (keys != "")
    if not dup.any():
        return df, pd.DataFrame(columns=RESOLUTION_COLUMNS)
    set_like = recordsets.set_like_columns(component)
    norm, _, _ = normalizer.normalize_frame(df, key)
    ref = reference()
    ref_rows: dict[str, list[dict]] = {}
    if ref is not None:
        for row in ref.to_dict("records"):
            ref_rows.setdefault(row[key], []).append(row)
    ids = list(legacy_id) if legacy_id is not None else [""] * len(df)

    def label(i):
        return f"row {i + 1}" + (f" (Id {ids[i]})" if str(ids[i]).strip() else "")

    def matches(i, candidates):
        row = norm.iloc[i]
        return any(
            all(recordsets._cells_equal(row[c], cand[c], c in set_like) for c in norm.columns if c in cand and c != key)
            for cand in candidates
        )

    drop, resolutions = set(), []
    for k in dict.fromkeys(keys[dup]):
        rows = [i for i in range(len(df)) if keys.iloc[i] == k]
        hits = [i for i in rows if matches(i, ref_rows.get(k, []))]
        if len(hits) == 1:
            kept, reason = hits[0], "matches the merged manifest"
        else:
            pool = hits or rows
            if hits:
                why = "both rows match the merged manifest" if len(hits) == len(rows) else "several rows match the merged manifest"
            elif k in ref_rows:
                why = "no row matches the merged manifest"
            else:
                why = "key is not in the merged manifest" if ref is not None else "no merged manifest to compare"
            blanks = {i: sum(_blank(norm.iloc[i][c]) for c in norm.columns) for i in pool}
            best = min(blanks.values())
            top = [i for i in pool if blanks[i] == best]
            if len(top) == 1:
                kept, reason = top[0], f"{why}; most complete row ({best} blank cells)"
            else:
                kept = max(top, key=lambda i: _id_value(ids[i]))
                reason = f"{why}; tie on completeness ({best} blank cells), higher legacy Id"
        for i in rows:
            if i != kept:
                drop.add(i)
                resolutions.append({"key": k, "kept_row": label(kept), "dropped_row": label(i), "reason": reason})
    keep = [i for i in range(len(df)) if i not in drop]
    return df.iloc[keep].reset_index(drop=True), pd.DataFrame(resolutions, columns=RESOLUTION_COLUMNS)


def convert_table(source: pd.DataFrame, component: str, normalizer: "value_maps.Normalizer", reference=lambda: None):
    """``(converted, changes, unmapped, dropped, resolutions)`` for one grant table.

    A key that occurs twice is resolved to one row (:func:`resolve_duplicates`,
    against ``reference``). Raises :class:`ConversionError` for unknown headers,
    a missing key column, or blank keys.
    """
    key = KEYS[component]
    df = _to_labels(source, component).reset_index(drop=True)
    legacy_id = df["Id"] if "Id" in df.columns else None
    schema = recordsets.schema_columns(component)
    dropped = [c for c in df.columns if c not in schema]
    df, resolutions = resolve_duplicates(df[[c for c in df.columns if c in schema]], legacy_id, component, normalizer, reference)
    df, changes, unmapped = normalizer.normalize_frame(df, key)
    try:
        df = recordsets.check_upload_keys(df, key)
    except ValueError as err:
        raise ConversionError(f"bad keys: {err}") from err
    return df.reset_index(drop=True), changes, unmapped, dropped, resolutions


# --- local validation ---------------------------------------------------------

_URI = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:\S+$")
_REQUIRED = re.compile(r"^'(.+)' is a required property$")


def _format_checker() -> jsonschema.FormatChecker:
    checker = jsonschema.FormatChecker(formats=[])
    checker.checks("uri")(lambda value: not isinstance(value, str) or bool(_URI.match(value)))
    return checker


def _instance(row: dict, properties: dict) -> dict:
    """A row as the JSON object Synapse validates: blank cells absent, numbers numeric."""
    out = {}
    for column, value in row.items():
        if value == "" or value == []:
            continue
        kind = properties.get(column, {}).get("type")
        if kind in ("number", "integer") and isinstance(value, str):
            try:
                value = float(value) if kind == "number" else int(value)
            except ValueError:
                pass  # left as text: the type error is the finding
        out[column] = value
    return out


@lru_cache(maxsize=None)
def load_schema(component: str, ref: str | None = None) -> dict:
    """``json_schemas/<component>.json`` at the data-models tag ``ref`` (e.g. ``v14.0.1``).

    An unavailable tag logs a warning and falls back to ``MODEL_REF``'s schema.
    """
    try:
        path = model_contract.fetch(f"json_schemas/{component}.json", ref=ref)
    except (urllib.error.URLError, FileNotFoundError) as err:
        logger.warning("Schema %s at data-models %s is not available (%s); validating against %s",
                       component, ref, err, model_contract.MODEL_REF)
        path = model_contract.fetch(f"json_schemas/{component}.json")
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def validate_frame(frame: pd.DataFrame, component: str, ref: str | None = None) -> pd.DataFrame:
    """Local jsonschema failures: ``key, field, validator, message``.

    The schema is the one at data-models tag ``ref`` (``--schema-version``).

    Array cells are the lists in ``frame`` (a string holds one string, as
    ``GrantViewKey`` does). Blank cells are absent, so ``required`` fails for them.
    """
    schema = load_schema(component, ref)
    cls = jsonschema.validators.validator_for(schema)
    validator = cls(schema, format_checker=_format_checker())
    key = KEYS[component]
    rows = []
    for record in frame.to_dict("records"):
        for error in sorted(validator.iter_errors(_instance(record, schema["properties"])), key=lambda e: list(map(str, e.path))):
            if error.path:
                field = str(error.path[0])
            else:
                match = _REQUIRED.match(error.message)
                field = match.group(1) if match else ""
            rows.append({"key": record[key], "field": field, "validator": error.validator, "message": error.message})
    return pd.DataFrame(rows, columns=VALIDATION_COLUMNS)


def validation_counts(validation: pd.DataFrame) -> pd.DataFrame:
    """Failures counted by field and validator, with one example message."""
    if validation.empty:
        return pd.DataFrame(columns=["field", "validator", "count", "example"])
    counts = (
        validation.groupby(["field", "validator"])
        .agg(count=("key", "size"), example=("message", "first"))
        .reset_index()
        .sort_values(["count", "field"], ascending=[False, True])
    )
    return counts


# --- one grant ----------------------------------------------------------------


def _write(df: pd.DataFrame, path: Path) -> None:
    recordsets.to_csv(df, path)


def find_or_create_folder(syn, project_id: str, name: str) -> str:
    folder_id = operations.find_entity_id(name=name, parent=project_id, synapse_client=syn)
    if folder_id is None:
        folder_id = Folder(name=name, parent_id=project_id).store(synapse_client=syn).id
    return folder_id


def create_recordset(syn, args, project_id: str, folder_id: str, grant: str, table_id: str) -> str:
    """Create the RecordSet and its curation task (no Grid session) and return the RecordSet ID."""
    component = args.component
    schema_uri = f"MC2Center-{component}-{args.schema_version}"
    record_set, _task = create_record_based_metadata_task(
        synapse_client=syn,
        project_id=project_id,
        folder_id=folder_id,
        record_set_name=f"MC2Center_{component}_RecordSet",
        record_set_description=f"{component} metadata of grant {grant}, migrated from table {table_id}.",
        curation_task_name=f"MC2Center_{component}_CurationTask",
        upsert_keys=[KEYS[component]],
        instructions=f"Edit the {component} records in the grid. Rows are validated against {schema_uri}.",
        schema_uri=schema_uri,
        bind_schema_to_record_set=True,
        create_grid=False,
    )
    return record_set.id


def _only(df: pd.DataFrame, columns) -> pd.DataFrame:
    return df[[c for c in df.columns if c in columns]]


def migrate_recordset(syn, entry, args, write_log, converted, converted_path, report_dir, summary, list_cols, set_like):
    """``--execute`` for one grant; updates ``summary`` (status, message, ...)."""
    component, key = args.component, KEYS[args.component]
    stored = recordsets.schema_columns(component) | {key}
    folder_id = find_or_create_folder(syn, entry["project"], FOLDER_MAP[component])
    rs_id = recordsets.find_recordset(syn, folder_id, component)
    principal = recordsets.principal_id(syn)
    recovered = {}
    if rs_id is not None:
        summary["recordset"] = rs_id
        locker = write_log.locked_by(rs_id, args.run_id, since=PROCESS_START)
        if locker:
            summary.update(status="locked", message=(
                f"run {locker} has an unfinished write (pending row under {recordsets.LOCK_HOURS} h old); skipped"))
            return
        latest, _ = recordsets.recordset_info(syn, rs_id)
        # A run that died after exporting leaves a pending row; claim its versions.
        recovered = recordsets.recover_pending(
            syn, rs_id, write_log, latest, record=True, skip_younger_than=recordsets.LOCK_HOURS
        )
        if not write_log.pipeline_map(rs_id):
            summary.update(status="failed", message=f"{rs_id} exists but not migrated; investigate")
            return
        if not recovered:
            summary.update(status="already_migrated", message=f"{rs_id} already migrated (write log has its load)")
            return
        summary["recovered"] = sorted(recovered)
        created = [max(recovered)]
        summary["version"] = created[-1]
    else:
        rs_id = create_recordset(syn, args, entry["project"], folder_id, entry["grant"], entry["table"])
        summary["recordset"] = rs_id
        latest, upsert = recordsets.recordset_info(syn, rs_id)
        if upsert != [key]:
            summary.update(status="failed", message=f"{rs_id} was created with upsert key {upsert}, expected {[key]}")
            return
        try:
            created = recordsets.write_logged(
                syn, rs_id, converted_path, write_log, args.run_id, principal, len(converted), latest
            )
        except recordsets.WriteRefused as refused:
            summary.update(status=refused.status, message=refused.message)
            return
        summary["version"] = created[-1]

    version = created[-1]
    written = recordsets.read_recordset(syn, rs_id, version=version, cache_dir=args.cache_dir, list_cols=list_cols)
    problems = recordsets.differences(converted, _only(written, stored), key, set_like)
    validation = recordsets.get_validation(
        syn, rs_id, version, report_dir, [recordsets._key_of(r, [key]) for _, r in written.iterrows()]
    )
    if validation is not None:
        validation.to_csv(report_dir / "synapse_validation.csv", index=False)
        summary["invalid_rows"] = int((~validation["is_valid"].astype(str).str.lower().eq("true")).sum())
    notes = [f"wrote v{version}" if not recovered else f"recovered v{version} from an earlier run"]
    newest, _ = recordsets.recordset_info(syn, rs_id)
    if newest > version:
        notes.append(f"v{newest} exists after the load; contributor edits are handled by the next sync's replay")
    if problems:
        (report_dir / "verification.txt").write_text("\n".join(problems) + "\n")
        summary.update(status="verification_mismatch", message=(
            f"v{version} differs from converted.csv ({len(problems)} differences)"))
        return
    summary.update(status="migrated", message="; ".join(notes))


def process_grant(syn, entry, args, normalizer, write_log, report_dir: Path, list_cols, set_like, reference=lambda: None):
    """Convert (and with ``--execute`` migrate) one grant table.

    Returns ``(summary, converted, source_keys)``; ``converted`` is None when the
    grant failed before conversion finished.
    """
    component, key = args.component, KEYS[args.component]
    summary = {"grant": entry["grant"], "table": entry["table"], "status": "", "message": ""}
    report_dir.mkdir(parents=True, exist_ok=True)
    source = read_source(syn, entry["table"])
    summary["source_rows"] = len(source)
    source_key = f"{component}_id"
    source_keys = [str(v).strip() for v in source[source_key]] if source_key in source.columns else []
    try:
        converted, changes, unmapped, dropped, resolutions = convert_table(source, component, normalizer, reference)
    except ConversionError as err:
        summary.update(status="failed", message=str(err))
        return summary, None, source_keys
    if dropped:
        logger.info("%s: not schema columns, dropped: %s", entry["grant"], dropped)
    validation = validate_frame(converted, component, f"v{args.schema_version}")
    converted_path = report_dir / "converted.csv"
    _write(converted, converted_path)
    _write(changes, report_dir / "changes.csv")
    _write(unmapped, report_dir / "unmapped.csv")
    _write(validation, report_dir / "validation.csv")
    _write(validation_counts(validation), report_dir / "validation_counts.csv")
    if len(resolutions):
        _write(resolutions, report_dir / "duplicates_resolved.csv")
    summary.update(
        rows=len(converted), changes=len(changes), unmapped=len(unmapped), validation_failures=len(validation),
        dropped=dropped, duplicates_resolved=len(resolutions),
        truncated=int((changes["rule"] == "truncated").sum()),
    )
    if not args.execute:
        summary.update(status="dryrun", message="dry run: nothing written")
        return summary, converted, source_keys
    migrate_recordset(syn, entry, args, write_log, converted, converted_path, report_dir, summary, list_cols, set_like)
    return summary, converted, source_keys


def summary_line(s: dict) -> str:
    parts = [f"{s['grant']}: {s['status']}"]
    for field in ("table", "recordset", "version", "source_rows", "rows", "changes", "unmapped",
                  "validation_failures", "duplicates_resolved", "truncated", "invalid_rows", "recovered"):
        if field in s:
            parts.append(f"{field}={s[field]}")
    if s.get("message"):
        parts.append(s["message"])
    return " ".join(parts)


# --- state --------------------------------------------------------------------


def load_state(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text())
    return {"grants": {}}


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True))
    tmp.replace(path)


# --- comparison ---------------------------------------------------------------


def compare_keys(label: str, expected_keys: list[str], actual_keys: list[str], expected_name: str) -> tuple[bool, list[str]]:
    """Row counts and primary-key sets must be equal; returns ``(ok, report lines)``."""
    exp, act = set(expected_keys), set(actual_keys)
    lines = [
        f"{label}: {expected_name} rows={len(expected_keys)} unique keys={len(exp)}; "
        f"compared rows={len(actual_keys)} unique keys={len(act)}"
    ]
    ok = len(expected_keys) == len(actual_keys) and exp == act
    for name, keys in (("missing from the compared side", sorted(exp - act)), ("extra in the compared side", sorted(act - exp))):
        if keys:
            lines.append(f"  {len(keys)} key(s) {name}: {keys[:20]}{' ...' if len(keys) > 20 else ''}")
    lines.append(f"  {'EQUAL' if ok else 'DIFFERENT'}")
    return ok, lines


def comparison(syn, args, legacy_keys: dict[str, list[str]], source_keys: dict[str, list[str]], projects: dict[str, str],
               migrated: list[str], full_run: bool) -> tuple[bool, str]:
    """Compare the legacy union with the RecordSet union (``--execute``) or the legacy view / source tables."""
    component = args.component
    key_display = f"{component}_id"
    lines = [f"{component} comparison, run {args.run_id} ({'execute' if args.execute else 'dry run'})"]
    ok = True
    if args.execute:
        grants = [g for g in migrated]
        union, stats = merge_tables.build_union(syn, component, [projects[g] for g in grants])
        legacy = [k for g in grants for k in legacy_keys[g]]
        actual = [str(k).strip() for k in union[key_display]] if key_display in union.columns else []
        res, more = compare_keys("RecordSet union vs legacy union (converted frames)", legacy, actual, "legacy")
        ok &= res
        lines += more
        lines.append(f"  union stats: {stats}")
        if stats["missing"]:
            ok = False
            lines.append(f"  {stats['missing']} grant(s) have no RecordSet in the union")
    else:
        grants = list(legacy_keys)
        legacy = [k for g in grants for k in legacy_keys[g]]
        union_id = args.union_id or operations.find_entity_id(
            name=f"{component}_UNION", parent=UNION_PARENT, synapse_client=syn
        )
        if union_id and full_run:
            view = Table(id=union_id).query(
                query=f"SELECT {key_display} FROM {union_id}",
                include_row_id_and_row_version=False, synapse_client=syn,
            )
            actual = [str(k).strip() for k in view[key_display]]
            res, more = compare_keys(f"legacy view {union_id} vs converted frames", actual, legacy, "legacy view")
            ok &= res
            lines += more
        else:
            why = "no legacy view found" if not union_id else "the run is filtered (--grants / --limit), skipped tables or has failed grants"
            lines.append(f"legacy MaterializedView not compared ({why}); comparing with the source tables")
            source = [k for g in grants for k in source_keys[g]]
            res, more = compare_keys("converted frames vs source tables", source, legacy, "source tables")
            ok &= res
            lines += more
    return ok, "\n".join(lines)


# --- run summary --------------------------------------------------------------


def run_report(summaries: list[dict], frames: dict[str, dict]) -> str:
    """Totals over every grant: changes by rule, unmapped and truncated values, duplicates, validation failures."""
    lines = []
    changes = [f["changes"] for f in frames.values() if f.get("changes") is not None and len(f["changes"])]
    all_changes = pd.concat(changes) if changes else pd.DataFrame(columns=value_maps.CHANGE_COLUMNS)
    by_rule = all_changes[all_changes["rule"] != "truncated"].groupby("rule").size().sort_values(ascending=False)
    lines.append("changes by rule: " + (", ".join(f"{r}={n}" for r, n in by_rule.items()) or "none"))
    lines.append(f"duplicate keys resolved: {sum(int(s.get('duplicates_resolved', 0)) for s in summaries)}")
    unmapped = [f["unmapped"] for f in frames.values() if f.get("unmapped") is not None and len(f["unmapped"])]
    if unmapped:
        counts = pd.concat(unmapped).groupby(["column", "value"]).size().sort_values(ascending=False)
        lines.append(f"unmapped values: {int(counts.sum())} ({len(counts)} distinct)")
        lines += [f"  {c} = {v!r}: {n}" for (c, v), n in counts.head(25).items()]
    else:
        lines.append("unmapped values: none")
    truncated = truncated_values(frames)
    lines.append(f"truncated values (need recovering from source records): {len(truncated)}")
    lines += [f"  {r.grant} / {r.key} / {r.column}: {r.value!r}" for r in truncated.head(25).itertuples()]
    validation = [f["validation"] for f in frames.values() if f.get("validation") is not None and len(f["validation"])]
    if validation:
        counts = pd.concat(validation).groupby(["field", "validator"]).size().sort_values(ascending=False)
        lines.append(f"local validation failures: {int(counts.sum())}")
        lines += [f"  {f} / {v}: {n}" for (f, v), n in counts.head(25).items()]
    else:
        lines.append("local validation failures: none")
    return "\n".join(lines)


def truncated_values(frames: dict[str, dict]) -> pd.DataFrame:
    """``grant, key, column, value`` of every cell reported as truncated."""
    rows = []
    for grant, f in frames.items():
        changes = f.get("changes")
        if changes is None or changes.empty:
            continue
        for r in changes[changes["rule"] == "truncated"].to_dict("records"):
            rows.append({"grant": grant, "key": r["key"], "column": r["column"], "value": r["from"]})
    return pd.DataFrame(rows, columns=["grant", "key", "column", "value"])


WORKLIST_COLUMNS = ["grant", "key", "field", "validator", "message", "current_value"]


def validation_worklist(frames: dict[str, dict], key: str) -> pd.DataFrame:
    """Every local validation failure of the run with the cell's current value, sorted by field."""
    rows = []
    for grant, f in frames.items():
        validation, converted = f.get("validation"), f.get("converted")
        if validation is None or validation.empty:
            continue
        cells = converted.set_index(key).to_dict("index") if converted is not None else {}
        for r in validation.itertuples():
            current = cells.get(r.key, {}).get(r.field, "")
            rows.append({"grant": grant, "key": r.key, "field": r.field, "validator": r.validator,
                         "message": r.message, "current_value": current})
    out = pd.DataFrame(rows, columns=WORKLIST_COLUMNS)
    return out.sort_values(["field", "grant", "key"], kind="stable").reset_index(drop=True)


def main(argv=None, syn=None):
    args = get_args(argv)
    syn = syn or utils.syn_login()
    component = args.component
    write_log = recordsets.SynapseTableWriteLog(syn, args.write_log_table) if args.write_log_table else None
    list_cols = recordsets.list_columns(component)
    set_like = recordsets.set_like_columns(component)
    overrides = value_maps.load_overrides(args.overrides) if args.overrides else None
    normalizer = value_maps.Normalizer.for_component(component, overrides)
    out_dir = Path(args.out_dir)
    run_dir = out_dir / args.run_id
    state_path = out_dir / "state.json"
    state = load_state(state_path)
    mode = "execute" if args.execute else "dryrun"

    ref_cache = []

    def reference():
        """The converted merged manifest, read once per run (None when unusable)."""
        if not ref_cache:
            try:
                ref_cache.append(reference_frame(read_reference(syn, component), component, normalizer))
            except Exception:
                logger.exception("Could not read the merged manifest for %s", component)
                ref_cache.append(None)
        return ref_cache[0]

    tables, skipped = select_grant_tables(syn, args)
    print(f"Run {args.run_id}: {'EXECUTE' if args.execute else 'dry run'}, {component}, {len(tables)} grant tables")
    for note in skipped:
        print(f"skipped {note}")

    summaries, frames, legacy_keys, source_keys, projects = [], {}, {}, {}, {}
    seen_projects: set[str] = set()
    for entry in tables:
        grant = entry["grant"]
        projects[grant] = entry["project"]
        report_dir = run_dir / safe_name(grant)
        previous = state["grants"].get(grant, {})
        if (
            grant not in args.redo
            and previous.get("mode") == mode
            and previous.get("status") in DONE[mode]
            and Path(previous.get("converted", "")).exists()
        ):
            summary = dict(previous["summary"])
            summary["message"] = f"already {previous['status']} in run {previous['run_id']}; skipped"
            converted = recordsets._load_csv(previous["converted"], list_cols)
            report = Path(previous["converted"]).parent
            frames[grant] = _frames_from(report)
            legacy_keys[grant] = [str(k).strip() for k in converted[KEYS[component]]]
            source_keys[grant] = previous.get("source_keys", legacy_keys[grant])
            summaries.append(summary)
            print(summary_line(summary))
            continue
        try:
            if entry["project"] in seen_projects:
                raise ConversionError(f"project {entry['project']} has more than one {component} table; one RecordSet per grant")
            seen_projects.add(entry["project"])
            summary, converted, s_keys = process_grant(
                syn, entry, args, normalizer, write_log, report_dir, list_cols, set_like, reference
            )
        except Exception as err:  # one grant's failure must not stop the others
            logger.exception("Grant %s failed", grant)
            summary, converted, s_keys = (
                {"grant": grant, "table": entry["table"], "status": "failed", "message": repr(err)}, None, [],
            )
        report_dir.mkdir(parents=True, exist_ok=True)
        (report_dir / "summary.txt").write_text(summary_line(summary) + "\n")
        print(summary_line(summary))
        summaries.append(summary)
        if converted is not None:
            frames[grant] = _frames_from(report_dir)
            legacy_keys[grant] = [str(k).strip() for k in converted[KEYS[component]]]
            source_keys[grant] = s_keys
        state["grants"][grant] = {
            "mode": mode, "status": summary["status"], "run_id": args.run_id, "summary": summary,
            "converted": str(report_dir / "converted.csv") if converted is not None else "",
            "source_keys": s_keys if converted is not None else [],
        }
        save_state(state_path, state)

    failed_grants = [s["grant"] for s in summaries if s["status"] in NEEDS_ATTENTION]
    ok_grants = [s["grant"] for s in summaries if s["grant"] in legacy_keys and s["status"] not in NEEDS_ATTENTION]
    compared = [g for g in ok_grants if not args.execute or
                next(s for s in summaries if s["grant"] == g)["status"] in DONE["execute"]]
    full_run = not args.grants and args.limit is None and not skipped and not failed_grants
    if compared:
        compare_ok, text = comparison(
            syn, args,
            {g: legacy_keys[g] for g in compared},
            {g: source_keys[g] for g in compared},
            projects, compared, full_run,
        )
    else:
        compare_ok, text = True, "no grants to compare"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "comparison.txt").write_text(text + "\n")
    pd.DataFrame(summaries).to_csv(run_dir / "summary.csv", index=False)
    report = run_report(summaries, frames)
    (run_dir / "run_summary.txt").write_text(report + "\n")
    validation_worklist(frames, KEYS[component]).to_csv(run_dir / "validation_worklist.csv", index=False)
    truncated_values(frames).to_csv(run_dir / "truncated.csv", index=False)

    unmapped_total = sum(int(s.get("unmapped", 0)) for s in summaries)
    invalid_total = sum(int(s.get("validation_failures", 0)) + int(s.get("invalid_rows", 0)) for s in summaries)
    print(text)
    print(report)
    print(f"{len(summaries) - len(failed_grants)} of {len(summaries)} grants ok; {len(failed_grants)} need attention"
          + (f" ({failed_grants})" if failed_grants else ""))
    if failed_grants or not compare_ok:
        return 1
    if unmapped_total or invalid_total:
        print(f"{unmapped_total} unmapped value(s), {invalid_total} validation failure(s): exit 2")
        return 2
    return 0


def _frames_from(report_dir: Path) -> dict:
    """The changes, unmapped and validation reports a grant wrote (for the run report)."""
    out = {}
    for name in ("changes", "unmapped", "validation", "converted"):
        path = report_dir / f"{name}.csv"
        out[name] = pd.read_csv(path, dtype=str, keep_default_na=False) if path.exists() else None
    return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    sys.exit(main())
