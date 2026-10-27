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
import hashlib
import json
import logging
import os
import re
import sys
import urllib.error
from collections import Counter
from datetime import datetime
from functools import lru_cache
from pathlib import Path

import jsonschema
import pandas as pd
from synapseclient import operations
from synapseclient.extensions.curator import create_record_based_metadata_task
from synapseclient.models import Folder, MaterializedView, Table

# removed when the mc2dcc package (CLI consolidation plan) lands
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "portal_tables"))
import crosswalk
import merge_tables
import model_contract
import pubmed
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
        "--duplicate-choices",
        help="CSV (key, keep_legacy_id): the owner's pick among duplicate rows of a key; it decides before any rule",
    )
    parser.add_argument(
        "--include-unlisted", action=argparse.BooleanOptionalAction, default=True,
        help="also migrate projects that hold a grant-level table but are not in the Grants table, "
        "labelled by project name (default: on)",
    )
    parser.add_argument(
        "--recover-truncated", action=argparse.BooleanOptionalAction, default=None,
        help="recover PublicationAuthors cells cut off with '(Read more on Pubmed)' from PubMed by PubmedId "
        "(read-only NCBI calls; default: on for PublicationView)",
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
    if args.recover_truncated is None:
        args.recover_truncated = args.component == "PublicationView"
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

    Every table of the component is scanned; ``--limit`` keeps the first N projects.
    A project with several tables appears once per table (``main`` fails that grant).

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
    tables, skipped = [], []
    for table_id in list_grant_tables(syn, args.component)["id"]:  # every table: a project may hold more than one
        project, name = project_of(syn, table_id)
        unlisted = project not in grants
        if unlisted and not args.include_unlisted:
            skipped.append(f"{table_id}: project {project} is not in the Grants table")
            continue
        if wanted is not None and project not in wanted:
            continue
        label = UNLISTED_LABELS.get(project) or name or project if unlisted else str(grants[project])
        tables.append({"grant": label, "project": project, "table": table_id, "unlisted": unlisted})
    if args.limit is not None:  # --limit counts grants (projects), after the full scan
        keep = list(dict.fromkeys(t["project"] for t in tables))[:args.limit]
        tables = [t for t in tables if t["project"] in keep]
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


def read_portal(syn, component: str) -> pd.DataFrame:
    """The live portal table of the component (read-only)."""
    table_id = utils.CONFIG[REFERENCE_RESOURCE[component]]["portal_table"]
    return Table(id=table_id).query(
        query=f"SELECT * FROM {table_id}", include_row_id_and_row_version=False, synapse_client=syn
    )


def _flat(value) -> str:
    """A cell as comparable text: lists ", "-joined, blank for missing."""
    if isinstance(value, (list, tuple)):
        return ", ".join(str(v).strip() for v in value)
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return ""
    text = str(value).strip()
    if text.startswith("[") and text.endswith("]"):
        parsed = recordsets.parse_cell(text)
        if isinstance(parsed, list):
            return ", ".join(str(v).strip() for v in parsed)
    return text


def portal_scorer(portal_df, component: str, columns):
    """``(score(key, row), compared)``: how many mapped portal cells a candidate row matches.

    Columns come from ``crosswalk.CROSSWALK[component]`` (derived columns are
    skipped); the portal row is the one whose key column equals the row's key
    (``DatasetViewId`` -> ``datasetId``; publications use ``PubmedId`` ->
    ``pubMedId``). Returns None when the portal table has no usable key.
    """
    key = KEYS[component]
    mapping = {src: col for src, col, _ in crosswalk.CROSSWALK[component] if not src.startswith(crosswalk.DERIVED)}
    key_source = key if key in mapping else ("PubmedId" if "PubmedId" in mapping else None)
    if key_source is None or mapping[key_source] not in portal_df.columns:
        return None
    by_key: dict[str, dict] = {}
    for row in portal_df.to_dict("records"):
        by_key.setdefault(_flat(row[mapping[key_source]]), row)
    compared = [c for c in columns if c in mapping and c != key and mapping[c] in portal_df.columns]
    list_cols = recordsets.list_columns(component)
    set_like = recordsets.set_like_columns(component)

    def same(column: str, ours, theirs) -> bool:
        if column in list_cols:  # list cells: items, order-insensitive where the items are an enum
            return recordsets._cells_equal(value_maps.split_items(_flat(ours)), value_maps.split_items(_flat(theirs)),
                                           column in set_like)
        return recordsets._canon(_flat(ours)) == recordsets._canon(_flat(theirs))

    def score(key_value: str, row: dict) -> tuple[int, int] | None:
        """``row`` is the raw (pre-normalization) candidate, compared with the raw portal cells."""
        portal_row = by_key.get(key_value if key_source == key else (_flat(row.get(key_source, "")) or key_value))
        if portal_row is None:
            return None
        return sum(same(c, row[c], portal_row[mapping[c]]) for c in compared), len(compared)

    return score


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


def _id_value(value) -> tuple[int, int, str]:
    """Sort key of a legacy ``Id``: integers numerically, anything else (a UUID) as text, below integers."""
    text = str(value).strip()
    try:
        return 1, int(text), ""
    except ValueError:
        return 0, 0, text


def load_duplicate_choices(path) -> dict[tuple[str, str], str]:
    """``{(grant, key): legacy Id to keep}`` from a CSV with columns ``key, keep_legacy_id`` and an
    optional ``grant`` (blank: any grant)."""
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = {"key", "keep_legacy_id"} - set(df.columns)
    if missing:
        raise ValueError(f"{path} lacks column(s) {sorted(missing)}")
    grants = df["grant"] if "grant" in df.columns else [""] * len(df)
    return {(g.strip(), k.strip()): i.strip() for g, k, i in zip(grants, df["key"], df["keep_legacy_id"]) if k.strip()}


def choices_for(choices: dict | None, grant: str) -> dict[str, str]:
    """The choices that apply to ``grant``: ``{key: legacy Id}``; a choice naming another grant is ignored
    and one naming this grant beats a general one."""
    out: dict[str, str] = {}
    for (g, key), legacy_id in (choices or {}).items():
        if g == "":
            out.setdefault(key, legacy_id)
    for (g, key), legacy_id in (choices or {}).items():
        if g == grant:
            out[key] = legacy_id
    return out


def resolve_duplicates(df: pd.DataFrame, legacy_id, component: str, normalizer, reference, portal=lambda: None,
                       choices=None):
    """Keep one row of each duplicated key: ``(frame, resolutions)``.

    The kept row is the one equal to the merged manifest's row of that key on
    the columns both have (after the same conversion). Failing that, the row
    matching more cells of the live portal table's row (``portal`` returns that
    table or None; read once, only when a duplicate exists), then the row with
    the fewest blank cells; on a final tie the higher legacy ``Id`` (arbitrary).
    ``choices`` (``{key: legacy Id}``) decides a key before all of that, reason
    ``owner choice``; the Id must be one of the key's rows (an exact match, or a
    prefix naming one row), else :class:`ConversionError`. ``reference`` is a callable
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
    portal_df = portal()
    score = portal_scorer(portal_df, component, list(df.columns)) if portal_df is not None else None

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
        if choices and k in choices:
            want = choices[k]
            exact = [i for i in rows if str(ids[i]).strip() == want]
            picked = exact or [i for i in rows if want and str(ids[i]).strip().startswith(want)]
            if not picked:
                raise ConversionError(
                    f"duplicate choice for key {k}: legacy Id {want!r} is not among the duplicate rows "
                    f"{[str(ids[i]).strip() for i in rows]}")
            if len(picked) > 1 and not exact:
                raise ConversionError(f"duplicate choice for key {k}: legacy Id prefix {want!r} names several rows")
            kept = picked[0]
            for i in rows:
                if i != kept:
                    drop.add(i)
                    resolutions.append({"key": k, "kept_row": label(kept), "dropped_row": label(i), "reason": "owner choice"})
            continue
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
            kept = None
            scores = {i: score(keys.iloc[i], df.iloc[i].to_dict()) for i in pool} if score else {}
            if scores and all(v is not None for v in scores.values()):
                top_score = max(v[0] for v in scores.values())
                top = [i for i in pool if scores[i][0] == top_score]
                if len(top) == 1:
                    kept = top[0]
                    others = ", ".join(str(scores[i][0]) for i in pool if i != kept)
                    reason = f"{why}; matches more portal cells ({scores[kept][0]} vs {others} of {scores[kept][1]})"
                else:
                    pool, why = top, f"{why}; tie on portal cells ({top_score} of {scores[top[0]][1]})"
            if kept is not None:
                for i in rows:
                    if i != kept:
                        drop.add(i)
                        resolutions.append({"key": k, "kept_row": label(kept), "dropped_row": label(i), "reason": reason})
                continue
            blanks = {i: sum(_blank(norm.iloc[i][c]) for c in norm.columns) for i in pool}
            best = min(blanks.values())
            top = [i for i in pool if blanks[i] == best]
            if len(top) == 1:
                kept, reason = top[0], f"{why}; most complete row ({best} blank cells)"
            else:
                kept = max(top, key=lambda i: _id_value(ids[i]))
                reason = f"{why}; tie on completeness ({best} blank cells); arbitrary tie-break: higher legacy Id"
        for i in rows:
            if i != kept:
                drop.add(i)
                resolutions.append({"key": k, "kept_row": label(kept), "dropped_row": label(i), "reason": reason})
    keep = [i for i in range(len(df)) if i not in drop]
    return df.iloc[keep].reset_index(drop=True), pd.DataFrame(resolutions, columns=RESOLUTION_COLUMNS)


RECOVERED_COLUMNS = ["key", "old", "new", "pubmed_id", "source", "reason"]


def _same_name(a: str, b: str) -> bool:
    return value_maps.fold(a) == value_maps.fold(b)


def _cut_text(item: str) -> str:
    """A truncated author item without the ``(Read more on Pubmed)`` suffix, trailing ``...`` / ``…`` and whitespace."""
    return re.sub(r"(?:\s|\.\.\.|…)+$", "", str(item).replace(value_maps.TRUNCATED_SUFFIX, ""))


def _cut_matches(item: str, author: str, cut: int) -> bool:
    """Whether the cut-off item is a prefix of PubMed's author at its position (empty text: only ``cut > 0`` accepts)."""
    text = value_maps.fold(_cut_text(item))
    return value_maps.fold(author).startswith(text) if text else cut > 0


def recover_truncated_authors(converted: pd.DataFrame, changes: pd.DataFrame, recover, key: str):
    """Replace truncated ``PublicationAuthors`` lists with PubMed's: ``(converted, changes, recovered)``.

    ``recover(pmids)`` returns ``(found, failed)``: ``{pmid: [author, ...]}`` for
    the PMIDs PubMed knows and the set of PMIDs whose request failed
    (``pubmed.fetch_authors``). The PMID is the row's ``PubmedId`` (the key when
    blank). A list is replaced only if the cell's complete leading items (all but
    the cut-off one) equal the first PubMed names (case and whitespace
    ignored) and the cut-off item's text is a prefix of PubMed's name at that position; then the change is ``recovered_pubmed`` instead of ``truncated``.
    Otherwise the cell stays truncated: ``recovery_mismatch`` when PubMed's
    list disagrees with the leading items; ``truncated`` when PubMed was
    unavailable or did not return the PMID. ``recovered`` lists every attempt
    (``key, old, new, pubmed_id, source, reason``; ``reason`` is blank for a recovered cell).
    """
    column = "PublicationAuthors"
    empty = pd.DataFrame(columns=RECOVERED_COLUMNS)
    if recover is None or column not in converted.columns:
        return converted, changes, empty
    suffix = value_maps.TRUNCATED_SUFFIX
    rows = [i for i, cell in enumerate(converted[column])
            if isinstance(cell, list) and any(suffix in str(item) for item in cell)]
    if not rows:
        return converted, changes, empty
    pmid_of = {i: (str(converted["PubmedId"].iloc[i]).strip() if "PubmedId" in converted.columns else "")
               or str(converted[key].iloc[i]).strip() for i in rows}
    found, failed = recover(sorted(set(pmid_of.values())))
    cells, records, done, mismatched = list(converted[column]), [], set(), set()
    for i in rows:
        pmid, k, old = pmid_of[i], converted[key].iloc[i], cells[i]
        base = {"key": k, "old": ", ".join(old), "pubmed_id": pmid, "source": pubmed.SOURCE}
        authors = found.get(pmid)
        if not authors:
            records.append({**base, "new": "",
                            "reason": "PubMed unavailable" if pmid in failed else "PMID not returned by PubMed"})
            continue
        cut = next(n for n, item in enumerate(old) if suffix in str(item))
        leading = old[:cut]
        if (len(authors) <= cut or not all(_same_name(a, b) for a, b in zip(leading, authors))
                or not _cut_matches(old[cut], authors[cut], cut)):
            records.append({**base, "new": ", ".join(authors),
                            "reason": f"recovery_mismatch: the {len(leading)} complete leading name(s) and the cut-off item "
                                      "do not match PubMed's"})
            mismatched.add(k)
            continue
        records.append({**base, "new": ", ".join(authors), "reason": ""})
        cells[i] = list(authors)
        done.add(k)
    recovered = pd.DataFrame(records, columns=RECOVERED_COLUMNS)
    if not done and not mismatched:
        return converted, changes, recovered
    converted = converted.copy()
    converted[column] = cells
    is_truncated = (changes["column"] == column) & (changes["rule"] == "truncated")
    gone = changes["key"].isin(done | mismatched) & is_truncated
    mismatch_rows = changes[gone & changes["key"].isin(mismatched)].assign(rule="recovery_mismatch")
    new = pd.DataFrame(
        [{"key": r["key"], "column": column, "from": r["old"], "to": r["new"], "rule": "recovered_pubmed"}
         for r in records if not r["reason"]],
        columns=value_maps.CHANGE_COLUMNS,
    )
    return converted, pd.concat([changes[~gone], mismatch_rows, new], ignore_index=True), recovered


def convert_table(source: pd.DataFrame, component: str, normalizer: "value_maps.Normalizer", reference=lambda: None,
                  portal=lambda: None, choices=None):
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
    df, resolutions = resolve_duplicates(df[[c for c in df.columns if c in schema]], legacy_id, component, normalizer, reference, portal, choices)
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


def verify_version(syn, rs_id, version, args, converted, key, stored, list_cols, set_like, report_dir, summary):
    """Read ``version`` back, compare it with ``converted`` and collect Synapse's validation results.

    Returns the list of differences (empty when it matches); writes ``synapse_validation.csv``.
    """
    written = recordsets.read_recordset(syn, rs_id, version=version, cache_dir=args.cache_dir, list_cols=list_cols)
    problems = recordsets.differences(converted, _only(written, stored), key, set_like)
    validation = recordsets.get_validation(
        syn, rs_id, version, report_dir, [recordsets._key_of(r, [key]) for _, r in written.iterrows()]
    )
    if validation is not None:
        validation.to_csv(report_dir / "synapse_validation.csv", index=False)
        summary["invalid_rows"] = int((~validation["is_valid"].astype(str).str.lower().eq("true")).sum())
    return problems


def migrate_recordset(syn, entry, args, write_log, converted, converted_path, report_dir, summary, list_cols, set_like):
    """``--execute`` for one grant; updates ``summary`` (status, message, ...).

    A RecordSet is ``already_migrated`` only when its newest logged load has a
    ``verified`` row; a logged load without one is verified now (read back and
    compared with ``converted``), then marked ``verified``. A RecordSet this
    script created whose load never started (a ``created`` row, no ``written``,
    still at the creation version) is loaded now.
    """
    component, key = args.component, KEYS[args.component]
    stored = recordsets.schema_columns(component) | {key}
    folder_id = find_or_create_folder(syn, entry["project"], FOLDER_MAP[component])
    rs_id = recordsets.find_recordset(syn, folder_id, component)
    principal = recordsets.principal_id(syn)
    recovered = {}
    note = None
    if rs_id is not None:
        summary["recordset"] = rs_id
        locker = write_log.locked_by(rs_id, args.run_id, since=PROCESS_START)
        if locker:
            summary.update(status="locked", message=(
                f"run {locker} has an unfinished write (pending row under {recordsets.LOCK_HOURS} h old); skipped"))
            return
        latest, upsert = recordsets.recordset_info(syn, rs_id)
        # A run that died after exporting leaves a pending row; claim its versions.
        recovered = recordsets.recover_pending(
            syn, rs_id, write_log, latest, record=True, skip_younger_than=recordsets.LOCK_HOURS
        )
        logged = write_log.pipeline_map(rs_id)
        if logged:
            version = max(logged)
            base = logged[version]
            if version in write_log.verified_versions(rs_id):
                summary.update(status="already_migrated", message=f"{rs_id} already migrated (v{version} verified)")
                return
            summary["version"] = version
            if recovered:
                summary["recovered"] = sorted(recovered)
            note = (f"verified v{version}, recovered from an earlier run" if recovered
                    else f"verified v{version}, logged by an earlier run but never verified")
        else:
            created_at = write_log.created_version(rs_id)
            if created_at is None:
                summary.update(status="failed", message=f"{rs_id} exists but not migrated; investigate")
                return
            if latest != created_at:
                summary.update(status="failed", message=(
                    f"{rs_id} exists but not migrated; investigate (created at v{created_at}, now v{latest}: "
                    "a version nobody logged was written)"))
                return
            if upsert != [key]:  # data must not load into a RecordSet with the wrong key
                summary.update(status="failed", message=f"{rs_id} has upsert key {upsert}, expected {[key]}")
                return
            version = None  # the load never started: load now, from the creation version
            base = latest
            note = f"loaded the RecordSet left empty at v{created_at} by an earlier run"
    else:
        rs_id = create_recordset(syn, args, entry["project"], folder_id, entry["grant"], entry["table"])
        summary["recordset"] = rs_id
        latest, upsert = recordsets.recordset_info(syn, rs_id)
        write_log.created(rs_id, latest, args.run_id, principal)
        if upsert != [key]:
            summary.update(status="failed", message=f"{rs_id} was created with upsert key {upsert}, expected {[key]}")
            return
        version, base = None, latest

    if version is None:
        try:
            created = recordsets.write_logged(
                syn, rs_id, converted_path, write_log, args.run_id, principal, len(converted), latest,
                retry_note="rerun the migration (the RecordSet stays empty)",
            )
        except recordsets.WriteRefused as refused:
            summary.update(status=refused.status, message=refused.message)
            return
        if not created:
            summary.update(status="failed", message=f"{rs_id}: the write exported no version; nothing to verify")
            return
        version = created[-1]
        summary["version"] = version
        note = (note + "; " if note else "") + f"wrote v{version}"

    problems = verify_version(syn, rs_id, version, args, converted, key, stored, list_cols, set_like, report_dir, summary)
    notes = [note]
    newest, _ = recordsets.recordset_info(syn, rs_id)
    if newest > version:
        notes.append(f"v{newest} exists after the load; contributor edits are handled by the next sync's replay")
    if problems:
        (report_dir / "verification.txt").write_text("\n".join(problems) + "\n")
        summary.update(status="verification_mismatch", message=(
            f"v{version} differs from converted.csv ({len(problems)} differences)"))
        return
    write_log.verified(rs_id, version, base, args.run_id, principal)
    summary.update(status="migrated", message="; ".join(notes))


def process_grant(syn, entry, args, normalizer, write_log, report_dir: Path, list_cols, set_like, reference=lambda: None, portal=lambda: None,
                  recover=None, choices=None):
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
        converted, changes, unmapped, dropped, resolutions = convert_table(source, component, normalizer, reference, portal, choices_for(choices, entry["grant"]))
    except ConversionError as err:
        summary.update(status="failed", message=str(err))
        return summary, None, source_keys
    converted, changes, recovered = recover_truncated_authors(converted, changes, recover, key)
    for k in resolutions["key"]:  # the source side of the comparison loses one row per resolved duplicate
        source_keys.remove(k)
    if dropped:
        logger.info("%s: not schema columns, dropped: %s", entry["grant"], dropped)
    validation = validate_frame(converted, component, args.model_ref)
    converted_path = report_dir / "converted.csv"
    _write(converted, converted_path)
    _write(changes, report_dir / "changes.csv")
    _write(unmapped, report_dir / "unmapped.csv")
    _write(validation, report_dir / "validation.csv")
    _write(validation_counts(validation), report_dir / "validation_counts.csv")
    if len(resolutions):
        _write(resolutions, report_dir / "duplicates_resolved.csv")
    if len(recovered):
        _write(recovered, report_dir / "recovered.csv")
    authors_recovered = int((recovered["reason"] == "").sum())
    summary.update(
        rows=len(converted), changes=len(changes), unmapped=len(unmapped), validation_failures=len(validation),
        dropped=dropped, duplicates_resolved=len(resolutions), authors_recovered=authors_recovered, _resolved_keys=resolutions["key"].tolist(),
        truncated=int(changes["rule"].isin(["truncated", "recovery_mismatch"]).sum()),
    )
    if converted.empty:  # nothing to load: no RecordSet is created and nothing is written
        summary.update(status="empty_source", message="no rows after conversion; no RecordSet created, nothing written")
        return summary, converted, source_keys
    if not args.execute:
        summary.update(status="dryrun", message="dry run: nothing written")
        return summary, converted, source_keys
    migrate_recordset(syn, entry, args, write_log, converted, converted_path, report_dir, summary, list_cols, set_like)
    return summary, converted, source_keys


def summary_line(s: dict) -> str:
    parts = [f"{s['grant']}: {s['status']}"]
    for field in ("table", "recordset", "version", "source_rows", "rows", "changes", "unmapped",
                  "validation_failures", "duplicates_resolved", "authors_recovered", "truncated", "invalid_rows", "recovered"):
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
    expected, actual = Counter(expected_keys), Counter(actual_keys)
    exp, act = set(expected), set(actual)
    lines = [
        f"{label}: {expected_name} rows={len(expected_keys)} unique keys={len(exp)}; "
        f"compared rows={len(actual_keys)} unique keys={len(act)}"
    ]
    ok = expected == actual  # as multisets: same keys with the same multiplicity
    for name, keys in (("missing from the compared side", sorted(expected - actual)),
                       ("extra in the compared side", sorted(actual - expected))):
        if keys:
            lines.append(f"  {len(keys)} key(s) {name}: {keys[:20]}{' ...' if len(keys) > 20 else ''}")
    lines.append(f"  {'EQUAL' if ok else 'DIFFERENT'}")
    return ok, lines


def view_scope(syn, view_id: str) -> set[str] | None:
    """Table IDs a MaterializedView's defining SQL reads (None when it cannot be read)."""
    try:
        return set(re.findall(r"syn\d+", MaterializedView(id=view_id).get(synapse_client=syn).defining_sql))
    except Exception:
        logger.warning("Could not read the defining SQL of %s", view_id)
        return None


def comparison(syn, args, legacy_keys: dict[str, list[str]], source_keys: dict[str, list[str]], projects: dict[str, str],
               migrated: list[str], full_run: bool, resolved: dict[str, list[str]] | None = None,
               tables: dict[str, str] | None = None) -> tuple[bool, str]:
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
            scope = view_scope(syn, union_id) if tables else None
            if scope is not None:
                # a view built before a table existed cannot hold its rows: compare what it scopes
                outside = [g for g in grants if tables.get(g) not in scope]
                if outside:
                    lines.append(f"grants whose table is not in the view's scope (the view predates them): {outside}")
                    grants = [g for g in grants if g not in outside]
                    legacy = [k for g in grants for k in legacy_keys[g]]
            view = Table(id=union_id).query(
                query=f"SELECT {key_display} FROM {union_id}",
                include_row_id_and_row_version=False, synapse_client=syn,
            )
            actual = [str(k).strip() for k in view[key_display]]
            for g in grants:  # the view still holds both rows of a resolved duplicate
                for k in (resolved or {}).get(g, []):
                    if k in actual:
                        actual.remove(k)
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
    recovered = [f["recovered"] for f in frames.values() if f.get("recovered") is not None and len(f["recovered"])]
    attempts = pd.concat(recovered) if recovered else pd.DataFrame(columns=RECOVERED_COLUMNS)
    lines.append(f"authors recovered from PubMed: {int((attempts['reason'] == '').sum())}")
    reasons = attempts[attempts["reason"] != ""]["reason"].map(lambda r: r.split(":")[0]).value_counts()
    lines.append("authors not recovered: " + (", ".join(f"{r}={n}" for r, n in reasons.items()) or "none"))
    empty = [s["grant"] for s in summaries if s["status"] == "empty_source"]
    lines.append("empty source tables (no RecordSet): " + (", ".join(map(str, empty)) or "none"))
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
    """``grant, key, column, value, rule`` of every cell that is still truncated (``truncated``, or
    ``recovery_mismatch`` when PubMed's authors did not agree with the cell)."""
    rows = []
    for grant, f in frames.items():
        changes = f.get("changes")
        if changes is None or changes.empty:
            continue
        for r in changes[changes["rule"].isin(["truncated", "recovery_mismatch"])].to_dict("records"):
            rows.append({"grant": grant, "key": r["key"], "column": r["column"], "value": r["from"], "rule": r["rule"]})
    return pd.DataFrame(rows, columns=["grant", "key", "column", "value", "rule"])


def portal_author_fixes(frames: dict[str, dict]) -> pd.DataFrame:
    """``pubMedId, authors, source`` of every recovered author list (one row per PMID), for the portal table and
    the merged manifest, which probably hold the same truncation."""
    parts = [f["recovered"][f["recovered"]["reason"] == ""][["pubmed_id", "new", "source"]] for f in frames.values()
             if f.get("recovered") is not None and len(f["recovered"])]
    if not parts:
        return pd.DataFrame(columns=["pubMedId", "authors", "source"])
    out = pd.concat(parts).drop_duplicates("pubmed_id").sort_values("pubmed_id")
    return out.rename(columns={"pubmed_id": "pubMedId", "new": "authors"}).reset_index(drop=True)


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


def options_hash(args) -> str:
    """A hash of the options that change what a grant converts to (a done grant is skipped only if it matches)."""
    def content(path):
        return Path(path).read_bytes().hex() if path else ""

    effective = {
        "component": args.component, "overrides": content(args.overrides),
        "duplicate_choices": content(args.duplicate_choices), "schema_version": args.schema_version,
        "include_unlisted": bool(args.include_unlisted), "recover_truncated": bool(args.recover_truncated),
    }
    return hashlib.sha256(json.dumps(effective, sort_keys=True).encode()).hexdigest()


def resolve_model_ref(args) -> str | None:
    """The data-models tag of ``--schema-version``, or None to use ``MODEL_REF``.

    The enum maps and the local validation both read the model files at this
    tag. A tag that is not available is an error with ``--execute`` (the RecordSet is
    bound to that version) and a warning in a dry run.
    """
    ref = f"v{args.schema_version}"
    try:
        for path in (f"json_schemas/{args.component}.json", "all_valid_values.csv"):
            model_contract.fetch(path, ref=ref)
    except (urllib.error.URLError, FileNotFoundError) as err:
        if args.execute:
            raise SystemExit(f"data-models tag {ref} is not available ({err}); --execute binds the RecordSets "
                             f"to {args.schema_version}, so it cannot run")
        logger.warning("data-models tag %s is not available (%s); the dry run uses %s", ref, err, model_contract.MODEL_REF)
        return None
    return ref


def main(argv=None, syn=None):
    args = get_args(argv)
    syn = syn or utils.syn_login()
    component = args.component
    write_log = recordsets.SynapseTableWriteLog(syn, args.write_log_table) if args.write_log_table else None
    list_cols = recordsets.list_columns(component)
    set_like = recordsets.set_like_columns(component)
    overrides = value_maps.load_overrides(args.overrides) if args.overrides else None
    args.model_ref = resolve_model_ref(args)
    normalizer = value_maps.Normalizer.for_component(component, overrides, ref=args.model_ref)
    out_dir = Path(args.out_dir)
    run_dir = out_dir / args.run_id
    state_path = out_dir / "state.json"
    state = load_state(state_path)
    mode = "execute" if args.execute else "dryrun"
    options = options_hash(args)

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

    choices = load_duplicate_choices(args.duplicate_choices) if args.duplicate_choices else None
    recover = None
    if args.recover_truncated:
        def recover(pmids):
            failed: set = set()
            return pubmed.fetch_authors(pmids, cache_dir=args.cache_dir, failed=failed), failed

    portal_cache = []

    def portal():
        """The live portal table, read once per run (None when unreadable)."""
        if not portal_cache:
            try:
                portal_cache.append(read_portal(syn, component))
            except Exception:
                logger.exception("Could not read the portal table for %s", component)
                portal_cache.append(None)
        return portal_cache[0]

    tables, skipped = select_grant_tables(syn, args)
    print(f"Run {args.run_id}: {'EXECUTE' if args.execute else 'dry run'}, {component}, {len(tables)} grant tables")
    for note in skipped:
        print(f"skipped {note}")

    summaries, frames, legacy_keys, source_keys, projects = [], {}, {}, {}, {}
    by_project: dict[str, list[dict]] = {}
    for entry in tables:
        by_project.setdefault(entry["project"], []).append(entry)
    for entry in tables:
        grant = entry["grant"]
        projects[grant] = entry["project"]
        report_dir = run_dir / safe_name(grant)
        siblings = by_project[entry["project"]]
        if len(siblings) > 1:  # one RecordSet per grant: none of its tables is read or migrated
            if entry is not siblings[0]:
                continue
            ids = [t["table"] for t in siblings]
            summary = {"grant": grant, "table": ", ".join(ids), "status": "failed", "message":
                       f"project {entry['project']} has more than one {component} table ({', '.join(ids)}); "
                       "one RecordSet per grant, none migrated"}
            report_dir.mkdir(parents=True, exist_ok=True)
            (report_dir / "summary.txt").write_text(summary_line(summary) + "\n")
            print(summary_line(summary))
            summaries.append(summary)
            continue
        previous = state["grants"].get(grant, {})
        if (
            grant not in args.redo
            and previous.get("mode") == mode
            and previous.get("status") in DONE[mode]
            and previous.get("options") == options
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
            summary, converted, s_keys = process_grant(
                syn, entry, args, normalizer, write_log, report_dir, list_cols, set_like, reference, portal, recover, choices
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
            "mode": mode, "options": options, "status": summary["status"], "run_id": args.run_id, "summary": summary,
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
            {s_["grant"]: s_.get("_resolved_keys", []) for s_ in summaries},
            {s_["grant"]: s_.get("table") for s_ in summaries},
        )
    else:
        compare_ok, text = True, "no grants to compare"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "comparison.txt").write_text(text + "\n")
    pd.DataFrame([{k: v for k, v in s_.items() if not k.startswith("_")} for s_ in summaries]).to_csv(
        run_dir / "summary.csv", index=False
    )
    report = run_report(summaries, frames)
    (run_dir / "run_summary.txt").write_text(report + "\n")
    validation_worklist(frames, KEYS[component]).to_csv(run_dir / "validation_worklist.csv", index=False)
    truncated_values(frames).to_csv(run_dir / "truncated.csv", index=False)
    portal_author_fixes(frames).to_csv(run_dir / "portal_author_fixes.csv", index=False)
    normalizer.rejected.to_csv(run_dir / "rejected_synonyms.csv", index=False)

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
    for name in ("changes", "unmapped", "validation", "converted", "recovered"):
        path = report_dir / f"{name}.csv"
        out[name] = pd.read_csv(path, dtype=str, keep_default_na=False) if path.exists() else None
    return out


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    sys.exit(main())
