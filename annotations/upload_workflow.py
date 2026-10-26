"""
upload_workflow.py

Parameterized driver for Phase 2 of the orchestrator (orchestration/README.md);
replaces upload-workflow.sh. Chains, for one component:

  1. split    split_manifest_grants.py     (in-process main(argv))
  2. paths    gen-mp-csv.py                (in-process main(folder, csv, type); read-only Synapse query)
  3. reshape  processing-splits.py         (in-process main(argv), on copies of the split files)
  4. links    create_entity_links.py       (in-process main(argv); DatasetView / ToolView only;
                                            creates Synapse entities, so --execute only)
  5. upload   upload_recordsets.py         (in-process main(argv)) for --target recordset
              upload-manifests.py          (subprocess) for --target schematic-table

Each stage writes to <out-dir>/<NN>_<stage>/ and <out-dir>/run.json records the
finished stages, their outputs and timestamps. ``--from-stage`` resumes a run.
The default is a dry run: nothing is written to Synapse.

Before ``links`` the driver (a) excludes split files whose grant has no target
folder (every type, logged), (b) for DatasetView, reuses Dataset entities that
already exist in the central project for a row's GEO/SRA accession instead of
creating a second one (only an indexed Dataset for the same grant; see
``reuse_existing_datasets``), then (c) drops rows whose link column is empty
(logged), dropping any split file left empty. The dropped rows and excluded
files are listed in run.json (``dropped_rows``, ``unresolved_folder_files``).

Each stage starts with an empty directory (``--from-stage`` keeps the earlier
stages'). ``--from-stage`` refuses if the manifest (path or SHA-256), type, target
or --cv-file differ from run.json; a new run in an out-dir holding another
manifest's or type's run needs ``--force-new`` (wipes the stage folders).

Exit codes: 0 all stages finished and nothing was dropped; 1 a stage failed (or
the run was interrupted; run.json marks the stage failed); 2 all stages finished
but rows were dropped or files excluded, so the upload is incomplete by design.

author: orion.banks
"""

import argparse
import hashlib
import importlib.util
import json
import logging
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
# removed when the mc2dcc package (CLI consolidation plan) lands
sys.path.insert(0, str(HERE.parent / "portal_tables"))

logger = logging.getLogger("upload_workflow")

STAGES = ["split", "paths", "reshape", "links", "upload"]
COMPONENTS = ["PublicationView", "DatasetView", "ToolView", "EducationalResource"]

# component -> split manifest_type, gen-mp-csv data_type, split file suffix,
#              gen-mp-csv folder column
CONFIG = {
    "PublicationView": ("publication", "publications", "_publication.csv", "folderIdPublication"),
    "DatasetView": ("dataset", "datasets", "_dataset.csv", "folderIdDatasets"),
    "ToolView": ("tool", "tools", "_tool.csv", "folderIdTools"),
    "EducationalResource": ("resource", "education", "_education.csv", "folderIdEducation"),
}

# components that get link entities -> (link column, primary key column)
LINK_COLUMNS = {
    "DatasetView": ("Dataset Url", "DatasetView_id"),
    "ToolView": ("Tool Homepage", "ToolView_id"),
}
DATASET_NAME_COLUMN = "Dataset Alias"

# Where create_entity_links.py puts GEO/SRA Dataset entities (_CENTRAL_PROJECT_SYNID
# there: create_synapse_dataset(syn, accession, _CENTRAL_PROJECT_SYNID) creates a
# Dataset named after the accession directly under the central project, next to
# the "datasets" folder holding its "<accession>_links" folder) and the
# DatasetCollection it adds them to (_COLLECTION_SYNID there). A test keeps these
# copies equal to create_entity_links.py's.
CENTRAL_PROJECT_SYNID = "syn76986881"
COLLECTION_SYNID = "syn77025587"
DATASETS_FOLDER = "datasets"

_modules = {}


def _load(filename):
    """Import annotations/<filename> (hyphenated names are not importable)."""
    if filename not in _modules:
        name = "uw_" + re.sub(r"\W", "_", filename)
        spec = importlib.util.spec_from_file_location(name, HERE / filename)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        _modules[filename] = mod
    return _modules[filename]


def now():
    return datetime.now().isoformat(timespec="seconds")


# --- run.json -------------------------------------------------------------


class Run:
    def __init__(self, out_dir: Path, args):
        self.out_dir = out_dir
        self.path = out_dir / "run.json"
        self.args = args
        self.state = {"stages": {}}
        if self.path.exists():
            self.state = json.loads(self.path.read_text())

    def stage_path(self, stage):
        return self.out_dir / f"{STAGES.index(stage) + 1:02d}_{stage}"

    def stage_dir(self, stage, keep=()):
        """Fresh directory for a starting stage: anything a previous attempt left
        there (including reused/, final/ and report dirs) is removed, except the
        top-level names in `keep`."""
        d = self.stage_path(stage)
        if d.exists():
            for child in d.iterdir():
                if child.name in keep:
                    continue
                shutil.rmtree(child) if child.is_dir() else child.unlink()
        d.mkdir(parents=True, exist_ok=True)
        return d

    def output(self, stage, key):
        return self.state["stages"][stage]["outputs"][key]

    def save(self):
        self.state.update(
            manifest=str(self.args.manifest),
            manifest_sha256=self.args.manifest_sha256,
            type=self.args.type,
            target=self.args.target,
            cv_file=self.args.cv_file,
            execute=self.args.execute,
            updated=now(),
        )
        self.path.write_text(json.dumps(self.state, indent=2) + "\n")

    def record(self, stage, status, started, outputs=None, note=None):
        self.state["stages"][stage] = {
            "status": status,
            "started": started,
            "finished": now(),
            "outputs": outputs or {},
            **({"note": note} if note else {}),
        }
        self.save()


# --- helpers ---------------------------------------------------------------


def subprocess_env():
    """Environment for every subprocess: inherits, and passes SYNAPSE_AUTH_TOKEN
    explicitly when set (never logged). The running interpreter's bin/ goes first
    on PATH so ``schematic`` resolves when invoked by absolute venv path."""
    env = dict(os.environ)
    token = os.environ.get("SYNAPSE_AUTH_TOKEN")
    if token:
        env["SYNAPSE_AUTH_TOKEN"] = token
    env["PATH"] = os.path.dirname(sys.executable) + os.pathsep + env.get("PATH", "")
    return env


def read_csv(path):
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def write_paths_csv(paths: pd.DataFrame, path: Path):
    paths.to_csv(path, index=False)
    return str(path)


def retarget(paths: pd.DataFrame, new_dir: Path, copy=True):
    """Copy each split file into new_dir; return the paths frame pointing at the copies."""
    paths = paths.copy()
    new = []
    for p in paths["File Paths"]:
        dest = new_dir / Path(p).name
        if copy:
            dest.write_bytes(Path(p).read_bytes())
        new.append(str(dest))
    paths["File Paths"] = new
    return paths


def parse_accession(name, link):
    """GEO/SRP accession for a manifest row, from geo_synapse's own parser (the one
    create_entity_links uses, so the two cannot disagree). Raises if geo_synapse is
    not importable: there is deliberately no fallback parser. An empty link falls
    back to the alias when it is itself a bare GSE/SRP/ERP/DRP accession (by
    building the canonical URL, so geo_synapse still makes the decision)."""
    try:
        from geo_synapse.mc2_table import parse_accession as pa
    except ImportError as err:
        raise RuntimeError(
            "geo_synapse is not importable; it is required to recognise GEO/SRA accessions "
            "(https://github.com/sagebio-ada/geo_dataset_creation)"
        ) from err
    if not str(link).strip():
        alias = str(name).strip()
        if re.fullmatch(r"GSE\d+", alias, re.IGNORECASE):
            link = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={alias}"
        elif re.fullmatch(r"(SRP|ERP|DRP)\d+", alias, re.IGNORECASE):
            link = f"https://www.ncbi.nlm.nih.gov/sra?term={alias}"
    return pa(f"[{name}]({link})")


def _grant_annotation(syn, entity_id):
    raw = syn.restGET(f"/entity/{entity_id}/annotations2").get("annotations", {})
    return [v.strip() for val in raw.get("grantNumber", {}).get("value", []) for v in re.split(r"[,;]", str(val))]


class DatasetIndex:
    """Read-only lookups deciding whether an accession's Dataset can be reused."""

    def __init__(self, syn):
        self.syn = syn
        self._entities = {}
        self._collection = None

    def collection_ids(self):
        if self._collection is None:
            coll = self.syn.restGET(f"/entity/{COLLECTION_SYNID}")
            self._collection = {i.get("entityId") for i in coll.get("items", [])}
        return self._collection

    def lookup(self, accession):
        """(entity_id, grants, reason): entity_id is set only for an indexed Dataset
        (a Dataset, with items, in the dataset collection)."""
        if accession not in self._entities:
            self._entities[accession] = self._lookup(accession)
        return self._entities[accession]

    def _lookup(self, accession):
        eid = self.syn.findEntityId(accession, parent=CENTRAL_PROJECT_SYNID)
        if not eid:
            return None, [], "no entity of that name in the central project"
        ent = self.syn.restGET(f"/entity/{eid}")
        if not ent.get("concreteType", "").endswith(".Dataset"):
            return None, [], f"{eid} is not a Dataset ({ent.get('concreteType')})"
        if not ent.get("items"):
            return None, [], f"{eid} has no items"
        if eid not in self.collection_ids():
            return None, [], f"{eid} is not in collection {COLLECTION_SYNID}"
        return eid, _grant_annotation(self.syn, eid), None


def grant_of(path):
    """Grant a split file belongs to (the CAxxxxxx prefix of its file name)."""
    return Path(path).name.split("_")[0]


def drop_empty_link_rows(paths: pd.DataFrame, link_column, report: list):
    """Drop rows with an empty link column from each split file (rewritten in
    place); drop files left empty from `paths`. Returns the filtered paths."""
    keep = []
    for _, prow in paths.iterrows():
        df = read_csv(prow["File Paths"])
        empty = df[link_column].str.strip() == ""
        for i in df.index[empty]:
            label = df.iloc[i].get("Tool Name") or df.iloc[i].get(DATASET_NAME_COLUMN) or f"row {i}"
            logger.warning("Dropping %s row %d (%s): empty %r", Path(prow["File Paths"]).name, i, label, link_column)
            report.append({"file": prow["File Paths"], "row": int(i), "name": label, "reason": f"empty {link_column}"})
        df = df[~empty]
        if df.empty:
            logger.warning("Dropping %s: no rows left", Path(prow["File Paths"]).name)
            continue
        df.to_csv(prow["File Paths"], index=False)
        keep.append(prow)
    return pd.DataFrame(keep, columns=paths.columns)


def reuse_existing_datasets(syn, paths: pd.DataFrame, pk, link_column, dir_: Path, report: list):
    """Split each file into rows whose accession already has an indexed Dataset
    (IDs filled in, kept aside in dir_/reused/) and the rest (left in the file
    for create_entity_links, which cannot take pre-existing IDs). Files with no
    unmatched rows are dropped from the returned paths.

    An entity is reused only if it is a Dataset with items, in the dataset
    collection, and already associated with this row's grant (its grantNumber
    annotation lists it). Anything else is passed through: create_entity_links
    then calls geo_synapse, whose create_synapse_dataset reuses a same-named
    Dataset (Dataset.store() merges into the existing entity), run_one adds the
    missing items, re-annotates (grantNumber/studyId for this grant) and adds it
    to the collection if absent; a non-Dataset of that name makes indexing fail
    and the row falls back to a plain File link. A Dataset for a *different*
    grant is passed through, not reused, because associating it with this grant
    is run_one's job (re-running it also replaces the earlier grantNumber)."""
    index = DatasetIndex(syn)
    reused_dir = dir_ / "reused"
    reused_dir.mkdir(exist_ok=True)
    keep = []
    for _, prow in paths.iterrows():
        path = prow["File Paths"]
        grant = grant_of(path)
        df = read_csv(path)
        matched = {}
        for i, row in df.iterrows():
            acc = parse_accession(row[DATASET_NAME_COLUMN], row[link_column])
            if not acc:
                continue
            eid, grants, reason = index.lookup(acc)
            if eid and grant not in grants:
                eid, reason = None, f"{eid} is not associated with grant {grant} (grants: {grants})"
            if eid:
                matched[i] = eid
                logger.info("Reusing %s for %s (%s)", eid, acc, Path(path).name)
                report.append({"file": path, "accession": acc, "entity_id": eid})
            else:
                logger.info("Not reusing an entity for %s (%s): %s", acc, Path(path).name, reason)
        if matched:
            hit = df.loc[list(matched)].copy()
            hit[pk] = [matched[i] for i in hit.index]
            hit.to_csv(reused_dir / Path(path).name, index=False)
            df = df.drop(index=list(matched))
            if df.empty:
                continue  # never hand a header-only CSV on
            df.to_csv(path, index=False)
        keep.append(prow)
    return pd.DataFrame(keep, columns=paths.columns)


def merge_reused(paths_after: pd.DataFrame, original_paths: pd.DataFrame, dir_: Path, final_dir: Path):
    """Build final per-grant files: reused rows (with IDs) + created rows."""
    final_dir.mkdir(exist_ok=True)
    rows = []
    for _, prow in original_paths.iterrows():
        name = Path(prow["File Paths"]).name
        parts = []
        created = dir_ / name
        if created.exists() and str(created) in set(paths_after["File Paths"]):
            parts.append(read_csv(created))
        reused = dir_ / "reused" / name
        if reused.exists():
            parts.append(read_csv(reused))
        if not parts:
            continue
        dest = final_dir / name
        pd.concat(parts, ignore_index=True).to_csv(dest, index=False)
        new = prow.copy()
        new["File Paths"] = str(dest)
        rows.append(new)
    return pd.DataFrame(rows, columns=original_paths.columns)


# --- stages ----------------------------------------------------------------


def stage_split(run, args):
    d = run.stage_dir("split")
    mtype = CONFIG[args.type][0]
    argv = [str(args.manifest), mtype, str(d), "--csv"]
    if args.cv_file:
        argv += ["--cv-file", args.cv_file]
    _load("split_manifest_grants.py").main(argv)
    files = sorted(str(p) for p in d.glob("*.csv"))
    if not files:
        raise RuntimeError("split produced no files")
    return {"dir": str(d), "files": files}


def stage_paths(run, args):
    d = run.stage_dir("paths")
    _, dtype, _, folder_col = CONFIG[args.type]
    out = d / "paths.csv"
    split_dir = run.output("split", "dir")
    _load("gen-mp-csv.py").main(split_dir, str(out), dtype)
    paths = read_csv(out)
    missing = paths[paths[folder_col].str.strip() == ""]["File Paths"].tolist()
    for m in missing:
        logger.warning("No target folder resolved for %s", m)
    return {"paths_csv": str(out), "unresolved": missing}


def stage_reshape(run, args):
    d = run.stage_dir("reshape")
    paths = retarget(read_csv(run.output("paths", "paths_csv")), d)
    out = write_paths_csv(paths, d / "paths.csv")
    _load("processing-splits.py").main([out])
    return {"paths_csv": out, "files": paths["File Paths"].tolist()}


def exclude_unresolved_folders(paths: pd.DataFrame, folder_col):
    """Drop split files whose grant has no target folder (logged). Returns
    (kept paths, excluded file paths)."""
    unresolved = paths[folder_col].str.strip() == ""
    excluded = paths[unresolved]["File Paths"].tolist()
    for p in excluded:
        logger.warning("Excluding %s: no target folder resolved for its grant", p)
    return paths[~unresolved], excluded


def stage_links(run, args, syn=None):
    reshaped = read_csv(run.output("reshape", "paths_csv"))
    folder_col = CONFIG[args.type][3]
    # Resuming at links keeps created_ids.csv: it is how finished rows are skipped.
    d = run.stage_dir("links", keep=("created_ids.csv",) if args.from_stage == "links" else ())
    run.state["dropped_rows"] = []
    run.state["unresolved_folder_files"] = []

    if args.type not in LINK_COLUMNS:
        kept, excluded = exclude_unresolved_folders(reshaped, folder_col)
        run.state["unresolved_folder_files"] = excluded
        out = write_paths_csv(kept, d / "paths.csv")
        return "skipped", {"paths_csv": out, "excluded_no_folder": len(excluded)}, f"{args.type} has no link entities"

    link_column, pk = LINK_COLUMNS[args.type]
    original = retarget(reshaped, d)
    paths, excluded = exclude_unresolved_folders(original, folder_col)
    run.state["unresolved_folder_files"] = excluded

    # Reuse comes first so a row whose alias carries a reusable accession is not
    # dropped for an empty link.
    reused = []
    if args.type == "DatasetView":
        if syn is None:
            import utils

            syn = utils.syn_login()
        paths = reuse_existing_datasets(syn, paths, pk, link_column, d, reused)
    pd.DataFrame(reused, columns=["file", "accession", "entity_id"]).to_csv(d / "reused_entities.csv", index=False)

    dropped = []
    paths = drop_empty_link_rows(paths, link_column, dropped)
    pd.DataFrame(dropped, columns=["file", "row", "name", "reason"]).to_csv(d / "dropped_rows.csv", index=False)
    run.state["dropped_rows"] = dropped

    pending = write_paths_csv(paths, d / "pending_paths.csv")
    outputs = {"pending_paths_csv": pending, "dropped": len(dropped), "reused": len(reused),
               "excluded_no_folder": len(excluded)}

    if not args.execute:
        # Nothing is created; the filtered (and ID-reusing) files are still produced.
        final = merge_reused(paths, original, d, d / "final")
        outputs["paths_csv"] = write_paths_csv(final, d / "paths.csv")
        return "skipped", outputs, "dry run: link entities not created (needs --execute)"

    if len(paths):
        # created_ids.csv lets an interrupted run resume without re-storing finished rows
        _load("create_entity_links.py").main(["-m", pending, "-t", args.type,
                                              "--created-ids", str(d / "created_ids.csv")])
    final = merge_reused(paths, original, d, d / "final")
    outputs["paths_csv"] = write_paths_csv(final, d / "paths.csv")
    return "done", outputs, None


def stage_upload(run, args):
    d = run.stage_dir("upload")
    links = run.state["stages"]["links"]
    if args.execute and "dry run" in links.get("note", ""):
        raise RuntimeError("links was skipped in a dry run; resume with --from-stage links --execute")
    paths_csv = links["outputs"]["paths_csv"]
    if not len(read_csv(paths_csv)):
        logger.warning("No split files left to upload")
        return {"paths_csv": paths_csv, "uploaded": 0}

    if args.target == "recordset":
        argv = ["--paths-csv", paths_csv, "--component", args.type, "--report-dir", str(d),
                "--run-id", "reports"]
        argv += (["--write-log-table", args.write_log_table] if args.write_log_table
                 else ["--write-log-csv", args.write_log_csv])
        if args.execute:
            argv.append("--execute")
        code = _load("upload_recordsets.py").main(argv)
        if code:
            raise RuntimeError(f"upload_recordsets.py reported failures (exit {code})")
        return {"paths_csv": paths_csv, "reports": str(d / "reports")}

    cmd = [sys.executable, str(HERE / "upload-manifests.py"), "-m", paths_csv, "-t", args.type]
    if args.schematic_config:
        cmd += ["-c", args.schematic_config]
    if not args.execute:
        # upload-manifests.py has no dry-run mode (it validates through schematic and
        # then submits), so it is only run with --execute.
        logger.info("Dry run: would run %s", " ".join(cmd))
        return {"paths_csv": paths_csv, "command": cmd, "ran": False}
    logger.info("Running %s", " ".join(cmd))
    result = subprocess.run(cmd, env=subprocess_env())
    if result.returncode:
        raise RuntimeError(f"upload-manifests.py failed (exit {result.returncode})")
    return {"paths_csv": paths_csv, "command": cmd, "ran": True}


# --- driver ----------------------------------------------------------------


def get_args(argv=None):
    p = argparse.ArgumentParser(
        description=__doc__.strip().split("\n")[0],
        epilog="exit codes: 0 ok; 1 a stage failed; 2 finished, but rows were dropped or files excluded "
               "(see run.json)")
    p.add_argument("--manifest", required=True, help="manifest CSV or .xlsx")
    p.add_argument("--type", required=True, choices=COMPONENTS)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--target", choices=["recordset", "schematic-table"], default="recordset")
    p.add_argument("--write-log-table", help="Synapse ID of the write-log table (recordset target)")
    p.add_argument("--write-log-csv", help="local CSV write log instead of a table (tests, dry runs)")
    p.add_argument("--schematic-config", help="schematic config file (schematic-table target)")
    p.add_argument("--cv-file", help="all_valid_values.csv path/URL for split (default: pinned release)")
    p.add_argument("--execute", action="store_true", help="write to Synapse; the default is a dry run")
    p.add_argument("--from-stage", choices=STAGES, help="resume at this stage, reusing earlier outputs in run.json")
    p.add_argument("--force-new", action="store_true",
                   help="start a new run in an out-dir holding a different manifest/type's run: "
                        "wipes the out-dir's stage folders")
    args = p.parse_args(argv)
    if args.force_new and args.from_stage:
        p.error("--force-new cannot be combined with --from-stage")
    if args.target == "recordset" and not (args.write_log_table or args.write_log_csv):
        p.error("--target recordset needs --write-log-table (or --write-log-csv)")
    if args.target == "schematic-table" and args.execute and not args.schematic_config:
        p.error("--target schematic-table with --execute needs --schematic-config")
    return args


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def check_same_run(run, args):
    """Refuse --from-stage unless the arguments that shaped the earlier stages are
    unchanged: manifest path and content, type, target and --cv-file."""
    current = {"manifest": str(args.manifest), "manifest_sha256": args.manifest_sha256,
               "type": args.type, "target": args.target, "cv_file": args.cv_file}
    diffs = [f"  {k}: run.json has {run.state.get(k)!r}, now {v!r}"
             for k, v in current.items() if run.state.get(k) != v]
    if diffs:
        sys.exit(f"Cannot resume at {args.from_stage}: arguments differ from the run recorded in {run.path}:\n"
                 + "\n".join(diffs) + "\nRerun without --from-stage (add --force-new to discard the old run).")


def check_out_dir_free(run, args):
    """A new run in an out-dir that already holds a different run's stages needs --force-new."""
    leftovers = [run.stage_path(s) for s in STAGES if run.stage_path(s).exists()]
    if not (run.path.exists() or leftovers) or args.force_new:
        return
    if not run.path.exists():
        sys.exit(f"{run.out_dir} holds stage folders but no run.json; use an empty --out-dir or --force-new.")
    diffs = [k for k, v in (("manifest", str(args.manifest)), ("manifest_sha256", args.manifest_sha256),
                            ("type", args.type)) if run.state.get(k) != v]
    if diffs:
        sys.exit(f"{run.out_dir} holds a run with a different {', '.join(diffs)} (see {run.path}); "
                 "use another --out-dir or --force-new to wipe its stage folders.")


def main(argv=None, syn=None):
    args = get_args(argv)
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    args.manifest = Path(args.manifest).resolve()
    args.manifest_sha256 = sha256_of(args.manifest)
    run = Run(out_dir, args)

    start = STAGES.index(args.from_stage) if args.from_stage else 0
    if args.from_stage:
        check_same_run(run, args)
    else:
        check_out_dir_free(run, args)
        if args.force_new:
            for s in STAGES:
                shutil.rmtree(run.stage_path(s), ignore_errors=True)
            run.state = {"stages": {}}
    for earlier in STAGES[:start]:
        if run.state["stages"].get(earlier, {}).get("status") not in ("done", "skipped"):
            sys.exit(f"Cannot resume at {args.from_stage}: stage {earlier!r} has not finished in {run.path}")
    for later in STAGES[start:]:
        run.state["stages"].pop(later, None)
    if start <= STAGES.index("links"):
        run.state.pop("dropped_rows", None)
        run.state.pop("unresolved_folder_files", None)
    run.save()  # never leave a run.json that still lists the stages about to be redone

    print(f"{'EXECUTE' if args.execute else 'Dry run'}: {args.type} -> {args.target}, out {out_dir}")
    for stage in STAGES[start:]:
        started = now()
        print(f"== {stage}")
        try:
            if stage == "links":
                status, outputs, note = stage_links(run, args, syn=syn)
            else:
                outputs = {"split": stage_split, "paths": stage_paths,
                           "reshape": stage_reshape, "upload": stage_upload}[stage](run, args)
                status, note = "done", None
        except BaseException as err:
            # SystemExit/KeyboardInterrupt (a stage script calling sys.exit, Ctrl-C)
            # must also leave the stage marked failed, then propagate.
            run.record(stage, "failed", started, note=repr(err))
            if not isinstance(err, Exception):
                logger.error("Stage %s interrupted: %r", stage, err)
                raise
            logger.error("Stage %s failed: %s", stage, err)
            return 1
        run.record(stage, status, started, outputs, note)
        if note:
            print(f"   {status}: {note}")
    dropped, excluded = run.state.get("dropped_rows"), run.state.get("unresolved_folder_files")
    if dropped or excluded:
        print(f"Finished with {len(dropped or [])} dropped row(s) and {len(excluded or [])} file(s) excluded "
              f"for an unresolved folder (listed in {run.path}); exit code 2")
        return 2
    print(f"Finished; see {run.path}")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    sys.exit(main())
