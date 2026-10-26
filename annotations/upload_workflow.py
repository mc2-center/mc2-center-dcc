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

Before ``links`` the driver (a) drops rows whose link column is empty (logged),
dropping any split file left empty, and (b) for DatasetView, reuses Dataset
entities that already exist in the central project for a row's GEO/SRA
accession instead of creating a second one.

author: orion.banks
"""

import argparse
import importlib.util
import json
import logging
import os
import re
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
# the "datasets" folder holding its "<accession>_links" folder).
CENTRAL_PROJECT_SYNID = "syn76986881"
DATASETS_FOLDER = "datasets"
ACCESSION_RE = re.compile(r"\b(GSE\d+|GDS\d+|SR[PX]\d+|PRJNA\d+|PRJEB\d+)\b")

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

    def stage_dir(self, stage):
        d = self.out_dir / f"{STAGES.index(stage) + 1:02d}_{stage}"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def output(self, stage, key):
        return self.state["stages"][stage]["outputs"][key]

    def save(self):
        self.state.update(
            manifest=str(self.args.manifest),
            type=self.args.type,
            target=self.args.target,
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
    try:
        from geo_synapse.mc2_table import parse_accession as pa

        return pa(f"[{name}]({link})")
    except ImportError:
        m = ACCESSION_RE.search(f"{name} {link}")
        return m.group(1) if m else None


def find_existing_dataset(syn, accession):
    """Synapse ID of an existing Dataset entity for `accession` in the central
    project, or None. Read-only."""
    from synapseclient.core.exceptions import SynapseFileNotFoundError

    try:
        return syn.findEntityId(accession, parent=CENTRAL_PROJECT_SYNID)
    except SynapseFileNotFoundError:
        return None


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
            report.append({"file": prow["File Paths"], "row": i, "name": label, "reason": f"empty {link_column}"})
        df = df[~empty]
        if df.empty:
            logger.warning("Dropping %s: no rows left", Path(prow["File Paths"]).name)
            continue
        df.to_csv(prow["File Paths"], index=False)
        keep.append(prow)
    return pd.DataFrame(keep, columns=paths.columns)


def reuse_existing_datasets(syn, paths: pd.DataFrame, pk, link_column, dir_: Path, report: list):
    """Split each file into rows whose accession already has a Dataset entity
    (IDs filled in, kept aside in dir_/reused/) and the rest (left in the file
    for create_entity_links, which cannot take pre-existing IDs). Files with no
    unmatched rows are dropped from the returned paths."""
    reused_dir = dir_ / "reused"
    reused_dir.mkdir(exist_ok=True)
    cache, keep = {}, []
    for _, prow in paths.iterrows():
        path = prow["File Paths"]
        df = read_csv(path)
        matched = {}
        for i, row in df.iterrows():
            acc = parse_accession(row[DATASET_NAME_COLUMN], row[link_column])
            if not acc:
                continue
            if acc not in cache:
                cache[acc] = find_existing_dataset(syn, acc)
            if cache[acc]:
                matched[i] = cache[acc]
                logger.info("Reusing %s for %s (%s)", cache[acc], acc, Path(path).name)
                report.append({"file": path, "accession": acc, "entity_id": cache[acc]})
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


def stage_links(run, args, syn=None):
    reshaped = run.output("reshape", "paths_csv")
    if args.type not in LINK_COLUMNS:
        return "skipped", {"paths_csv": reshaped}, f"{args.type} has no link entities"
    d = run.stage_dir("links")
    link_column, pk = LINK_COLUMNS[args.type]
    folder_col = CONFIG[args.type][3]

    original = retarget(read_csv(reshaped), d)
    unresolved = original[folder_col].str.strip() == ""
    for p in original[unresolved]["File Paths"]:
        logger.warning("Excluding %s from links: no target folder", p)
    paths = original[~unresolved]

    dropped = []
    paths = drop_empty_link_rows(paths, link_column, dropped)
    pd.DataFrame(dropped, columns=["file", "row", "name", "reason"]).to_csv(d / "dropped_rows.csv", index=False)

    reused = []
    if args.type == "DatasetView":
        if syn is None:
            import utils

            syn = utils.syn_login()
        paths = reuse_existing_datasets(syn, paths, pk, link_column, d, reused)
    pd.DataFrame(reused, columns=["file", "accession", "entity_id"]).to_csv(d / "reused_entities.csv", index=False)

    pending = write_paths_csv(paths, d / "pending_paths.csv")
    outputs = {"pending_paths_csv": pending, "dropped": len(dropped), "reused": len(reused)}

    if not args.execute:
        # Nothing is created; the filtered (and ID-reusing) files are still produced.
        final = merge_reused(paths, original, d, d / "final")
        outputs["paths_csv"] = write_paths_csv(final, d / "paths.csv")
        return "skipped", outputs, "dry run: link entities not created (needs --execute)"

    if len(paths):
        _load("create_entity_links.py").main(["-m", pending, "-t", args.type])
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
    p = argparse.ArgumentParser(description=__doc__.strip().split("\n")[0])
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
    args = p.parse_args(argv)
    if args.target == "recordset" and not (args.write_log_table or args.write_log_csv):
        p.error("--target recordset needs --write-log-table (or --write-log-csv)")
    if args.target == "schematic-table" and args.execute and not args.schematic_config:
        p.error("--target schematic-table with --execute needs --schematic-config")
    return args


def main(argv=None, syn=None):
    args = get_args(argv)
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    args.manifest = Path(args.manifest).resolve()
    run = Run(out_dir, args)

    start = STAGES.index(args.from_stage) if args.from_stage else 0
    for earlier in STAGES[:start]:
        if run.state["stages"].get(earlier, {}).get("status") not in ("done", "skipped"):
            sys.exit(f"Cannot resume at {args.from_stage}: stage {earlier!r} has not finished in {run.path}")
    for later in STAGES[start:]:
        run.state["stages"].pop(later, None)

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
        except Exception as err:
            run.record(stage, "failed", started, note=repr(err))
            logger.error("Stage %s failed: %s", stage, err)
            return 1
        run.record(stage, status, started, outputs, note)
        if note:
            print(f"   {status}: {note}")
    print(f"Finished; see {run.path}")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    sys.exit(main())
