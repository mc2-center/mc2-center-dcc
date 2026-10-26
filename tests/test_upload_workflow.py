"""D15: annotations/upload_workflow.py, dry run end to end on the D8 handoff fixtures.

Synapse is mocked: gen-mp-csv's query is a canned grant/folder table, the
upload scripts are replaced by recorders, and Synapse entity lookups use a fake.
"""

import importlib.util
import json
import sys
import types
from pathlib import Path

import pandas as pd
import pytest

ANNOTATIONS = Path(__file__).resolve().parent.parent / "annotations"
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "handoff"
CV = FIXTURES / "all_valid_values.csv"
GRANTS = ["CA209997", "CA209923", "CA184898"]

spec = importlib.util.spec_from_file_location("upload_workflow", ANNOTATIONS / "upload_workflow.py")
uw = importlib.util.module_from_spec(spec)
spec.loader.exec_module(uw)


@pytest.fixture
def env(monkeypatch):
    """Mock Synapse-facing pieces; returns a record of upload/link calls."""
    calls = {"links": [], "upload": []}
    genmp = uw._load("gen-mp-csv.py")

    def fake_query(ref_path, data_type, folder_col, grant_col):
        ref = pd.DataFrame({"grantNumber": GRANTS, grant_col: ["syn0", "syn1", "syn2"],
                            folder_col: ["syn100", "syn101", "syn102"]})
        ref.to_csv(ref_path, index=False)
        return ref

    monkeypatch.setattr(genmp, "query_synapse_for_folder_info", fake_query)

    # geo_synapse is not a dependency of this repo's tests: stand in for its parser
    import re

    def fake_parse(cell):
        m = re.search(r"\]\(([^)]+)\)", cell or "")
        m = m and re.search(r"[?&](?:acc|term)=((?:GSE|SRP|ERP|DRP)\d+)", m.group(1), re.I)
        return m.group(1).upper() if m else None

    pkg, mod = types.ModuleType("geo_synapse"), types.ModuleType("geo_synapse.mc2_table")
    mod.parse_accession = fake_parse
    pkg.mc2_table = mod
    monkeypatch.setitem(sys.modules, "geo_synapse", pkg)
    monkeypatch.setitem(sys.modules, "geo_synapse.mc2_table", mod)

    rec = types.SimpleNamespace(main=lambda argv: calls["upload"].append(argv) or 0)
    links = types.SimpleNamespace(main=lambda argv: calls["links"].append(argv))
    real_load = uw._load
    monkeypatch.setattr(uw, "_load", lambda f: {"upload_recordsets.py": rec,
                                                "create_entity_links.py": links}.get(f) or real_load(f))
    return calls


class FakeSyn:
    """Read-only Synapse fake. `existing` maps accession -> ID; `entities` overrides
    the default entity (an indexed Dataset with items, in the collection, annotated
    with all three fixture grants)."""

    def __init__(self, existing, entities=None, collection=True):
        self.existing = existing
        self.entities = entities or {}
        self.collection = collection

    def findEntityId(self, name, parent=None):
        assert parent == uw.CENTRAL_PROJECT_SYNID
        return self.existing.get(name)

    def _entity(self, eid):
        default = {"concreteType": "org.sagebionetworks.repo.model.table.Dataset",
                   "items": [{"entityId": "syn1", "versionNumber": 1}], "grants": GRANTS}
        return {**default, **self.entities.get(eid, {})}

    def restGET(self, url):
        parts = url.strip("/").split("/")
        eid = parts[1]
        if eid == uw.COLLECTION_SYNID:
            ids = list(self.existing.values()) if self.collection else []
            return {"items": [{"entityId": i, "versionNumber": 1} for i in ids]}
        ent = self._entity(eid)
        if parts[-1] == "annotations2":
            return {"annotations": {"grantNumber": {"value": ent["grants"]}} if ent["grants"] else {}}
        return {"concreteType": ent["concreteType"], "items": ent["items"]}


def run(tmp_path, type_="PublicationView", *extra, manifest=None, syn=None):
    name = {"PublicationView": "publication.csv", "DatasetView": "dataset.csv", "ToolView": "tool.xlsx"}[type_]
    argv = ["--manifest", str(manifest or FIXTURES / "p1" / name), "--type", type_,
            "--out-dir", str(tmp_path / "out"), "--cv-file", str(CV),
            "--write-log-csv", str(tmp_path / "log.csv"), *extra]
    code = uw.main(argv, syn=syn)
    return code, json.loads((tmp_path / "out" / "run.json").read_text())


def test_dry_run_writes_stage_folders_and_run_json(tmp_path, env):
    code, state = run(tmp_path)
    assert code == 0
    out = tmp_path / "out"
    for d in ["01_split", "02_paths", "03_reshape", "05_upload"]:
        assert (out / d).is_dir()
    assert list(state["stages"]) == ["split", "paths", "reshape", "links", "upload"]
    for s in state["stages"].values():
        assert s["started"] and s["finished"]
    assert state["stages"]["links"]["status"] == "skipped"
    assert state["execute"] is False
    assert len(list((out / "01_split").glob("*_publication.csv"))) == 3
    assert "--execute" not in env["upload"][0]
    assert env["links"] == []


def test_links_skipped_in_dry_run_and_created_on_execute(tmp_path, env):
    code, state = run(tmp_path, "ToolView")
    assert env["links"] == []
    assert "dry run" in state["stages"]["links"]["note"]
    code, state = run(tmp_path, "ToolView", "--execute")
    assert code == 0 and len(env["links"]) == 1
    assert env["links"][0][2:4] == ["-t", "ToolView"]
    assert env["links"][0][-2:] == ["--created-ids", str(tmp_path / "out" / "04_links" / "created_ids.csv")]
    assert "--execute" in env["upload"][-1]


def test_from_stage_resumes(tmp_path, env):
    run(tmp_path)
    split_stamp = json.loads((tmp_path / "out" / "run.json").read_text())["stages"]["split"]["finished"]
    code, state = run(tmp_path, "PublicationView", "--from-stage", "upload")
    assert code == 0
    assert state["stages"]["split"]["finished"] == split_stamp
    assert len(env["upload"]) == 2


def test_resume_requires_earlier_stages(tmp_path, env):
    with pytest.raises(SystemExit):
        run(tmp_path, "PublicationView", "--from-stage", "reshape")


def test_resume_execute_after_dry_run_must_redo_links(tmp_path, env):
    run(tmp_path, "ToolView")
    code, state = run(tmp_path, "ToolView", "--from-stage", "upload", "--execute")
    assert code == 1 and state["stages"]["upload"]["status"] == "failed"


def test_empty_link_rows_dropped_and_empty_files_removed(tmp_path, env):
    df = pd.read_excel(FIXTURES / "p1" / "tool.xlsx", sheet_name="manifest")
    # tool0 is one of two CA209997 rows; tool1 is CA209923's only row (and also in CA184898)
    df.loc[df["Tool Name"].isin(["tool0", "tool1"]), "Tool Homepage"] = None
    manifest = tmp_path / "tools.csv"
    df.to_csv(manifest, index=False)
    code, state = run(tmp_path, "ToolView", manifest=manifest)
    assert code == 2  # rows were dropped: distinct from failure (1)
    assert len(state["dropped_rows"]) == 3 and "reason" in state["dropped_rows"][0]
    links = tmp_path / "out" / "04_links"
    dropped = pd.read_csv(links / "dropped_rows.csv")
    assert len(dropped) == 3  # tool0 once, tool1 in two grant files
    assert "Tool Homepage" in dropped["reason"].iloc[0]
    final = sorted((links / "final").glob("*.csv"))
    assert final and all(len(pd.read_csv(f)) > 0 for f in final)
    assert not any("CA209923" in f.name for f in final)
    uploaded = pd.read_csv(state["stages"]["upload"]["outputs"]["paths_csv"])
    assert not any("CA209923" in p for p in uploaded["File Paths"])


def test_existing_geo_accession_is_reused(tmp_path, env):
    syn = FakeSyn({"GSE150001": "syn999"})
    code, state = run(tmp_path, "DatasetView", "--execute", syn=syn)
    assert code == 0
    links = tmp_path / "out" / "04_links"
    reused = pd.read_csv(links / "reused_entities.csv")
    assert set(reused["accession"]) == {"GSE150001"} and set(reused["entity_id"]) == {"syn999"}
    # create_entity_links never sees the matched row
    pending = pd.read_csv(env["links"][0][1])
    for p in pending["File Paths"]:
        assert "GSE150001" not in pd.read_csv(p)["Dataset Alias"].tolist()
    # ...and the final files carry the existing ID
    rows = pd.concat([pd.read_csv(f, dtype=str) for f in (links / "final").glob("*.csv")])
    assert set(rows.loc[rows["Dataset Alias"] == "GSE150001", "DatasetView_id"]) == {"syn999"}


def reuse_run(tmp_path, syn):
    code, state = run(tmp_path, "DatasetView", "--execute", syn=syn)
    links = tmp_path / "out" / "04_links"
    pending = pd.read_csv(links / "pending_paths.csv")
    pending_aliases = [a for p in pending["File Paths"] for a in pd.read_csv(p)["Dataset Alias"]]
    return state, pending_aliases


@pytest.mark.parametrize("entity", [
    {"concreteType": "org.sagebionetworks.repo.model.Folder"},
    {"items": []},
    {"grants": ["CA000000"]},   # indexed, but for another grant: run_one must associate it
    {"grants": []},
])
def test_unusable_existing_entity_is_passed_through(tmp_path, env, entity):
    state, pending = reuse_run(tmp_path, FakeSyn({"GSE150001": "syn999"}, {"syn999": entity}))
    assert state["stages"]["links"]["outputs"]["reused"] == 0
    assert pending.count("GSE150001") == 2


def test_entity_outside_the_collection_is_passed_through(tmp_path, env):
    state, pending = reuse_run(tmp_path, FakeSyn({"GSE150001": "syn999"}, collection=False))
    assert state["stages"]["links"]["outputs"]["reused"] == 0 and pending.count("GSE150001") == 2


def test_reuse_is_per_grant(tmp_path, env):
    syn = FakeSyn({"GSE150001": "syn999"}, {"syn999": {"grants": ["CA209923"]}})
    state, pending = reuse_run(tmp_path, syn)
    assert state["stages"]["links"]["outputs"]["reused"] == 1  # CA209923's file only
    assert pending.count("GSE150001") == 1


def test_accession_parsing_requires_geo_synapse(tmp_path, env, monkeypatch):
    monkeypatch.setitem(sys.modules, "geo_synapse.mc2_table", None)
    code, state = run(tmp_path, "DatasetView", "--execute", syn=FakeSyn({}))
    assert code == 1 and "geo_synapse" in state["stages"]["links"]["note"]


def test_collection_constants_match_create_entity_links():
    src = (ANNOTATIONS / "create_entity_links.py").read_text()
    assert f'_CENTRAL_PROJECT_SYNID = "{uw.CENTRAL_PROJECT_SYNID}"' in src
    assert f'_COLLECTION_SYNID = "{uw.COLLECTION_SYNID}"' in src


def test_dry_run_still_reports_reuse_without_creating(tmp_path, env):
    code, state = run(tmp_path, "DatasetView", syn=FakeSyn({"GSE150000": "syn5"}))
    assert env["links"] == [] and state["stages"]["links"]["outputs"]["reused"] == 1


def test_target_selection(tmp_path, env, monkeypatch):
    run(tmp_path)  # default recordset
    assert "PublicationView" in env["upload"][0] and "--write-log-csv" in env["upload"][0]

    ran = []
    monkeypatch.setattr(uw.subprocess, "run",
                        lambda cmd, env=None: ran.append((cmd, env)) or types.SimpleNamespace(returncode=0))
    out2 = tmp_path / "b"
    base = ["--manifest", str(FIXTURES / "p1" / "publication.csv"), "--type", "PublicationView",
            "--out-dir", str(out2), "--cv-file", str(CV), "--target", "schematic-table"]
    assert uw.main(base) == 0 and ran == []  # dry run: not invoked
    assert json.loads((out2 / "run.json").read_text())["stages"]["upload"]["outputs"]["ran"] is False

    monkeypatch.setenv("SYNAPSE_AUTH_TOKEN", "tok")
    assert uw.main(base + ["--schematic-config", "c.yml", "--execute", "--from-stage", "upload"]) == 0
    cmd, senv = ran[0]
    assert "upload-manifests.py" in cmd[1] and cmd[-2:] == ["-c", "c.yml"]
    assert senv["SYNAPSE_AUTH_TOKEN"] == "tok"
    assert len(env["upload"]) == 1  # the recordset uploader was only used by the first run


def test_recordset_target_needs_write_log():
    with pytest.raises(SystemExit):
        uw.get_args(["--manifest", "m.csv", "--type", "ToolView", "--out-dir", "o"])


def test_create_entity_links_main_takes_argv(monkeypatch):
    """Backward-compatible main(argv=None) refactor: argv parses, no argv reads sys.argv."""
    for name in ["geo_synapse", "geo_synapse.mc2_table", "geo_synapse.synapse_ops", "geo_synapse.pipeline"]:
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    sys.modules["geo_synapse.mc2_table"].parse_accession = lambda s: None
    sys.modules["geo_synapse.mc2_table"].find_project_for_grant = lambda *a: None
    for n in ["find_or_create_folder", "create_synapse_dataset", "add_to_dataset_collection"]:
        setattr(sys.modules["geo_synapse.synapse_ops"], n, None)
    sys.modules["geo_synapse.pipeline"].run_one = None
    spec2 = importlib.util.spec_from_file_location("cel", ANNOTATIONS / "create_entity_links.py")
    mod = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(mod)
    args = mod.get_args(["-m", "p.csv", "-t", "ToolView"])
    assert (args.m, args.t) == ("p.csv", "ToolView")
    monkeypatch.setattr(sys, "argv", ["x", "-t", "DatasetView"])
    assert mod.get_args().t == "DatasetView"


# --- create_entity_links: resume safety ---------------------------------------


def load_cel(monkeypatch):
    for name in ["geo_synapse", "geo_synapse.mc2_table", "geo_synapse.synapse_ops", "geo_synapse.pipeline"]:
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    sys.modules["geo_synapse.mc2_table"].parse_accession = lambda s: None
    sys.modules["geo_synapse.mc2_table"].find_project_for_grant = lambda *a: None
    for n in ["find_or_create_folder", "create_synapse_dataset", "add_to_dataset_collection"]:
        setattr(sys.modules["geo_synapse.synapse_ops"], n, None)
    sys.modules["geo_synapse.pipeline"].run_one = None
    spec2 = importlib.util.spec_from_file_location("cel_resume", ANNOTATIONS / "create_entity_links.py")
    mod = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(mod)
    return mod


class LinkSyn:
    """Fake client: findEntityId looks in `existing`; File.store records and can fail."""

    def __init__(self, existing=None, fail_on=None):
        self.existing, self.stored, self.fail_on = dict(existing or {}), [], fail_on

    def findEntityId(self, name, parent=None):
        return self.existing.get((parent, name))


def link_manifest(tmp_path, names):
    split = tmp_path / "CA1_tool.csv"
    pd.DataFrame({"Tool Name": names, "Tool Homepage": [f"https://x/{n}" for n in names],
                  "ToolView_id": ""}).to_csv(split, index=False)
    pending = tmp_path / "pending_paths.csv"
    pd.DataFrame({"File Paths": [str(split)], "folderIdTools": ["syn100"]}).to_csv(pending, index=False)
    return split, pending


def patch_file(mod, monkeypatch, syn):
    class FakeFile:
        def __init__(self, external_url, name, parent_id, synapse_store):
            self.name, self.parent_id = name, parent_id

        def store(self, synapse_client):
            if syn.fail_on == self.name:
                raise RuntimeError("boom")
            syn.stored.append(self.name)
            self.id = f"syn{1000 + len(syn.stored)}"
            return self

    monkeypatch.setattr(mod, "File", FakeFile)
    monkeypatch.setattr(mod.utils, "syn_login", lambda: syn)


def test_links_resume_skips_rows_already_created(tmp_path, monkeypatch):
    mod = load_cel(monkeypatch)
    split, pending = link_manifest(tmp_path, ["a", "b", "c"])
    ids = tmp_path / "created_ids.csv"
    argv = ["-m", str(pending), "-t", "ToolView", "--created-ids", str(ids)]

    syn = LinkSyn(fail_on="c")
    patch_file(mod, monkeypatch, syn)
    with pytest.raises(RuntimeError):
        mod.main(argv)
    assert syn.stored == ["a", "b"]
    assert len(pd.read_csv(ids)) == 2  # persisted as made, before the failure
    assert pd.read_csv(split, dtype=str, keep_default_na=False)["ToolView_id"].tolist() == ["", "", ""]

    syn = LinkSyn()  # resume: only the failed row is stored
    patch_file(mod, monkeypatch, syn)
    mod.main(argv)
    assert syn.stored == ["c"]
    assert pd.read_csv(split, dtype=str)["ToolView_id"].tolist() == ["syn1001", "syn1002", "syn1001"]


def test_links_reuse_existing_entity_by_name_under_parent(tmp_path, monkeypatch):
    mod = load_cel(monkeypatch)
    split, pending = link_manifest(tmp_path, ["a", "b"])
    syn = LinkSyn(existing={("syn100", "a"): "syn555"})
    patch_file(mod, monkeypatch, syn)
    mod.main(["-m", str(pending), "-t", "ToolView"])  # default ledger sits next to the manifest
    assert syn.stored == ["b"]
    assert pd.read_csv(split, dtype=str)["ToolView_id"].tolist()[0] == "syn555"
    assert (tmp_path / "created_ids.csv").exists()


# --- stale outputs ---------------------------------------------------------------


def test_stage_directories_are_cleared_when_a_stage_starts(tmp_path, env):
    run(tmp_path, "DatasetView", syn=FakeSyn({"GSE150001": "syn999"}))
    out = tmp_path / "out"
    stale = [out / "03_reshape" / "stale.csv", out / "04_links" / "reused" / "stale.csv",
             out / "04_links" / "final" / "stale.csv", out / "05_upload" / "old-run" / "decisions.csv"]
    for f in stale:
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text("x")
    run(tmp_path, "DatasetView", "--from-stage", "reshape", syn=FakeSyn({"GSE150001": "syn999"}))
    assert not any(f.exists() for f in stale)
    assert (out / "02_paths" / "paths.csv").exists()  # earlier stages are untouched


def test_resume_keeps_earlier_stage_directories_and_created_ids(tmp_path, env):
    run(tmp_path, "ToolView")
    out = tmp_path / "out"
    (out / "04_links" / "created_ids.csv").write_text("key,synapse_id\nk,syn1\n")
    (out / "04_links" / "note.txt").write_text("x")
    run(tmp_path, "ToolView", "--from-stage", "links", "--execute")
    assert (out / "04_links" / "created_ids.csv").read_text().startswith("key,synapse_id\nk,syn1")
    assert not (out / "04_links" / "note.txt").exists()
    run(tmp_path, "ToolView", "--from-stage", "upload", "--execute")
    assert (out / "04_links" / "created_ids.csv").exists()
    run(tmp_path, "ToolView", "--from-stage", "reshape", "--execute")  # a fresh links stage starts clean
    assert not (out / "04_links" / "created_ids.csv").exists()


# --- resume / new-run argument checks ------------------------------------------------


def test_run_json_records_identity(tmp_path, env):
    code, state = run(tmp_path)
    import hashlib

    assert state["manifest_sha256"] == hashlib.sha256((FIXTURES / "p1" / "publication.csv").read_bytes()).hexdigest()
    assert state["cv_file"] == str(CV) and state["type"] == "PublicationView" and state["target"] == "recordset"


def test_resume_refuses_changed_arguments(tmp_path, env, capsys):
    run(tmp_path)
    with pytest.raises(SystemExit) as e:  # different --cv-file
        uw.main(["--manifest", str(FIXTURES / "p1" / "publication.csv"), "--type", "PublicationView",
                 "--out-dir", str(tmp_path / "out"), "--write-log-csv", "l.csv", "--from-stage", "upload"])
    assert "cv_file" in str(e.value)
    with pytest.raises(SystemExit) as e:  # different target
        run(tmp_path, "PublicationView", "--from-stage", "upload", "--target", "schematic-table")
    assert "target" in str(e.value)
    with pytest.raises(SystemExit) as e:  # different type
        run(tmp_path, "ToolView", "--from-stage", "upload")
    assert "type" in str(e.value)
    edited = tmp_path / "edited.csv"  # same path semantics, different content
    edited.write_bytes((FIXTURES / "p1" / "publication.csv").read_bytes() + b"\n")
    with pytest.raises(SystemExit) as e:
        run(tmp_path, "PublicationView", "--from-stage", "upload", manifest=edited)
    assert "manifest" in str(e.value) and "manifest_sha256" in str(e.value)


def test_manifest_content_change_is_detected_at_same_path(tmp_path, env):
    manifest = tmp_path / "m.csv"
    manifest.write_bytes((FIXTURES / "p1" / "publication.csv").read_bytes())
    run(tmp_path, manifest=manifest)
    manifest.write_bytes(manifest.read_bytes() + b"\n")
    with pytest.raises(SystemExit) as e:
        run(tmp_path, "PublicationView", "--from-stage", "upload", manifest=manifest)
    assert "manifest_sha256" in str(e.value)


def test_new_run_in_used_out_dir_needs_force_new(tmp_path, env):
    run(tmp_path, "PublicationView")
    run(tmp_path, "PublicationView")  # same manifest and type: fine
    with pytest.raises(SystemExit) as e:
        run(tmp_path, "ToolView")
    assert "--force-new" in str(e.value)
    # the refused run left the earlier one intact
    assert json.loads((tmp_path / "out" / "run.json").read_text())["type"] == "PublicationView"
    code, state = run(tmp_path, "ToolView", "--force-new")
    assert code == 0 and state["type"] == "ToolView"
    assert not list((tmp_path / "out" / "01_split").glob("*_publication.csv"))
    with pytest.raises(SystemExit):
        uw.get_args(["--manifest", "m.csv", "--type", "ToolView", "--out-dir", "o", "--write-log-csv", "l",
                     "--force-new", "--from-stage", "links"])


# --- interrupts ---------------------------------------------------------------------


@pytest.mark.parametrize("exc", [SystemExit(3), KeyboardInterrupt()])
def test_base_exceptions_mark_stage_failed_and_propagate(tmp_path, env, monkeypatch, exc):
    def boom(run_, args):
        raise exc

    monkeypatch.setattr(uw, "stage_reshape", boom)
    with pytest.raises(type(exc)):
        run(tmp_path)
    stages = json.loads((tmp_path / "out" / "run.json").read_text())["stages"]
    assert stages["reshape"]["status"] == "failed" and stages["paths"]["status"] == "done"
    assert "upload" not in stages


def test_run_json_has_no_stale_done_records_while_running(tmp_path, env, monkeypatch):
    run(tmp_path)
    seen = {}
    real = uw.stage_split

    def peek(run_, args):
        seen.update(json.loads((tmp_path / "out" / "run.json").read_text())["stages"])
        return real(run_, args)

    monkeypatch.setattr(uw, "stage_split", peek)
    run(tmp_path)
    assert seen == {}


# --- dropped rows / unresolved folders -------------------------------------------------


def test_clean_run_exits_zero_and_records_nothing_dropped(tmp_path, env):
    code, state = run(tmp_path, "ToolView")
    assert code == 0 and state["dropped_rows"] == [] and state["unresolved_folder_files"] == []


def blank_first_folder(tmp_path, folder_col):
    paths_csv = tmp_path / "out" / "02_paths" / "paths.csv"
    paths = pd.read_csv(paths_csv, dtype=str, keep_default_na=False)
    paths.loc[0, folder_col] = ""
    paths.to_csv(paths_csv, index=False)
    return paths.loc[0, "File Paths"]


@pytest.mark.parametrize("type_,col", [("ToolView", "folderIdTools"), ("PublicationView", "folderIdPublication")])
def test_unresolved_folder_files_excluded_for_every_type(tmp_path, env, type_, col):
    run(tmp_path, type_)
    gone = Path(blank_first_folder(tmp_path, col)).name
    code, state = run(tmp_path, type_, "--from-stage", "reshape")
    assert code == 2
    assert [Path(f).name for f in state["unresolved_folder_files"]] == [gone]
    uploaded = pd.read_csv(state["stages"]["upload"]["outputs"]["paths_csv"])
    assert gone not in [Path(p).name for p in uploaded["File Paths"]] and len(uploaded) == 2


def test_alias_accession_is_reused_before_empty_link_drop(tmp_path, env):
    df = pd.read_csv(FIXTURES / "p1" / "dataset.csv", dtype=str, keep_default_na=False)
    df.loc[df["Dataset Alias"] == "GSE150000", "Dataset Url"] = ""
    manifest = tmp_path / "datasets.csv"
    df.to_csv(manifest, index=False)
    code, state = run(tmp_path, "DatasetView", "--execute", manifest=manifest, syn=FakeSyn({"GSE150000": "syn5"}))
    assert not any(r["name"] == "GSE150000" for r in state["dropped_rows"])
    final = pd.concat([pd.read_csv(f, dtype=str) for f in (tmp_path / "out" / "04_links" / "final").glob("*.csv")])
    assert set(final.loc[final["Dataset Alias"] == "GSE150000", "DatasetView_id"]) == {"syn5"}
    # without an existing entity the same row is dropped
    code, state = run(tmp_path, "DatasetView", "--force-new", manifest=manifest, syn=FakeSyn({}))
    assert [r["name"] for r in state["dropped_rows"]] == ["GSE150000"] and code == 2


# --- run id -------------------------------------------------------------------------


def test_run_id_is_passed_to_upload_recordsets(tmp_path, env):
    import re

    code, state = run(tmp_path)
    argv = env["upload"][0]
    rid = argv[argv.index("--run-id") + 1]
    assert re.fullmatch(r"out-\d{8}T\d{6}", rid) and state["run_id"] == rid
    assert state["stages"]["upload"]["outputs"]["reports"].endswith(rid)

    code, state = run(tmp_path, "PublicationView", "--run-id", "batch-7")
    argv = env["upload"][1]
    assert argv[argv.index("--run-id") + 1] == "batch-7" and state["run_id"] == "batch-7"


# --- stray reference file -----------------------------------------------------------


def test_split_ignores_grant_folder_reference(tmp_path, env, monkeypatch):
    split_main = uw._load("split_manifest_grants.py").main

    def with_stray(argv):
        split_main(argv)
        (Path(argv[2]) / "grant_folder_reference_2026-01-01.csv").write_text("grantNumber\n")

    monkeypatch.setattr(uw._load("split_manifest_grants.py"), "main", with_stray)
    code, state = run(tmp_path)
    files = state["stages"]["split"]["outputs"]["files"]
    assert len(files) == 3 and not any("grant_folder_reference" in f for f in files)


# --- end ---
