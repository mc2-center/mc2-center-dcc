"""D15: annotations/upload_workflow.py, dry run end to end on the D8 handoff fixtures.

Synapse is mocked: gen-mp-csv's query is a canned grant/folder table, the
upload scripts are replaced by recorders, and Synapse entity lookups use a fake.
"""

import json
import sys
import types
from pathlib import Path

import pandas as pd
import pytest

from conftest import load_script

ANNOTATIONS = Path(__file__).resolve().parent.parent / "annotations"
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "handoff"
CV = FIXTURES / "all_valid_values.csv"
GRANTS = ["CA209997", "CA209923", "CA184898"]

uw = load_script("annotations/upload_workflow.py", "upload_workflow")


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

    real_load = uw._load
    real_upload_get_args = real_load("upload_recordsets.py").get_args

    def fake_upload_main(argv):
        real_upload_get_args(argv)  # a bad argv fails here, as it would in production
        calls["upload"].append(argv)
        return 0

    rec = types.SimpleNamespace(main=fake_upload_main)
    links = types.SimpleNamespace(main=lambda argv: calls["links"].append(argv))
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
            "--write-log-table", "syn999", *extra]
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
    assert "--overwrite-csv" not in env["upload"][0]
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


def test_tool_rows_with_empty_homepage_are_kept(tmp_path, env):
    # Was test_empty_link_rows_dropped_and_empty_files_removed: tools without a homepage
    # now get a Folder in the shared tools folder, so nothing is dropped.
    df = pd.read_excel(FIXTURES / "p1" / "tool.xlsx", sheet_name="manifest")
    df.loc[df["Tool Name"].isin(["tool0", "tool1"]), "Tool Homepage"] = None
    manifest = tmp_path / "tools.csv"
    df.to_csv(manifest, index=False)
    code, state = run(tmp_path, "ToolView", manifest=manifest)
    assert code == 0 and state["dropped_rows"] == []
    links = tmp_path / "out" / "04_links"
    assert len(pd.read_csv(links / "dropped_rows.csv")) == 0
    final = sorted((links / "final").glob("*.csv"))
    assert any("CA209923" in f.name for f in final)
    uploaded = pd.read_csv(state["stages"]["upload"]["outputs"]["paths_csv"])
    assert any("CA209923" in p for p in uploaded["File Paths"])


def test_drop_empty_link_rows_keeps_tools_but_drops_datasets(tmp_path):
    for link_col, name_col, pk in [("Tool Homepage", "Tool Name", "ToolView_id"),
                                   ("Dataset Url", "Dataset Alias", "DatasetView_id")]:
        f = tmp_path / f"CA1_{pk}.csv"
        pd.DataFrame({name_col: ["a", "b"], link_col: ["http://x", " "]}).to_csv(f, index=False)
        paths = pd.DataFrame({"File Paths": [str(f)]})
        report = []
        out = uw.drop_empty_link_rows(paths, link_col, report)
        kept = len(pd.read_csv(f))
        if link_col == "Tool Homepage":
            assert kept == 2 and report == [] and len(out) == 1
        else:
            assert kept == 1 and len(report) == 1 and len(out) == 1


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


def test_collection_constants_match_create_entity_links(monkeypatch):
    for name in ["geo_synapse", "geo_synapse.mc2_table", "geo_synapse.synapse_ops", "geo_synapse.pipeline"]:
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    for n in ["parse_accession", "find_project_for_grant"]:
        setattr(sys.modules["geo_synapse.mc2_table"], n, None)
    for n in ["create_synapse_dataset", "add_to_dataset_collection", "find_or_create_folder", "find_or_create_link"]:
        setattr(sys.modules["geo_synapse.synapse_ops"], n, None)
    sys.modules["geo_synapse.pipeline"].run_one = None
    cel = load_script("annotations/create_entity_links.py", "create_entity_links")
    utils = sys.modules["utils"]
    assert cel._CENTRAL_PROJECT_SYNID == uw.CENTRAL_PROJECT_SYNID == utils.CENTRAL_PROJECT == "syn76986881"
    assert cel._COLLECTION_SYNID == uw.COLLECTION_SYNID == utils.DATASET_COLLECTION == "syn77025587"


def test_dry_run_still_reports_reuse_without_creating(tmp_path, env):
    code, state = run(tmp_path, "DatasetView", syn=FakeSyn({"GSE150000": "syn5"}))
    assert env["links"] == [] and state["stages"]["links"]["outputs"]["reused"] == 1


def test_target_selection(tmp_path, env, monkeypatch):
    run(tmp_path)  # default recordset
    assert "PublicationView" in env["upload"][0] and "--write-log-table" in env["upload"][0]

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


def test_create_entity_links_main_takes_argv(monkeypatch):
    """Backward-compatible main(argv=None) refactor: argv parses, no argv reads sys.argv."""
    for name in ["geo_synapse", "geo_synapse.mc2_table", "geo_synapse.synapse_ops", "geo_synapse.pipeline"]:
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    sys.modules["geo_synapse.mc2_table"].parse_accession = lambda s: None
    sys.modules["geo_synapse.mc2_table"].find_project_for_grant = lambda *a: None
    for n in ["create_synapse_dataset", "add_to_dataset_collection"]:
        setattr(sys.modules["geo_synapse.synapse_ops"], n, None)
    sys.modules["geo_synapse.synapse_ops"].find_or_create_folder = lambda syn, name, parent: TOOLS_FOLDER
    sys.modules["geo_synapse.pipeline"].run_one = None
    mod = load_script("annotations/create_entity_links.py", "cel")
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
    for n in ["create_synapse_dataset", "add_to_dataset_collection"]:
        setattr(sys.modules["geo_synapse.synapse_ops"], n, None)
    sys.modules["geo_synapse.synapse_ops"].find_or_create_folder = lambda syn, name, parent: TOOLS_FOLDER
    sys.modules["geo_synapse.pipeline"].run_one = None
    return load_script("annotations/create_entity_links.py", "cel_resume")


TOOLS_FOLDER = "syn900"  # the central tools folder, as the faked find_or_create_folder returns it


class LinkSyn:
    """Fake client: findEntityId looks in `existing`; File.store records and can fail."""

    def __init__(self, existing=None, fail_on=None):
        self.existing, self.stored, self.fail_on = dict(existing or {}), [], fail_on
        self.updated, self.folders = [], []
        self.concrete, self.urls = {}, {}  # entity id -> concreteType / current external URL
        self.children = {}  # entity id -> list of child entities

    def findEntityId(self, name, parent=None):
        return self.existing.get((parent, name))

    def getChildren(self, parent):
        return iter(self.children.get(parent, []))

    def restGET(self, url):
        return {"concreteType": self.concrete.get(url.rsplit("/", 1)[-1], "org.sagebionetworks.repo.model.FileEntity")}


def link_manifest(tmp_path, names):
    split = tmp_path / "CA1_tool.csv"
    pd.DataFrame({"Tool Name": names, "Tool Homepage": [f"https://x/{n}" for n in names],
                  "ToolView_id": ""}).to_csv(split, index=False)
    pending = tmp_path / "pending_paths.csv"
    pd.DataFrame({"File Paths": [str(split)], "folderIdTools": ["syn100"]}).to_csv(pending, index=False)
    return split, pending


def patch_file(mod, monkeypatch, syn):
    class FakeFile:
        def __init__(self, external_url=None, name=None, parent_id=None, synapse_store=True, id=None,
                     download_file=True):
            self.name, self.parent_id, self.external_url, self.id = name, parent_id, external_url, id

        def get(self, synapse_client):
            self.external_url = syn.urls.get(self.id, self.external_url)
            return self

        def store(self, synapse_client):
            if self.id:  # update of an existing File
                syn.updated.append((self.id, self.external_url))
                return self
            if syn.fail_on == self.name:
                raise RuntimeError("boom")
            syn.stored.append(self.name)
            self.id = f"syn{1000 + len(syn.stored)}"
            return self

    class FakeFolder:
        def __init__(self, name=None, parent_id=None):
            self.name, self.parent_id = name, parent_id

        def store(self, synapse_client):
            syn.folders.append((self.name, self.parent_id))
            self.id = f"syn{2000 + len(syn.folders)}"
            return self

    monkeypatch.setattr(mod, "File", FakeFile)
    monkeypatch.setattr(mod, "Folder", FakeFolder)
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
    syn = LinkSyn(existing={(TOOLS_FOLDER, "a"): "syn555"})
    syn.urls["syn555"] = "https://x/a"
    patch_file(mod, monkeypatch, syn)
    mod.main(["-m", str(pending), "-t", "ToolView"])  # no --created-ids: no resume file
    assert syn.stored == ["b"]
    assert pd.read_csv(split, dtype=str)["ToolView_id"].tolist()[0] == "syn555"
    assert not (tmp_path / "created_ids.csv").exists()


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
                 "--out-dir", str(tmp_path / "out"), "--write-log-table", "syn999", "--from-stage", "upload"])
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
        uw.get_args(["--manifest", "m.csv", "--type", "ToolView", "--out-dir", "o", "--write-log-table", "l",
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


def test_removals_table_is_passed_to_upload_recordsets(tmp_path, env):
    run(tmp_path, "ToolView", "--execute")
    argv = env["upload"][-1]
    assert argv[argv.index("--removals-table") + 1] == "syn78404342"


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


def test_grant_lookup_uses_the_split_files_grant(monkeypatch, tmp_path):
    # GrantView Key can list several grants; the split file is per grant.
    mod = load_cel(monkeypatch)
    split = tmp_path / "CA184898_dataset.csv"
    pd.DataFrame({"Dataset Alias": ["GSE1"], "GrantView Key": ["CA209923, CA184898"]}).to_csv(split, index=False)
    other = tmp_path / "misc.csv"
    pd.DataFrame({"Dataset Alias": ["GSE2"], "GrantView Key": ["CA209923, CA184898"]}).to_csv(other, index=False)
    paths = tmp_path / "paths.csv"
    pd.DataFrame({"File Paths": [str(split), str(other)]}).to_csv(paths, index=False)
    lookup = mod._build_grant_lookup(str(paths))
    assert lookup[(str(split), "GSE1")] == "CA184898"
    assert lookup[(str(other), "GSE2")] == "CA209923"  # no grant prefix: first listed grant


def test_grant_lookup_skips_a_leading_comma_in_the_grant_key(monkeypatch, tmp_path):
    mod = load_cel(monkeypatch)
    other = tmp_path / "misc.csv"
    pd.DataFrame({"Dataset Alias": ["GSE2"], "GrantView Key": [", CA209923"]}).to_csv(other, index=False)
    paths = tmp_path / "paths.csv"
    pd.DataFrame({"File Paths": [str(other)]}).to_csv(paths, index=False)
    assert mod._build_grant_lookup(str(paths))[(str(other), "GSE2")] == "CA209923"


# --- exit-code mapping ------------------------------------------------------------------


def test_the_tables_default_to_the_production_ones(tmp_path):
    base = ["--manifest", "m.csv", "--type", "ToolView", "--out-dir", str(tmp_path / "o")]
    args = uw.get_args(base)
    assert (args.write_log_table, args.removals_table) == ("syn78404341", "syn78404342")
    assert uw.get_args(base + ["--execute", "--write-log-table", "syn1"]).write_log_table == "syn1"


@pytest.mark.parametrize("code,ok", [(0, True), (None, True), (1, False), (2, False), ("msg", False)])
def test_script_system_exit_is_mapped(tmp_path, env, monkeypatch, code, ok):
    def exits(argv):
        raise SystemExit(code)

    patched = uw._load
    monkeypatch.setattr(uw, "_load", lambda f: types.SimpleNamespace(main=exits) if f == "processing-splits.py"
                        else patched(f))
    result, state = run(tmp_path)
    assert result == (0 if ok else 1)  # never 2, which means "dropped rows"
    assert state["stages"]["reshape"]["status"] == ("done" if ok else "failed")


@pytest.mark.parametrize("code,status,exit_code", [(0, "done", 0), (1, "failed", 1)])
def test_upload_recordsets_nonzero_marks_upload_failed(tmp_path, env, monkeypatch, code, status, exit_code):
    real = uw._load
    monkeypatch.setattr(uw, "_load", lambda f: types.SimpleNamespace(main=lambda argv: code)
                        if f == "upload_recordsets.py" else real(f))
    result, state = run(tmp_path)
    assert result == exit_code and state["stages"]["upload"]["status"] == status



def test_store_link_same_url_file_is_reused_untouched(monkeypatch):
    mod = load_cel(monkeypatch)
    syn = LinkSyn(existing={("syn100", "a"): "syn555"})
    syn.urls["syn555"] = "https://x/a"
    patch_file(mod, monkeypatch, syn)
    assert mod.store_link(syn, "a", "https://x/a", "syn100") == "syn555"
    assert syn.updated == [] and syn.stored == []


def test_store_link_file_with_other_url_is_updated(monkeypatch):
    mod = load_cel(monkeypatch)
    syn = LinkSyn(existing={("syn100", "a"): "syn555"})
    syn.urls["syn555"] = "https://old/a"
    patch_file(mod, monkeypatch, syn)
    assert mod.store_link(syn, "a", "https://x/a", "syn100") == "syn555"
    assert syn.updated == [("syn555", "https://x/a")] and syn.stored == []


def test_store_link_non_file_entity_fails_the_row(monkeypatch):
    mod = load_cel(monkeypatch)
    syn = LinkSyn(existing={("syn100", "a"): "syn555"})
    syn.concrete["syn555"] = "org.sagebionetworks.repo.model.Folder"
    patch_file(mod, monkeypatch, syn)
    with pytest.raises(ValueError, match=r"syn555.*not a File.*Folder"):
        mod.store_link(syn, "a", "https://x/a", "syn100")
    assert syn.updated == [] and syn.stored == []


# --- create_entity_links: non-GEO dataset placeholder folders ----------------

DATASETS_FOLDER = "syn901"  # the central datasets folder (the faked folder lookup below)
FOLDER_TYPE = "org.sagebionetworks.repo.model.Folder"


def dataset_run(tmp_path, monkeypatch, syn, rows, geo_fails=False):
    """Run main on a DatasetView split; rows are (alias, url). Returns (mod, split, pending, lookups)."""
    mod = load_cel(monkeypatch)
    split = tmp_path / "CA1_dataset.csv"
    pd.DataFrame(rows, columns=["Dataset Alias", "Dataset Url"]).assign(DatasetView_id="").to_csv(split, index=False)
    pending = tmp_path / "pending_paths.csv"
    pd.DataFrame({"File Paths": [str(split)], "folderIdDatasets": ["syn100"]}).to_csv(pending, index=False)
    patch_file(mod, monkeypatch, syn)
    lookups = []

    def folder(syn, name, parent):
        lookups.append(name)
        return DATASETS_FOLDER

    monkeypatch.setattr(mod, "find_or_create_folder", folder)
    if geo_fails:
        monkeypatch.setattr(mod, "_index_geo_dataset", lambda *a, **k: (None, "boom"))
    return mod, split, pending, lookups


def test_dataset_placeholder_is_created_empty_in_given_folder(monkeypatch):
    mod = load_cel(monkeypatch)
    syn = LinkSyn()
    patch_file(mod, monkeypatch, syn)
    assert mod.store_dataset_placeholder(syn, "a_links", DATASETS_FOLDER) == "syn2001"
    assert syn.folders == [("a_links", DATASETS_FOLDER)] and syn.stored == []


def test_dataset_placeholder_same_name_folder_is_reused(monkeypatch):
    mod = load_cel(monkeypatch)
    syn = LinkSyn(existing={(DATASETS_FOLDER, "a_links"): "syn555"})
    syn.concrete["syn555"] = FOLDER_TYPE
    patch_file(mod, monkeypatch, syn)
    assert mod.store_dataset_placeholder(syn, "a_links", DATASETS_FOLDER) == "syn555"
    assert syn.folders == [] and syn.stored == [] and syn.updated == []


def test_dataset_placeholder_non_empty_same_name_folder_is_a_conflict(monkeypatch):
    mod = load_cel(monkeypatch)
    syn = LinkSyn(existing={(DATASETS_FOLDER, "a_links"): "syn555"})
    syn.concrete["syn555"] = FOLDER_TYPE
    syn.children["syn555"] = [{"id": "syn777"}]
    patch_file(mod, monkeypatch, syn)
    with pytest.raises(mod.ToolConflict, match=r"'a_links'.*syn555.*not empty"):
        mod.store_dataset_placeholder(syn, "a_links", DATASETS_FOLDER)
    assert syn.folders == [] and syn.stored == [] and syn.updated == []


def test_dataset_placeholder_same_name_file_is_a_conflict(monkeypatch):
    mod = load_cel(monkeypatch)
    syn = LinkSyn(existing={(DATASETS_FOLDER, "a_links"): "syn555"})
    patch_file(mod, monkeypatch, syn)  # restGET reports a File by default
    with pytest.raises(mod.ToolConflict, match=r"'a_links'.*syn555.*not a Folder"):
        mod.store_dataset_placeholder(syn, "a_links", DATASETS_FOLDER)
    assert syn.folders == [] and syn.stored == [] and syn.updated == []


def test_dataset_view_non_geo_row_gets_placeholder_in_central_folder(tmp_path, monkeypatch):
    syn = LinkSyn()
    mod, split, pending, lookups = dataset_run(tmp_path, monkeypatch, syn, [("a", "https://x/a")])
    assert mod.main(["-m", str(pending), "-t", "DatasetView"]) == []
    assert syn.folders == [("a_links", DATASETS_FOLDER)] and syn.stored == [] and lookups == ["datasets"]
    assert pd.read_csv(split, dtype=str)["DatasetView_id"].tolist() == ["syn2001"]


def test_dataset_view_failed_geo_row_gets_placeholder(tmp_path, monkeypatch):
    syn = LinkSyn()
    mod, split, pending, _ = dataset_run(tmp_path, monkeypatch, syn, [("GSE150000", "https://x/GSE150000")],
                                         geo_fails=True)
    monkeypatch.setattr(mod, "parse_accession", lambda text: "GSE150000")
    mod.main(["-m", str(pending), "-t", "DatasetView"])
    assert syn.folders == [("GSE150000_links", DATASETS_FOLDER)] and syn.stored == []
    assert pd.read_csv(split, dtype=str)["DatasetView_id"].tolist() == ["syn2001"]


def test_dataset_view_all_geo_run_never_looks_up_datasets_folder(tmp_path, monkeypatch):
    syn = LinkSyn()
    mod, split, pending, lookups = dataset_run(tmp_path, monkeypatch, syn, [("GSE1", "u1"), ("GSE2", "u2")])
    monkeypatch.setattr(mod, "parse_accession", lambda text: "GSE")
    monkeypatch.setattr(mod, "_index_geo_dataset", lambda *a, **k: ("syn777", None))
    mod.main(["-m", str(pending), "-t", "DatasetView"])
    assert lookups == [] and syn.folders == []
    assert pd.read_csv(split, dtype=str)["DatasetView_id"].tolist() == ["syn777", "syn777"]


def test_dataset_conflict_skips_only_that_row(tmp_path, monkeypatch):
    syn = LinkSyn(existing={(DATASETS_FOLDER, "b_links"): "syn555"})  # a File
    mod, split, pending, _ = dataset_run(
        tmp_path, monkeypatch, syn, [("a", "https://x/a"), ("b", "https://x/b"), ("c", "https://x/c")])
    ids = tmp_path / "created_ids.csv"
    conflicts = mod.main(["-m", str(pending), "-t", "DatasetView", "--created-ids", str(ids)])
    assert [f[0] for f in syn.folders] == ["a_links", "c_links"]
    assert pd.read_csv(split, dtype=str, keep_default_na=False)["DatasetView_id"].tolist() == ["syn2001", "", "syn2002"]
    assert len(pd.read_csv(ids)) == 2  # the skipped row is not recorded, so a rerun retries it
    assert [(c["name"], c["existing_id"]) for c in conflicts] == [("b_links", "syn555")]
    assert conflicts[0]["file"] == str(split)


def test_dataset_resume_returns_recorded_id(tmp_path, monkeypatch):
    syn = LinkSyn()
    mod, split, pending, _ = dataset_run(tmp_path, monkeypatch, syn, [("a", "https://x/a")])
    ids = tmp_path / "created_ids.csv"
    argv = ["-m", str(pending), "-t", "DatasetView", "--created-ids", str(ids)]
    mod.main(argv)
    pd.DataFrame({"File Paths": [str(split)], "folderIdDatasets": ["syn100"]}).to_csv(pending, index=False)
    mod.main(argv)
    assert syn.folders == [("a_links", DATASETS_FOLDER)]  # second run stores nothing
    assert pd.read_csv(split, dtype=str)["DatasetView_id"].tolist() == ["syn2001"]


def test_workflow_keeps_conflicting_dataset_rows_out_of_the_upload(tmp_path, env, monkeypatch):
    real = uw._load

    def fake_links(argv):
        pending = pd.read_csv(argv[argv.index("-m") + 1])
        for path in pending["File Paths"]:
            df = pd.read_csv(path, dtype=str, keep_default_na=False)
            df["DatasetView_id"] = [f"syn{i + 1}" for i in range(len(df))]
            df.loc[df["Dataset Alias"] == df["Dataset Alias"].iloc[0], "DatasetView_id"] = ""  # skipped
            df.to_csv(path, index=False)
        return [{"file": pending["File Paths"][0], "name": "x_links", "homepage": "", "existing_id": "syn9",
                 "existing_type": "File", "existing_url": None, "message": "conflict"}]

    monkeypatch.setattr(uw, "_load", lambda f: types.SimpleNamespace(main=fake_links)
                        if f == "create_entity_links.py" else real(f))
    code, state = run(tmp_path, "DatasetView", "--execute")
    assert code == 2
    assert state["dropped_rows"] and all(r["reason"] == "dataset conflict" for r in state["dropped_rows"])
    links = tmp_path / "out" / "04_links"
    assert pd.read_csv(links / "dataset_conflicts.csv")["existing_id"].tolist() == ["syn9"]
    assert not (links / "tool_conflicts.csv").exists()


def test_dataset_view_synapse_url_uses_that_id_without_new_entity(tmp_path, monkeypatch):
    syn = LinkSyn()
    urls = ["https://www.synapse.org/Synapse:syn123", "https://www.synapse.org/#!Synapse:syn456",
            "https://www.synapse.org/Synapse:syn789/datasets/"]
    mod, split, pending, lookups = dataset_run(tmp_path, monkeypatch, syn, [(f"s{i}", u) for i, u in enumerate(urls)])
    ids = tmp_path / "created_ids.csv"
    assert mod.main(["-m", str(pending), "-t", "DatasetView", "--created-ids", str(ids)]) == []
    assert syn.folders == [] and syn.stored == [] and lookups == []
    assert pd.read_csv(split, dtype=str)["DatasetView_id"].tolist() == ["syn123", "syn456", "syn789"]
    assert len(pd.read_csv(ids)) == 3


def test_dataset_view_synapse_url_without_id_is_a_conflict(tmp_path, monkeypatch):
    syn = LinkSyn()
    mod, split, pending, _ = dataset_run(
        tmp_path, monkeypatch, syn, [("a", "https://www.synapse.org/"), ("b", "https://x/b")])
    conflicts = mod.main(["-m", str(pending), "-t", "DatasetView"])
    assert [c["name"] for c in conflicts] == ["a"] and "no Synapse ID" in conflicts[0]["message"]
    assert [f[0] for f in syn.folders] == ["b_links"]
    assert pd.read_csv(split, dtype=str, keep_default_na=False)["DatasetView_id"].tolist() == ["", "syn2001"]


def test_dataset_view_alias_with_different_urls_across_files_skips_all_its_rows(tmp_path, monkeypatch):
    syn = LinkSyn()
    mod, split1, pending, _ = dataset_run(tmp_path, monkeypatch, syn, [("a", "https://x/a1"), ("b", "https://x/b")])
    split2 = tmp_path / "CA2_dataset.csv"
    pd.DataFrame([("a", " https://x/a2 "), ("b", "https://x/b ")], columns=["Dataset Alias", "Dataset Url"]
                 ).assign(DatasetView_id="").to_csv(split2, index=False)
    pd.DataFrame({"File Paths": [str(split1), str(split2)], "folderIdDatasets": ["syn100", "syn101"]}
                 ).to_csv(pending, index=False)
    conflicts = mod.main(["-m", str(pending), "-t", "DatasetView"])
    assert [f[0] for f in syn.folders] == ["b_links", "b_links"]  # b: equal after strip, so not a mismatch
    assert len(conflicts) == 2 and {c["file"] for c in conflicts} == {str(split1), str(split2)}
    for c in conflicts:
        assert all(t in c["message"] for t in ["'a'", "CA1_dataset.csv", "CA2_dataset.csv", "https://x/a1",
                                               "https://x/a2"])
    ids = lambda f: pd.read_csv(f, dtype=str, keep_default_na=False)["DatasetView_id"].tolist()
    assert ids(split1) == ["", "syn2001"] and ids(split2) == ["", "syn2002"]


def test_workflow_drops_rows_of_a_mismatched_alias(tmp_path, env, monkeypatch):
    real = uw._load

    def fake_links(argv):
        pending = pd.read_csv(argv[argv.index("-m") + 1])
        for path in pending["File Paths"]:
            df = pd.read_csv(path, dtype=str, keep_default_na=False)
            df["DatasetView_id"] = [f"syn{i + 1}" for i in range(len(df))]
            df.loc[df["Dataset Alias"] == df["Dataset Alias"].iloc[0], "DatasetView_id"] = ""  # skipped
            df.to_csv(path, index=False)
        return [{"file": pending["File Paths"][0], "name": "x", "homepage": "u", "existing_id": "",
                 "existing_type": "", "existing_url": None, "message": "different Dataset Url values"}]

    monkeypatch.setattr(uw, "_load", lambda f: types.SimpleNamespace(main=fake_links)
                        if f == "create_entity_links.py" else real(f))
    code, state = run(tmp_path, "DatasetView", "--execute")
    assert code == 2 and state["dropped_rows"]
    assert pd.read_csv(tmp_path / "out" / "04_links" / "dataset_conflicts.csv")["message"].tolist() == [
        "different Dataset Url values"]


def tool_link_run(tmp_path, monkeypatch, syn, rows):
    mod = load_cel(monkeypatch)
    split = tmp_path / "CA1_tool.csv"
    pd.DataFrame(rows, columns=["Tool Name", "Tool Homepage"]).assign(ToolView_id="").to_csv(split, index=False)
    pending = tmp_path / "pending_paths.csv"
    pd.DataFrame({"File Paths": [str(split)], "folderIdTools": ["syn100"]}).to_csv(pending, index=False)
    patch_file(mod, monkeypatch, syn)
    return mod, split, pending


def test_tool_with_homepage_creates_file_in_central_tools_folder(tmp_path, monkeypatch):
    syn = LinkSyn()
    mod, split, pending = tool_link_run(tmp_path, monkeypatch, syn, [("a", "https://x/a")])
    mod.main(["-m", str(pending), "-t", "ToolView"])
    assert syn.stored == ["a"] and syn.folders == []
    assert pd.read_csv(split, dtype=str)["ToolView_id"].tolist() == ["syn1001"]


def test_tool_file_is_created_under_tools_folder_not_grant_folder(tmp_path, monkeypatch):
    syn = LinkSyn()
    mod, _, _ = tool_link_run(tmp_path, monkeypatch, syn, [("a", "https://x/a")])
    seen = []
    real = mod.File

    def spy(**kw):
        seen.append(kw.get("parent_id"))
        return real(**kw)

    monkeypatch.setattr(mod, "File", spy)
    mod.store_tool_entity(syn, "a", "https://x/a", TOOLS_FOLDER)
    assert seen == [TOOLS_FOLDER]


def test_tool_same_name_and_url_is_reused(tmp_path, monkeypatch):
    syn = LinkSyn(existing={(TOOLS_FOLDER, "a"): "syn555"})
    syn.urls["syn555"] = "https://x/a"
    mod, split, pending = tool_link_run(tmp_path, monkeypatch, syn, [("a", "https://x/a")])
    mod.main(["-m", str(pending), "-t", "ToolView"])
    assert syn.stored == [] and syn.updated == []
    assert pd.read_csv(split, dtype=str)["ToolView_id"].tolist() == ["syn555"]


def test_tool_same_name_other_url_is_flagged_not_overwritten(tmp_path, monkeypatch):
    syn = LinkSyn(existing={(TOOLS_FOLDER, "a"): "syn555"})
    syn.urls["syn555"] = "https://old/a"
    mod, split, pending = tool_link_run(tmp_path, monkeypatch, syn, [("a", "https://x/a")])
    with pytest.raises(mod.ToolConflict, match=r"'a'.*syn555.*https://old/a.*https://x/a"):
        mod.store_tool_entity(syn, "a", "https://x/a", TOOLS_FOLDER)
    assert syn.stored == [] and syn.updated == []


def test_tool_conflict_skips_only_that_row(tmp_path, monkeypatch):
    syn = LinkSyn(existing={(TOOLS_FOLDER, "b"): "syn555"})
    syn.urls["syn555"] = "https://old/b"
    mod, split, pending = tool_link_run(
        tmp_path, monkeypatch, syn, [("a", "https://x/a"), ("b", "https://x/b"), ("c", "https://x/c")])
    ids = tmp_path / "created_ids.csv"
    conflicts = mod.main(["-m", str(pending), "-t", "ToolView", "--created-ids", str(ids)])
    assert syn.stored == ["a", "c"] and syn.updated == []
    # the skipped row stays blank and does not shift the IDs of the rows after it
    assert pd.read_csv(split, dtype=str, keep_default_na=False)["ToolView_id"].tolist() == ["syn1001", "", "syn1002"]
    assert len(pd.read_csv(ids)) == 2  # not recorded, so a rerun retries it
    assert [(c["name"], c["existing_id"], c["existing_url"], c["homepage"]) for c in conflicts] == \
        [("b", "syn555", "https://old/b", "https://x/b")]
    assert conflicts[0]["file"] == str(split) and "https://old/b" in conflicts[0]["message"]


def test_non_conflict_error_still_stops_after_a_conflict(tmp_path, monkeypatch):
    syn = LinkSyn(existing={(TOOLS_FOLDER, "a"): "syn555"}, fail_on="c")
    syn.urls["syn555"] = "https://old/a"
    mod, split, pending = tool_link_run(
        tmp_path, monkeypatch, syn, [("a", "https://x/a"), ("b", "https://x/b"), ("c", "https://x/c")])
    with pytest.raises(RuntimeError, match="boom"):
        mod.main(["-m", str(pending), "-t", "ToolView"])
    assert syn.stored == ["b"]


def test_workflow_keeps_conflicting_tool_rows_out_of_the_upload(tmp_path, env, monkeypatch):
    real = uw._load
    seen = {}

    def fake_links(argv):
        pending = pd.read_csv(argv[argv.index("-m") + 1])
        for path in pending["File Paths"]:
            df = pd.read_csv(path, dtype=str, keep_default_na=False)
            df["ToolView_id"] = [f"syn{i + 1}" for i in range(len(df))]
            df.loc[df["Tool Name"] == "tool0", "ToolView_id"] = ""  # skipped: conflict
            df.to_csv(path, index=False)
        return [{"file": pending["File Paths"][0], "name": "tool0", "homepage": "http://x", "existing_id": "syn9",
                 "existing_type": "Folder", "existing_url": None, "message": "conflict"}]

    monkeypatch.setattr(uw, "_load", lambda f: types.SimpleNamespace(main=fake_links)
                        if f == "create_entity_links.py" else real(f))
    code, state = run(tmp_path, "ToolView", "--execute")
    assert code == 2  # finished with dropped rows
    assert [r["name"] for r in state["dropped_rows"]] == ["tool0"]
    assert state["dropped_rows"][0]["reason"] == "tool conflict"
    links = tmp_path / "out" / "04_links"
    assert pd.read_csv(links / "tool_conflicts.csv")["existing_id"].tolist() == ["syn9"]
    uploaded = pd.concat(pd.read_csv(p, dtype=str, keep_default_na=False)
                         for p in pd.read_csv(state["stages"]["links"]["outputs"]["paths_csv"])["File Paths"])
    assert "tool0" not in uploaded["Tool Name"].tolist() and (uploaded["ToolView_id"] != "").all()


def test_tool_without_homepage_gets_a_folder_or_reuses_one(tmp_path, monkeypatch):
    syn = LinkSyn(existing={(TOOLS_FOLDER, "b"): "syn556"})
    syn.concrete["syn556"] = "org.sagebionetworks.repo.model.Folder"
    mod, split, pending = tool_link_run(tmp_path, monkeypatch, syn, [("a", " "), ("b", "")])
    mod.main(["-m", str(pending), "-t", "ToolView"])
    assert syn.folders == [("a", TOOLS_FOLDER)] and syn.stored == []
    assert pd.read_csv(split, dtype=str)["ToolView_id"].tolist() == ["syn2001", "syn556"]


def test_tool_without_homepage_but_existing_file_is_flagged(tmp_path, monkeypatch):
    syn = LinkSyn(existing={(TOOLS_FOLDER, "a"): "syn555"})
    syn.urls["syn555"] = "https://x/a"
    mod, _, pending = tool_link_run(tmp_path, monkeypatch, syn, [("a", "")])
    conflicts = mod.main(["-m", str(pending), "-t", "ToolView"])
    assert [c["existing_id"] for c in conflicts] == ["syn555"]
    assert syn.folders == []


def test_get_names_reads_text_literally(tmp_path, monkeypatch):
    mod = load_cel(monkeypatch)
    split = tmp_path / "CA1_tool.csv"
    split.write_text("Tool Name,Tool Homepage\nNA,http://x/1\nnull,\n007,http://x/3\n")
    pending = tmp_path / "p.csv"
    pd.DataFrame({"File Paths": [str(split)], "folderIdTools": ["syn100"]}).to_csv(pending, index=False)
    got = mod.get_names("Tool Name", "folderIdTools", "Tool Homepage", manifest=str(pending))
    assert [(n, l) for _, n, l, _ in got] == [("NA", "http://x/1"), ("null", ""), ("007", "http://x/3")]
    got = mod.get_names("Tool Name", "folderIdTools", "Tool Homepage", data=str(split))
    assert [n for _, n, _, _ in got] == ["NA", "null", "007"]


def test_resuming_upload_keeps_earlier_run_report_dirs(tmp_path, env):
    run(tmp_path, "ToolView", "--run-id", "run-1")
    up = tmp_path / "out" / "05_upload"
    for name in ("run-1", "run-2"):
        (up / name).mkdir(exist_ok=True)
        (up / name / "decisions.csv").write_text("x")
    run(tmp_path, "ToolView", "--from-stage", "upload", "--run-id", "run-2")
    assert (up / "run-1" / "decisions.csv").exists()  # earlier run kept
    assert not (up / "run-2" / "decisions.csv").exists()  # current run ID cleared


def test_fresh_upload_stage_still_starts_empty(tmp_path, env):
    run(tmp_path, "ToolView", "--run-id", "run-1")
    old = tmp_path / "out" / "05_upload" / "run-0"
    old.mkdir()
    run(tmp_path, "ToolView", "--from-stage", "links", "--run-id", "run-1")
    assert not old.exists()
