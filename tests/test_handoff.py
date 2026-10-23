"""D8: hand-off from the AI curation pipeline.

Runs split_manifest_grants -> gen-mp-csv -> processing-splits offline over two
fixture sets: today's pipeline output (legacy) and the P1 output (14.0.0
headers). gen-mp-csv's Synapse query is replaced with a canned grant/folder
table; everything else in it runs as written.
"""

import importlib.util
import logging
import shutil
from pathlib import Path

import pandas as pd
import pytest

import model_contract

ANNOTATIONS = Path(__file__).resolve().parent.parent / "annotations"
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "handoff"
CV = FIXTURES / "all_valid_values.csv"

BOOKKEEPING = [
    "_download_status", "_download_source", "_pdf_path", "_parse_error",
    "_tool_availability_text", "keywords",
]

# fixture name -> (component, split manifest_type, gen-mp-csv data_type, suffix)
COMPONENTS = {
    "publication": ("PublicationView", "publication", "publications", ".csv"),
    "dataset": ("DatasetView", "dataset", "datasets", ".csv"),
    "tool": ("ToolView", "tool", "tools", ".xlsx"),
}
GRANTS = ["CA209997", "CA209923", "CA184898"]


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, ANNOTATIONS / filename)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def split_mod():
    return load("split_manifest_grants", "split_manifest_grants.py")


@pytest.fixture(scope="module")
def genmp_mod():
    return load("gen_mp_csv", "gen-mp-csv.py")


@pytest.fixture(scope="module")
def proc_mod():
    return load("processing_splits", "processing-splits.py")


def run_workflow(split_mod, genmp_mod, proc_mod, monkeypatch, tmp_path, source, data_type, manifest_type):
    out = tmp_path / "splits"
    split_mod.main([str(source), manifest_type, str(out), "--csv", "--cv-file", str(CV)])

    folder_col = {
        "publications": "folderIdPublication",
        "datasets": "folderIdDatasets",
        "tools": "folderIdTools",
    }[data_type]

    def fake_query(ref_path, data_type, folder_id_column_name, grant_id_column_name):
        ref = pd.DataFrame(
            {
                "grantNumber": GRANTS,
                grant_id_column_name: [f"syn{i}" for i in range(3)],
                folder_id_column_name: [f"syn10{i}" for i in range(3)],
            }
        )
        ref.to_csv(ref_path, index=False)
        return ref

    monkeypatch.setattr(genmp_mod, "query_synapse_for_folder_info", fake_query)
    paths_csv = tmp_path / "paths.csv"
    genmp_mod.main(str(out), str(paths_csv), data_type)
    proc_mod.main([str(paths_csv)])
    return pd.read_csv(paths_csv)["File Paths"].tolist()


def fixture_path(kind, name):
    component, mtype, dtype, suffix = COMPONENTS[name]
    return FIXTURES / kind / f"{name}{suffix}"


@pytest.mark.parametrize("name", list(COMPONENTS))
def test_fixtures_have_expected_shape(name):
    """P1 fixtures equal the template; legacy ones do not."""
    component = COMPONENTS[name][0]
    p1 = pd.read_csv(fixture_path("p1", name)) if name != "tool" else pd.read_excel(fixture_path("p1", name), sheet_name="manifest")
    assert list(p1.columns) == model_contract.template_headers(component)
    legacy = pd.read_csv(fixture_path("legacy", name)) if name != "tool" else pd.read_excel(fixture_path("legacy", name), sheet_name="Sheet1")
    assert list(legacy.columns) != model_contract.template_headers(component)


@pytest.mark.parametrize("kind", ["legacy", "p1"])
@pytest.mark.parametrize("name", list(COMPONENTS))
def test_headers_match_template(split_mod, genmp_mod, proc_mod, monkeypatch, tmp_path, caplog, kind, name):
    component, mtype, dtype, _ = COMPONENTS[name]
    with caplog.at_level(logging.INFO):
        files = run_workflow(split_mod, genmp_mod, proc_mod, monkeypatch, tmp_path,
                             fixture_path(kind, name), dtype, mtype)
    assert len(files) == 3
    expected = model_contract.template_headers(component)
    for f in files:
        df = pd.read_csv(f)
        assert list(df.columns) == expected
        grant = Path(f).name.split("_")[0]
        assert set(df["GrantView Key"]) == {grant}

    log = caplog.text
    if kind == "legacy":
        if name == "publication":
            for col in BOOKKEEPING + ["Publication Theme Name", "Publication Consortium Name"]:
                assert f"'{col}'" in log, col
            assert "legacy 'Publication Grant Number'" in log
            assert "Renaming legacy column 'Publication Assay' to 'Assay'" in log
        elif name == "dataset":
            assert "Renaming legacy column 'Data Use Codes' to 'Dataset Data Use Codes'" in log
        else:
            assert "Renaming legacy column 'Tool License' to 'License'" in log
    else:
        assert "Renaming" not in log
        assert "Dropping" not in log
        assert "Deprecated" not in log


def test_multi_grant_row_lands_in_each_grant(split_mod, genmp_mod, proc_mod, monkeypatch, tmp_path):
    files = run_workflow(split_mod, genmp_mod, proc_mod, monkeypatch, tmp_path,
                         fixture_path("p1", "publication"), "publications", "publication")
    by_grant = {Path(f).name.split("_")[0]: pd.read_csv(f) for f in files}
    assert 31000002 in set(by_grant["CA209923"]["Pubmed Id"])
    assert 31000002 in set(by_grant["CA184898"]["Pubmed Id"])
    assert (by_grant["CA184898"]["PublicationView_id"] == by_grant["CA184898"]["Pubmed Id"]).all()


@pytest.mark.parametrize("kind", ["legacy", "p1"])
def test_cap_truncates_text_but_not_keys(split_mod, genmp_mod, proc_mod, monkeypatch, tmp_path, kind):
    pub = run_workflow(split_mod, genmp_mod, proc_mod, monkeypatch, tmp_path / "pub",
                       fixture_path(kind, "publication"), "publications", "publication")
    abstracts = pd.concat(pd.read_csv(f) for f in pub)["Publication Abstract"]
    long = abstracts[abstracts.str.endswith("(Read more on Pubmed)")]
    assert len(long) >= 1 and (long.str.len() == 400 + len("(Read more on Pubmed)")).all()

    ds = run_workflow(split_mod, genmp_mod, proc_mod, monkeypatch, tmp_path / "ds",
                      fixture_path(kind, "dataset"), "datasets", "dataset")
    df = pd.concat(pd.read_csv(f) for f in ds)
    assert df["PublicationView Key"].str.len().max() > 500
    assert not df["PublicationView Key"].str.contains("Read more").any()
    assert df["Dataset Description"].str.contains("Read more on Pubmed").any()

    tl = run_workflow(split_mod, genmp_mod, proc_mod, monkeypatch, tmp_path / "tl",
                      fixture_path(kind, "tool"), "tools", "tool")
    df = pd.concat(pd.read_csv(f) for f in tl)
    assert df["DatasetView Key"].str.len().max() > 500
    assert not df["DatasetView Key"].dropna().str.contains("Read more").any()


def test_split_reads_both_xlsx_sheet_names(split_mod):
    assert len(split_mod.read_manifest(fixture_path("legacy", "tool"))) == 4
    assert len(split_mod.read_manifest(fixture_path("p1", "tool"))) == 4


def test_split_legacy_grant_column_warns(split_mod, caplog):
    df = pd.read_csv(fixture_path("legacy", "publication"))
    with caplog.at_level(logging.WARNING):
        split_mod.split_manifest(df, "publication", False)
    assert "Deprecated" in caplog.text


def test_educational_resource_legacy_grant_and_license(proc_mod, caplog):
    headers = model_contract.template_headers("EducationalResource")
    legacy = ["Resource Grant Number" if h == "GrantView Key" else "Resource License" if h == "License" else h for h in headers]
    df = pd.DataFrame([["EducationalResource"] + ["x"] * (len(legacy) - 1)], columns=legacy)
    with caplog.at_level(logging.INFO):
        out = proc_mod.process_df(df)
    assert list(out.columns) == headers
    assert "Resource Grant Number" in caplog.text and "Resource License" in caplog.text
