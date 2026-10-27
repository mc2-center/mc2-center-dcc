"""Offline tests for union_qc.py (I6): regeneration, jsonschema validation, corrections."""

import inspect
import subprocess
from unittest import mock

import pandas as pd
import pytest

import model_contract
import union_qc as uq
import utils

JOIN = '""","".join"'  # the CSV spelling of the legacy ",".join
MAPPING = "\n".join([
    "component,attribute,mapping,tag,filter",
    "DatasetView,Dataset Alias,first,aliasColumn,all",
    "DatasetView,Component,first,componentColumn,",
    f"DatasetView,DatasetView_id,{JOIN},idColumn,all",
    f"DatasetView,GrantView Key,{JOIN},grantColumn,",
    f"DatasetView,PublicationView Key,{JOIN},pubsColumn,",
    f"DatasetView,Data Use Codes,{JOIN},,",
]) + "\n"


def _row(i, alias, **over):
    row = {
        "DatasetViewId": i,
        "GrantViewKey": "CA100001",
        "StudyKey": "",
        "PublicationViewKey": "11111111",
        "DatasetName": f"Name {alias}",
        "DatasetAlias": alias,
        "DatasetDescription": "d",
        "DatasetDesign": "d",
        "Assay": '["RNA Sequencing", "ATAC-Seq"]',
        "Species": '["Human"]',
        "TumorType": '["Breast Carcinoma"]',
        "Tissue": '["Breast"]',
        "DatasetUrl": f"https://www.synapse.org/Synapse:{i}",
        "DatasetDoi": f"https://doi.org/10.7303/{i}",
        "DatasetFileFormats": '["CSV"]',
        "DatasetDataUseCodes": '["DUO:0000042", "DUO:0000011"]',
    }
    row.update(over)
    return row


def _write_grants(tmp_path, grants):
    """``<tmp>/DatasetView/<grant>/converted.csv`` as the I5a dry run writes them."""
    for grant, rows in grants.items():
        d = tmp_path / "DatasetView" / grant
        d.mkdir(parents=True)
        pd.DataFrame(rows).to_csv(d / "converted.csv", index=False)
    return tmp_path


@pytest.fixture
def mapping_csv(tmp_path):
    p = tmp_path / "mapping.csv"
    p.write_text(MAPPING)
    return str(p)


def _regenerate(tmp_path, mapping_csv, grants, **kw):
    root = _write_grants(tmp_path / "in", grants)
    args = uq.get_args([
        "--regenerate", "--components", "DatasetView", "--input-dir", str(root),
        "--output-dir", str(tmp_path / "out"), "--mapping", mapping_csv,
        "--carry-columns", *kw.pop("carry", []),
        *kw.pop("extra", []),
    ])
    with mock.patch.object(subprocess, "run", side_effect=AssertionError("subprocess must not run")):
        code = uq.regenerate_main(args)
    manifest = pd.read_csv(tmp_path / "out" / "DatasetView" / "DatasetView_merged_manifest.csv", dtype=str, keep_default_na=False)
    return code, manifest, tmp_path / "out" / "DatasetView"


def test_no_schematic_subprocess():
    assert not hasattr(uq, "subprocess")
    assert not hasattr(uq, "trim_tables") and not hasattr(uq, "parse_out")
    with pytest.raises(SystemExit):  # the schematic config argument is gone
        uq.get_args(["-c", "config.yml"])
    with pytest.raises(SystemExit):  # so is trimming
        uq.get_args(["-t"])


def test_headers_are_display_names_in_template_order(tmp_path, mapping_csv):
    _, manifest, _ = _regenerate(tmp_path, mapping_csv, {"CA100001": [_row("syn1", "A")]})
    assert list(manifest.columns) == model_contract.template_headers("DatasetView")
    assert "Dataset Data Use Codes" in manifest.columns and "DatasetViewId" not in manifest.columns


def test_list_cells_parse_to_the_same_items_in_the_sync(tmp_path, mapping_csv):
    _, manifest, _ = _regenerate(tmp_path, mapping_csv, {"CA100001": [_row("syn1", "A")]})
    cell = manifest["Assay"].iat[0]
    assert cell == "RNA Sequencing, ATAC-Seq"
    assert utils.convert_to_stringlist(pd.Series([cell])).iat[0] == ["RNA Sequencing", "ATAC-Seq"]
    codes = manifest["Dataset Data Use Codes"].iat[0]
    assert [c.strip() for c in codes.split(",")] == ["DUO:0000042", "DUO:0000011"]
    assert manifest["PublicationView Key"].iat[0] == "11111111"


def test_rows_with_the_same_alias_merge_keys_and_keep_first_values(tmp_path, mapping_csv):
    grants = {
        "CA100001": [_row("syn1", "A", DatasetName="first")],
        "CA100002": [_row("syn1", "A", GrantViewKey="CA100002", DatasetName="second", PublicationViewKey="22222222")],
    }
    _, manifest, _ = _regenerate(tmp_path, mapping_csv, grants)
    assert len(manifest) == 1
    row = manifest.iloc[0]
    assert row["Dataset Name"] == "first"
    assert row["GrantView Key"] == "CA100001,CA100002"
    assert row["DatasetView_id"] == "syn1"  # identical ids are not repeated
    assert row["PublicationView Key"] == "11111111,22222222"


def test_invalid_rows_are_kept_and_reported(tmp_path, mapping_csv):
    grants = {"CA100001": [_row("syn1", "A"), _row("syn2", "B", Assay='["Not An Assay"]', DatasetDoi="")]}
    code, manifest, out = _regenerate(tmp_path, mapping_csv, grants)
    assert code == 2
    assert manifest["DatasetView_id"].tolist() == ["syn1", "syn2"]  # nothing trimmed
    report = pd.read_csv(out / "validation.csv")
    assert list(report.columns) == ["key", "field", "validator", "message"]
    assert set(report["key"]) == {"syn2"}
    assert {"Assay", "DatasetDoi"} <= set(report["field"])
    assert "enum" in set(report["validator"])
    assert (out / "validation_counts.csv").exists()


def test_valid_manifest_has_an_empty_report(tmp_path, mapping_csv):
    code, _, out = _regenerate(tmp_path, mapping_csv, {"CA100001": [_row("syn1", "A")]})
    assert code == 0
    assert len(pd.read_csv(out / "validation.csv")) == 0


def _fixes(tmp_path, rows):
    p = tmp_path / "pmid.csv"
    pd.DataFrame(rows, columns=["source_id", "row_key", "column", "current_value", "proposed_value", "basis", "needs_curator"]).to_csv(p, index=False)
    return str(p)


def test_mangled_publication_key_is_corrected(tmp_path, mapping_csv):
    mangled = "3,811,083,638,110,830"
    fixes = _fixes(tmp_path, [
        ["syn53478774", "syn1,syn1", "PublicationViewKey", mangled, "38110836", "x", False],
        ["syn53478774", "syn9", "PublicationViewKey", "1,2,3", "1", "x", False],
        ["syn53478774", "syn2", "PublicationViewKey", "9,9,9", "9", "x", False],
        ["syn53478774", "syn3", "PublicationViewKey", "7,7,7", "7", "x", False],
    ])
    grants = {"CA100001": [
        _row("syn1", "A", PublicationViewKey=mangled),
        _row("syn2", "B", PublicationViewKey="9"),  # already clean
        _row("syn3", "C", PublicationViewKey="123"),  # something else entirely
    ]}
    _, manifest, out = _regenerate(tmp_path, mapping_csv, grants, extra=["--pmid-keys", fixes])
    assert manifest.set_index("DatasetView_id")["PublicationView Key"].to_dict() == {
        "syn1": "38110836", "syn2": "9", "syn3": "123"}
    status = pd.read_csv(out / "key_corrections.csv", dtype=str).set_index("key")["status"].to_dict()
    assert status == {"syn1,syn1": "applied", "syn9": "not_found", "syn2": "already_correct", "syn3": "cell_differs"}


def test_icontags_carried_by_alias_from_the_reference(tmp_path, mapping_csv):
    ref = tmp_path / "ref.csv"
    pd.DataFrame({"DatasetAlias": ["A"], "iconTags": ["expressionProfiling"]}).to_csv(ref, index=False)
    grants = {"CA100001": [_row("syn1", "A"), _row("syn2", "B")]}
    _, manifest, _ = _regenerate(tmp_path, mapping_csv, grants, carry=["iconTags"], extra=["--reference-csv", str(ref)])
    assert manifest["iconTags"].tolist() == ["expressionProfiling", ""]


def test_upload_is_refused_with_a_local_input_dir(tmp_path, mapping_csv):
    root = _write_grants(tmp_path / "in", {"CA100001": [_row("syn1", "A")]})
    args = uq.get_args(["--regenerate", "--input-dir", str(root), "--upload", "--mapping", mapping_csv])
    with pytest.raises(SystemExit):
        uq.regenerate_main(args)


def test_dry_run_never_uploads(tmp_path, mapping_csv):
    with mock.patch.object(uq, "upload_manifest") as upload:
        _regenerate(tmp_path, mapping_csv, {"CA100001": [_row("syn1", "A")]})
    upload.assert_not_called()


def test_upload_stores_a_new_version_of_the_manifest_file():
    entity = mock.MagicMock()
    entity.get.return_value = entity
    entity.store.return_value = mock.MagicMock(id="syn53478774", version_number=9)
    with mock.patch.object(uq, "File", return_value=entity) as file_cls:
        out = uq.upload_manifest(mock.MagicMock(), "DatasetView", "/tmp/x.csv", "why")
    assert file_cls.call_args.kwargs["id"] == "syn53478774"
    assert entity.path == "/tmp/x.csv" and entity.version_comment == "why"
    assert out == "syn53478774 version 9"


def test_empty_curated_cells_are_filled_like_merge_and_correct(tmp_path, mapping_csv):
    _, manifest, _ = _regenerate(tmp_path, mapping_csv, {"CA100001": [_row("syn1", "A", Tissue="", PublicationViewKey="")]})
    assert manifest["Tissue"].iat[0] == "Pending Annotation"
    assert manifest["PublicationView Key"].iat[0] == "Pending Annotation"


def test_no_fill_keeps_empty_cells(tmp_path, mapping_csv):
    _, manifest, _ = _regenerate(tmp_path, mapping_csv, {"CA100001": [_row("syn1", "A", Tissue="")]}, extra=["--no-fill"])
    assert manifest["Tissue"].iat[0] == ""


def test_license_list_is_comma_joined_text_in_the_manifest(tmp_path):
    """D-Q51: portal license stays STRING, so the manifest cell is plain comma-joined text."""
    import merge_tables

    d = tmp_path / "ToolView" / "CA100001"
    d.mkdir(parents=True)
    pd.DataFrame([{"ToolViewId": "t1", "ToolName": "A", "License": '["MIT", "Apache-2.0"]'}]).to_csv(d / "converted.csv", index=False)
    union, _ = merge_tables.build_union_from_dir("ToolView", str(tmp_path))
    assert union["License"].iat[0] == "MIT, Apache-2.0"


def test_same_alias_different_ids_is_a_conflict_worklist(tmp_path, mapping_csv):
    grants = {
        "CA100001": [_row("syn1", "A"), _row("syn5", "B")],
        "CA100002": [_row("syn2", "A", GrantViewKey="CA100002"), _row("syn5", "B", GrantViewKey="CA100002")],
    }
    _, manifest, out = _regenerate(tmp_path, mapping_csv, grants)
    conflicts = pd.read_csv(out / "id_conflicts.csv", dtype=str, keep_default_na=False)
    assert conflicts.to_dict("records") == [
        {"alias": "A", "ids": "syn1,syn2", "grants": "CA100001,CA100002", "rows": "2"}]  # B agrees: not listed
    assert "id conflicts (same alias, different ids; --upload is refused while any): 1" in (out / "report.txt").read_text()


def test_upload_is_refused_while_id_conflicts_exist(tmp_path, mapping_csv):
    import merge_tables
    root = _write_grants(tmp_path / "in", {"CA100001": [_row("syn1", "A")],
                                           "CA100002": [_row("syn2", "A", GrantViewKey="CA100002")]})
    union, stats = merge_tables.build_union_from_dir("DatasetView", str(root))
    args = uq.get_args(["--regenerate", "--components", "DatasetView", "--grant-projects", "syn1",
                        "--output-dir", str(tmp_path / "out"), "--mapping", mapping_csv, "--upload"])
    args.carry_columns = []
    with mock.patch.object(merge_tables, "build_union", return_value=(union, stats)), \
            mock.patch.object(utils, "syn_login", return_value=mock.MagicMock()), \
            mock.patch.object(uq, "upload_manifest") as upload:
        with pytest.raises(SystemExit, match="id_conflicts"):
            uq.regenerate_main(args)
    upload.assert_not_called()
