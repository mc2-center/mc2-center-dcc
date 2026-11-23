"""D4: sync_datasets transform, offline, on 14.0.0 and 13.1.0 header fixtures."""

import logging

import pandas as pd
import pytest

import crosswalk
import model_contract
import sync_datasets
import utils

REAL_IDENTIFY = utils.identify_download_type

MANIFEST_14 = [
    {
        "Component": "DatasetView",
        "DatasetView_id": "syn111,syn112",
        "GrantView Key": "CA1,Affiliated/Non-Grant Associated",
        "PublicationView Key": "123",
        "Dataset Name": "Set A",
        "Dataset Alias": "setA",
        "Dataset Description": "desc",
        "Dataset Design": "design",
        "Assay": "RNA-seq,ATAC-seq",
        "Species": "Human",
        "Tumor Type": "Breast Cancer",
        "Tissue": "Breast",
        "Dataset Url": "",
        "Dataset Doi": "",
        "Dataset File Formats": "csv",
        "Dataset Data Use Codes": "DUO:0000042,RTN",
        "iconTags": "RNA-seq",
    },
    {
        "Component": "DatasetView",
        "DatasetView_id": "syn222",
        "GrantView Key": "CA1",
        "PublicationView Key": "",
        "Dataset Name": "Set B",
        "Dataset Alias": "setB",
        "Dataset Description": "desc",
        "Dataset Design": "design",
        "Assay": "Pending Annotation",
        "Species": "Pending Annotation",
        "Tumor Type": "Pending Annotation",
        "Tissue": "Pending Annotation",
        "Dataset Url": "",
        "Dataset Doi": "10.1/x",
        "Dataset File Formats": "tsv",
        "Dataset Data Use Codes": "NOPE,Pending Annotation,DUOPlus1",
        "iconTags": "",
    },
]

LEGACY_NAMES = {
    "Assay": "Dataset Assay",
    "Species": "Dataset Species",
    "Tumor Type": "Dataset Tumor Type",
    "Tissue": "Dataset Tissue",
    "Dataset Data Use Codes": "Data Use Codes",
}

GRANTS = pd.DataFrame(
    {
        "grantNumber": ["CA1"],
        "grantName": ["Grant One"],
        "theme": [["Theme X"]],
        "consortium": [["Consortium Y"]],
    }
)
PUBS = pd.DataFrame(
    {
        "pubMedId": [123],
        "publicationTitle": ["A title"],
        "doi": ["10.9/pub"],
        "publicationYear": [2020],
    }
)


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(sync_datasets, "_get_croissant_versions", lambda syn: {})
    monkeypatch.setattr(sync_datasets.operations, "get", lambda *a, **k: None)

    received = {}  # first DatasetViewId -> the entity add_missing_info passed

    def fake_download(syn, row, repo, entity=None):
        assert "DatasetViewId" in row  # the 14.0.0 key, passed straight through
        received[row["DatasetViewId"].split(",")[0]] = entity
        return "Open Access", row["DatasetViewId"].split(",")[0]

    monkeypatch.setattr(utils, "identify_download_type", fake_download)
    return received


def live_types():
    types = {c: "STRING" for c in crosswalk.expected_columns("DatasetView", [])}
    for c in ("assay", "species", "tissue", "tumorType", "fileFormats", "grantNumber",
              "pubMedId", "theme", "consortium", "grantName", "publicationTitle",
              "dataType", "accessInformation"):
        types[c] = "STRING_LIST"
    types["version"] = "INTEGER"
    return types


def prepare(rows, legacy, stripped=False):
    df = pd.DataFrame(rows).fillna("")
    if legacy:
        df = df.rename(columns=LEGACY_NAMES)
    if stripped:  # the live manifest: 13.1.0 names with spaces removed
        df = df.rename(columns=lambda c: c if c == "iconTags" else c.replace(" ", ""))
    df = model_contract.restore_display_names(
        df, sync_datasets.COMPONENT, sync_datasets.MANIFEST_PASSTHROUGH
    )
    df = model_contract.rename_legacy_headers(df, "DatasetView")
    return model_contract.normalize_headers(
        df, to="keys", passthrough=sync_datasets.MANIFEST_PASSTHROUGH
    )


def run(rows, legacy=False, stripped=False):
    df = prepare(rows, legacy, stripped)
    df = sync_datasets.add_missing_info(None, df, GRANTS, PUBS)
    return sync_datasets.clean_table(df, live_types())


def canon(df):
    """Order-insensitive view of list cells."""
    return df.map(lambda v: sorted(v) if isinstance(v, list) else v).astype(str)


def test_legacy_and_current_headers_give_identical_output(caplog):
    new = run(MANIFEST_14)
    with caplog.at_level(logging.WARNING):
        old = run(MANIFEST_14, legacy=True)
    assert "Renaming legacy column" in caplog.text
    pd.testing.assert_frame_equal(canon(new), canon(old))
    assert list(new.columns) == crosswalk.expected_columns("DatasetView", [])
    assert sorted(new["datasetId"]) == ["syn111", "syn222"]


def test_live_style_stripped_headers_give_identical_output():
    new = run(MANIFEST_14)
    live = run(MANIFEST_14, legacy=True, stripped=True)
    pd.testing.assert_frame_equal(canon(new), canon(live))
    also_14 = run(MANIFEST_14, stripped=True)  # stripped 14.0.0 names work too
    pd.testing.assert_frame_equal(canon(new), canon(also_14))


def test_access_text_for_ids_and_short_codes_match():
    code_to_id = sync_datasets.duo_code_to_id()
    for duo_id, code in [("DUO:0000042", "GRU"), ("DUO:0000029", "RTN"), ("DUO:0000018", "NPUNCU"), ("DUO:0000012", "RS")]:
        assert code_to_id[code] == duo_id
        assert sync_datasets.translate_access(duo_id, code_to_id) == utils.DUO_DICT[duo_id]
        assert sync_datasets.translate_access(code, code_to_id) == utils.DUO_DICT[duo_id]


def test_pending_annotation_and_duoplus_are_mapped():
    code_to_id = sync_datasets.duo_code_to_id()
    assert sync_datasets.translate_access("Pending Annotation", code_to_id) == sync_datasets.PENDING_ACCESS_TEXT
    assert sync_datasets.translate_access("DUOPlus1", code_to_id).startswith("Source geography")
    assert sync_datasets.translate_access("NOPE", code_to_id) is None


def test_duoplus_text_matches_data_models_description():
    duo = pd.read_csv(model_contract.fetch("modules/shared/duo.csv"), dtype=str).fillna("")
    duoplus = {k: v for k, v in utils.DUO_DICT.items() if k.startswith("DUOPlus")}
    assert len(duoplus) == 7
    for code, text in duoplus.items():
        desc = duo.loc[duo["Attribute"] == code, "Description"].iloc[0]
        assert desc.startswith(text), code


def test_entry_with_no_mapped_code_gets_no_access_text():
    rows = [dict(MANIFEST_14[1], **{"Dataset Data Use Codes": "NOPE,ALSONOPE"})]
    row = run(rows).iloc[0]
    assert row["accessInformation"] == [sync_datasets.NO_ACCESS_TEXT]


def test_unknown_code_warns_and_row_is_still_processed(caplog):
    with caplog.at_level(logging.WARNING):
        out = run(MANIFEST_14)
    assert "NOPE" in caplog.text
    row = out[out["datasetAlias"] == "setB"].iloc[0]
    assert not any("NOPE" in t for t in row["accessInformation"])  # unmapped code dropped
    assert sync_datasets.PENDING_ACCESS_TEXT in row["accessInformation"]
    assert row["downloadType"] == "Open Access"
    assert row["downloadSynId"] == "syn222"
    # set from the link/alias, not skipped because of the unknown code
    assert row["sourceRepository"] == utils.extract_map_repository("", "setB")
    row_a = out[out["datasetAlias"] == "setA"].iloc[0]
    assert sorted(row_a["accessInformation"]) == sorted([utils.DUO_DICT["DUO:0000042"], utils.DUO_DICT["DUO:0000029"]])


def test_theme_consortium_and_pub_lists_keep_first_seen_order(monkeypatch):
    grants = pd.DataFrame(
        {
            "grantNumber": ["CA1", "CA2"],
            "grantName": ["One", "Two"],
            "theme": [["Zeta", "Alpha"], ["Alpha", "Mu"]],
            "consortium": [["Q", "B"], ["B", "A"]],
        }
    )
    pubs = pd.DataFrame(
        {
            "pubMedId": [1001, 1002, 1003],
            "publicationTitle": ["Z title", "A title", "Z title"],
            "doi": ["d1", "d2", "d3"],
            "publicationYear": [2020, 2021, 2022],
        }
    )
    row = dict(MANIFEST_14[1], **{"GrantView Key": "CA1,CA2", "PublicationView Key": "1001,1002,1003"})
    df = prepare([row], legacy=False)
    out = sync_datasets.add_missing_info(None, df, grants, pubs)
    assert out.at[0, "theme"] == ["Zeta", "Alpha", "Mu"]
    assert out.at[0, "consortium"] == ["Q", "B", "A"]
    assert out.at[0, "pub"] == ["Z title", "A title"]


# Short codes held by the live manifest. Each must keep resolving to the text
# of its DUO ID (the text itself now comes from governanceDUO).
LEGACY_CODE_IDS = {
    "RTN": "DUO:0000029",
    "NPUNCU": "DUO:0000018",
    "GRU": "DUO:0000042",
    "IRB": "DUO:0000021",
    "NCU": "DUO:0000046",
    "NPU": "DUO:0000045",
    "NRES": "DUO:0000004",
    "RS": "DUO:0000012",
}


def test_placeholder_cells_count_as_incomplete_in_the_sort():
    # B: 4 placeholder cells; C: the same row with those 4 annotated but 2 other cells blank
    b = dict(MANIFEST_14[1], **{"Dataset Name": "Set B", "Dataset Alias": "setB"})
    c = dict(b, **{"Dataset Name": "Set C", "Dataset Alias": "setC", "DatasetView_id": "syn333",
                   "Assay": "RNA-seq", "Species": "Human", "Tumor Type": "Breast Cancer", "Tissue": "Breast",
                   "Dataset Description": "", "Dataset Design": ""})
    out = run([b, c])
    assert out["datasetName"].tolist() == ["Set C", "Set B"]


def test_legacy_short_codes_resolve_to_their_id_text():
    code_to_id = sync_datasets.duo_code_to_id()
    for code, duo_id in LEGACY_CODE_IDS.items():
        assert sync_datasets.translate_access(code, code_to_id) == utils.DUO_DICT[duo_id]


def test_data_models_and_official_shorthands_share_text():
    code_to_id = sync_datasets.duo_code_to_id()
    for old, official, duo_id in [("USE", "US", "DUO:0000026"), ("RST", "RS", "DUO:0000012")]:
        assert code_to_id[old] == code_to_id[official] == duo_id
        assert sync_datasets.translate_access(old, code_to_id) == utils.DUO_DICT[duo_id]
        assert sync_datasets.translate_access(official, code_to_id) == utils.DUO_DICT[duo_id]


def test_aliased_npoa_id_translates_to_npoa_text():
    code_to_id = sync_datasets.duo_code_to_id()
    assert sync_datasets.translate_access("DUO:00000044", code_to_id) == utils.DUO_DICT["DUO:0000044"]


def test_every_14_duo_id_has_access_text():
    code_to_id = sync_datasets.duo_code_to_id()
    duo_ids = list(model_contract.duo_id_to_code())
    assert len(duo_ids) == 24
    for duo_id in duo_ids:
        text = sync_datasets.translate_access(duo_id, code_to_id)
        assert text == utils.DUO_DICT[duo_id]
        assert not text.startswith("Access information not mapped")


def test_duo_dict_keys_are_ids_or_manifest_sentinels():
    for key in utils.DUO_DICT:
        assert key.startswith("DUO:") or key.startswith("DUOPlus") or key == "Pending Annotation", key


def test_identify_download_type_accepts_both_keys(monkeypatch):
    seen = []
    monkeypatch.setattr(
        utils.operations, "get", lambda eid, **k: seen.append(eid) or None
    )
    for key in ("DatasetViewId", "DatasetView_id"):
        REAL_IDENTIFY(None, pd.Series({key: "syn5, syn6"}), "Synapse")
    assert seen == ["syn5", "syn5"]


def test_identify_download_type_skips_a_leading_comma(monkeypatch):
    seen = []
    monkeypatch.setattr(utils.operations, "get", lambda eid, **k: seen.append(eid) or None)
    REAL_IDENTIFY(None, pd.Series({"DatasetViewId": ", syn5,syn6"}), "Synapse")
    assert seen == ["syn5"]


def test_identify_download_type_uses_a_given_entity_without_a_call(monkeypatch):
    def no_call(*a, **k):
        raise AssertionError("no Synapse call expected")

    monkeypatch.setattr(utils.operations, "get", no_call)
    row = pd.Series({"DatasetViewId": "syn5, syn6"})
    assert REAL_IDENTIFY(None, row, "Synapse", entity=utils.Dataset(id="syn5")) == ("Synapse Hosted", "syn5")
    assert REAL_IDENTIFY(None, row, "GEO", entity=utils.Dataset(id="syn5")) == ("Synapse Indexed", "syn5")
    assert REAL_IDENTIFY(None, row, "Synapse", entity=None) == ("Not Available for Download", None)
    assert REAL_IDENTIFY(None, row, "GEO", entity=None) == ("Externally Hosted", None)


def test_access_texts_containing_commas_are_not_split():
    npuncu = utils.DUO_DICT["DUO:0000018"]
    assert "," in npuncu  # the case that used to be split into fragments
    rows = [dict(MANIFEST_14[0], **{"Dataset Data Use Codes": "RTN, NPUNCU"})]
    out = run(rows)
    assert out["accessInformation"].iloc[0] == [utils.DUO_DICT["DUO:0000029"], npuncu]


def _access_texts(cell):
    """The accessInformation add_missing_info gives for one DatasetDataUseCodes cell (NaN kept as it is)."""
    df = prepare([dict(MANIFEST_14[0])], legacy=False)
    df["DatasetDataUseCodes"] = pd.Series([cell], index=df.index, dtype=object)
    out = sync_datasets.add_missing_info(None, df, GRANTS, PUBS)
    return out["DatasetDataUseCodes"].iloc[0]


def test_data_use_codes_json_array_gives_its_items():
    rtn, npuncu = utils.DUO_DICT["DUO:0000029"], utils.DUO_DICT["DUO:0000018"]
    assert _access_texts('["RTN","NPUNCU"]') == [rtn, npuncu]  # was split on the commas inside the brackets
    assert _access_texts("RTN,NPUNCU") == [rtn, npuncu]


def test_data_use_codes_trailing_comma_gives_no_empty_item(caplog):
    with caplog.at_level(logging.WARNING):
        assert _access_texts("RTN,") == [utils.DUO_DICT["DUO:0000029"]]
    assert caplog.text == ""  # the empty piece is not looked up, so it logs no unmapped code


def test_data_use_codes_nan_cell_gives_no_access_text():
    assert _access_texts(float("nan")) == [sync_datasets.NO_ACCESS_TEXT]  # raised AttributeError on .split before


def test_no_doi_listed_is_standardized_for_datasets():
    rows = [
        dict(MANIFEST_14[0], **{"Dataset Doi": "No DOI Listed", "PublicationView Key": "123"}),
        dict(MANIFEST_14[1], **{"Dataset Doi": ""}),
    ]
    out = run(rows).set_index("datasetAlias")
    assert out.loc["setA", "doi"] == "DOI Not Available"  # not the linked publication's DOI
    assert out.loc["setB", "doi"] == "DOI Not Available"  # blank, no publication


def _doi_for(pub_dois):
    pubs = pd.DataFrame(
        {
            "pubMedId": [1001, 1002],
            "publicationTitle": ["T1", "T2"],
            "doi": pub_dois,
            "publicationYear": [2020, 2021],
        }
    )
    row = dict(MANIFEST_14[1], **{"Dataset Doi": "", "PublicationView Key": "1001,1002"})
    out = sync_datasets.add_missing_info(None, prepare([row], legacy=False), GRANTS, pubs)
    return out.at[0, "DatasetDoi"]


def test_blank_first_publication_doi_falls_through_to_first_non_blank():
    assert _doi_for([float("nan"), "10.2/b"]) == "10.2/b"
    assert _doi_for(["", "10.2/b"]) == "10.2/b"


def test_all_blank_publication_dois_give_not_available():
    assert _doi_for([float("nan"), ""]) == utils.DOI_NOT_AVAILABLE


def test_joined_grant_cell_with_comma_space_resolves_every_grant():
    grants = pd.DataFrame(
        {
            "grantNumber": ["CA1", "CA2"],
            "grantName": ["One", "Two"],
            "theme": [["T1"], ["T2"]],
            "consortium": [["C1"], ["C2"]],
        }
    )
    row = dict(MANIFEST_14[1], **{"GrantView Key": "CA1, CA2, Affiliated/Non-Grant Associated"})
    out = sync_datasets.add_missing_info(None, prepare([row], legacy=False), grants, PUBS)
    assert out.at[0, "grantName"] == ["One", "Two"]
    assert out.at[0, "theme"] == ["T1", "T2"]
    assert out.at[0, "consortium"] == ["C1", "C2"]


def _run_main_collecting_table_reads(monkeypatch, argv):
    """Run ``sync_datasets.main`` offline; return the (Table id, query) pairs it reads."""
    from unittest import mock

    reads = []

    def fake_table(id):
        table = mock.MagicMock()
        table.query.side_effect = lambda query, **kw: reads.append((id, query))
        return table

    monkeypatch.setattr("sys.argv", ["sync_datasets.py", "--dryrun", "--noprint", *argv])
    monkeypatch.setattr(sync_datasets, "Table", fake_table)
    monkeypatch.setattr(utils, "syn_login", lambda: mock.MagicMock())
    monkeypatch.setattr(utils, "read_sync_manifest", lambda syn, args: pd.DataFrame())
    monkeypatch.setattr(utils, "fill_blank", lambda df: df)
    monkeypatch.setattr(model_contract, "restore_display_names", lambda df, *a, **kw: df)
    monkeypatch.setattr(model_contract, "rename_legacy_headers", lambda df, c: df)
    monkeypatch.setattr(model_contract, "normalize_headers", lambda df, **kw: df)
    monkeypatch.setattr(sync_datasets, "add_missing_info", lambda *a: None)
    monkeypatch.setattr(crosswalk, "check_crosswalk", lambda *a: None)
    monkeypatch.setattr(sync_datasets, "clean_table", lambda *a: pd.DataFrame())
    sync_datasets.main()
    return reads


def test_main_reads_staging_grants_and_publications_by_default(monkeypatch):
    reads = _run_main_collecting_table_reads(monkeypatch, [])
    assert [r[0] for r in reads] == ["syn78748038", "syn78748028"]
    assert reads[0][1].endswith("FROM syn78748038")
    assert reads[1][1].endswith("FROM syn78748028")


def test_main_has_no_production_flag(monkeypatch):
    with pytest.raises(SystemExit):
        _run_main_collecting_table_reads(monkeypatch, ["--production"])


def test_a_leading_comma_in_the_dataset_id_does_not_hide_the_first_id(monkeypatch):
    seen = []
    monkeypatch.setattr(sync_datasets.operations, "get", lambda eid, **k: seen.append(eid) or None)
    df = prepare([dict(MANIFEST_14[0], **{"DatasetView_id": ",syn111, syn112"})], legacy=False)
    sync_datasets.add_missing_info(None, df, GRANTS, PUBS)
    assert seen == ["syn111"]


class FakeEntity:
    def __init__(self, version_number):
        self.version_number = version_number


def test_each_distinct_id_is_read_once_when_rows_share_it(monkeypatch):
    seen = []
    monkeypatch.setattr(sync_datasets.operations, "get", lambda eid, **k: seen.append(eid) or None)
    rows = [dict(MANIFEST_14[0], **{"DatasetView_id": "syn111"}),
            dict(MANIFEST_14[1], **{"DatasetView_id": "syn111"}),
            dict(MANIFEST_14[1], **{"DatasetView_id": "syn222"})]
    sync_datasets.add_missing_info(None, prepare(rows, legacy=False), GRANTS, PUBS)
    assert sorted(seen) == ["syn111", "syn222"]


def test_a_failed_read_gives_the_same_version_as_no_entity(monkeypatch, capsys):
    import synapseclient

    def fail(eid, **k):
        raise synapseclient.core.exceptions.SynapseHTTPError("boom")

    monkeypatch.setattr(sync_datasets.operations, "get", fail)
    out = sync_datasets.add_missing_info(None, prepare(MANIFEST_14, legacy=False), GRANTS, PUBS)
    assert list(out["version"]) == [1, 1]
    assert "Encountered error: boom" in capsys.readouterr().out


def test_entities_go_to_the_right_rows_when_reads_finish_out_of_order(monkeypatch, offline):
    import time

    delays = {"syn111": 0.3, "syn222": 0.0}
    versions = {"syn111": 5, "syn222": 9}

    def slow_get(eid, **k):
        time.sleep(delays[eid])
        return FakeEntity(versions[eid])

    monkeypatch.setattr(sync_datasets.operations, "get", slow_get)
    out = sync_datasets.add_missing_info(None, prepare(MANIFEST_14, legacy=False), GRANTS, PUBS)
    assert list(out["version"]) == [4, 8]
    assert {k: e.version_number for k, e in offline.items()} == versions  # identify_download_type got the same entities


def test_a_json_array_in_the_publication_key_gives_its_items():
    pubs = pd.DataFrame(
        {"pubMedId": [11111111, 22222222], "publicationTitle": ["T1", "T2"], "doi": ["d1", "d2"],
         "publicationYear": [2020, 2021]}
    )
    row = dict(MANIFEST_14[1], **{"PublicationView Key": '["11111111", "22222222"]'})
    out = sync_datasets.add_missing_info(None, prepare([row], legacy=False), GRANTS, pubs)
    assert out.at[0, "pub"] == ["T1", "T2"]


def test_list_columns_drop_empty_pieces_and_read_a_json_array():
    row = dict(MANIFEST_14[0], **{"Assay": "RNA-seq,,ATAC-seq", "Tissue": '["Breast", "Lung"]'})
    out = run([row])
    assert sorted(out["assay"].iloc[0]) == ["ATAC-seq", "RNA-seq"]
    assert sorted(out["tissue"].iloc[0]) == ["Breast", "Lung"]
