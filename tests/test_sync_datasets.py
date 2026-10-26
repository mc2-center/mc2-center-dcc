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

    def fake_download(syn, row, repo):
        assert "DatasetViewId" in row  # the 14.0.0 key, passed straight through
        return "Open Access", row["DatasetViewId"].split(",")[0]

    monkeypatch.setattr(utils, "identify_download_type", fake_download)


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
    df = sync_datasets.restore_display_names(df)
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
    assert sync_datasets.translate_access("Pending Annotation", code_to_id) == utils.DUO_DICT["Pending Annotation"]
    assert sync_datasets.translate_access("DUOPlus1", code_to_id).startswith("Source geography")


def test_duoplus_text_matches_data_models_description():
    duo = pd.read_csv(model_contract.fetch("modules/shared/duo.csv"), dtype=str).fillna("")
    duoplus = {k: v for k, v in utils.DUO_DICT.items() if k.startswith("DUOPlus")}
    assert len(duoplus) == 7
    for code, text in duoplus.items():
        desc = duo.loc[duo["Attribute"] == code, "Description"].iloc[0]
        assert desc.startswith(text), code


def test_unknown_code_warns_and_row_is_still_processed(caplog):
    with caplog.at_level(logging.WARNING):
        out = run(MANIFEST_14)
    assert "NOPE" in caplog.text
    row = out[out["datasetAlias"] == "setB"].iloc[0]
    assert "Access information not mapped: NOPE" in row["accessInformation"]
    assert utils.DUO_DICT["Pending Annotation"] in row["accessInformation"]
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


def test_access_texts_containing_commas_are_not_split():
    npuncu = utils.DUO_DICT["DUO:0000018"]
    assert "," in npuncu  # the case that used to be split into fragments
    rows = [dict(MANIFEST_14[0], **{"Dataset Data Use Codes": "RTN, NPUNCU"})]
    out = run(rows)
    assert out["accessInformation"].iloc[0] == [utils.DUO_DICT["DUO:0000029"], npuncu]
