import pandas as pd
import pytest

import crosswalk
import sync_publications as sp

LIVE_TYPES = {
    "doi": "LINK", "journal": "STRING", "pubMedId": "INTEGER", "pubMedUrl": "LINK",
    "pubMedLink": "LARGETEXT", "publicationTitle": "STRING", "publicationYear": "INTEGER",
    "keywords": "LARGETEXT", "authors": "LARGETEXT", "abstract": "LARGETEXT",
    "assay": "STRING_LIST", "tumorType": "STRING_LIST", "tissue": "STRING_LIST",
    "theme": "STRING_LIST", "consortium": "STRING_LIST", "grantNumber": "STRING_LIST",
    "grantName": "STRING_LIST", "dataset": "STRING", "accessibility": "STRING",
    "dataType": "STRING_LIST",
}

GRANTS = pd.DataFrame(
    {
        "grantNumber": ["CA1", "CA2"],
        "theme": ["['Metastasis']", "['Drug Resistance']"],
        "consortium": ["['CSBC']", "['PS-ON']"],
        "grantName": ["Grant One", "Grant Two"],
    }
)

NEW_HEADERS = {"Assay": "Assay", "Tumor Type": "Tumor Type", "Tissue": "Tissue", "GrantView Key": "GrantView Key"}
LEGACY = {
    "Assay": "Publication Assay",
    "Tumor Type": "Publication Tumor Type",
    "Tissue": "Publication Tissue",
    "GrantView Key": "Publication Grant Number",
}


def manifest(names: dict) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Pubmed Id": [1, 2],
            "Component": ["PublicationView"] * 2,
            "PublicationView_id": ["1", "2"],
            names["GrantView Key"]: ["CA1,CA2", "CA2"],
            "Study Key": ["", ""],
            "Publication Doi": ["d1", "d2"],
            "Publication Journal": ["J", "J"],
            "Pubmed Url": ["http://p/1", "http://p/2"],
            "Publication Title": ["T1", "T2"],
            "Publication Year": [2020, 2021],
            "Publication Keywords": ["", ""],
            "Publication Authors": ["A", "B"],
            "Publication Abstract": ["x", "y"],
            names["Assay"]: ["RNA-seq", "Not Applicable"],
            names["Tumor Type"]: ["Breast", "Lung"],
            names["Tissue"]: ["Breast", "Lung"],
            "Publication Accessibility": ["Open Access", "Open Access"],
            "Publication Dataset Alias": ["ds1, SRX123", "Not Applicable"],
            "entityId": ["syn1", "syn2"],
            "iconTags": ["", "tag"],
        }
    )


def run(tmp_path, names):
    path = tmp_path / "m.csv"
    manifest(names).to_csv(path, index=False)
    m = sp.prepare_manifest(pd.read_csv(path, dtype=str))
    db = sp.add_missing_info(m, GRANTS, ["theme", "consortium", "grantName"])
    return sp.clean_table(db, LIVE_TYPES)


def test_new_and_legacy_manifests_give_identical_output(tmp_path):
    new = run(tmp_path, NEW_HEADERS)
    old = run(tmp_path, LEGACY)
    pd.testing.assert_frame_equal(new, old)


def test_portal_shape(tmp_path):
    out = run(tmp_path, NEW_HEADERS)
    assert list(out.columns) == crosswalk.expected_columns("PublicationView", list(LIVE_TYPES))
    assert out["pubMedId"].tolist() == [2, 1]  # most recent first
    row = out[out.pubMedId == 1].iloc[0]
    assert row["grantNumber"] == ["CA1", "CA2"]
    assert row["grantName"] == ["Grant One", "Grant Two"]
    assert row["dataset"] == "ds1"  # SRX alias dropped
    assert row["pubMedLink"] == "[PMID:1](http://p/1)"
    assert row["assay"] == ["RNA-seq"]


def test_blank_and_no_doi_listed_become_doi_not_available(tmp_path):
    m = manifest(NEW_HEADERS)
    m["Publication Doi"] = ["", "No DOI Listed"]
    m["Pubmed Id"] = [1, 2]
    m = pd.concat([m, m.assign(**{"Pubmed Id": [3, 4], "Publication Doi": ["10.1/x", "  "]})])
    path = tmp_path / "m.csv"
    m.to_csv(path, index=False)
    prepared = sp.prepare_manifest(pd.read_csv(path, dtype=str))
    db = sp.add_missing_info(prepared, GRANTS, ["theme", "consortium", "grantName"])
    out = sp.clean_table(db, LIVE_TYPES).set_index("pubMedId")
    assert out["doi"].to_dict() == {
        1: "DOI Not Available", 2: "DOI Not Available", 3: "10.1/x", 4: "DOI Not Available",
    }
