"""DUO portal text generated from governanceDUO (portal_tables/duo_terms.csv)."""

import csv
import sys
from pathlib import Path

import pytest

import model_contract
import utils

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "utils"))
import sync_duo_terms  # noqa: E402

FIXTURE = Path(__file__).parent / "fixtures" / "duo" / "mixins.yaml"


def test_every_14_duo_id_has_portal_text():
    for duo_id in model_contract.duo_id_to_code():
        assert utils.DUO_DICT[duo_id]


def test_no_portal_text_has_curator_guidance():
    for text in utils.DUO_DICT.values():
        assert "If providing" not in text


def test_alias_translates_to_npoa_text():
    assert utils.translate_duo("DUO:00000044") == utils.translate_duo("DUO:0000044")
    assert utils.DUO_DICT["DUO:0000044"].startswith("Population Origins or Ancestry Research Prohibited")


def test_shorthands_map_to_ids():
    assert utils.DUO_SHORTHANDS["US"] == "DUO:0000026"
    assert utils.DUO_SHORTHANDS["RS"] == "DUO:0000012"
    assert "" not in utils.DUO_SHORTHANDS


def test_build_rows_from_fixture():
    values = sync_duo_terms.load_enum(FIXTURE)
    rows = sync_duo_terms.build_rows(values, "src")
    assert rows == [
        {
            "id": "DUO:0000026",
            "shorthand": "US",
            "label": "User Specific Restriction",
            "portal_text": "User Specific Restriction - This data use modifier indicates that use is limited to use by approved users.",
            "source": "src",
        },
        {
            "id": "DUOPlus1",
            "shorthand": "",
            "label": "",
            "portal_text": "Source geography is relevant to governance decisions.",
            "source": "src",
        },
        {
            "id": "Pending Annotation",
            "shorthand": "",
            "label": "",
            "portal_text": "Not yet annotated.",
            "source": "src",
        },
    ]


def test_cli_writes_csv(tmp_path, monkeypatch):
    root = tmp_path / "gov"
    (root / "linkml").mkdir(parents=True)
    (root / "linkml" / "mixins.yaml").write_text(FIXTURE.read_text())
    out = tmp_path / "out.csv"
    monkeypatch.setattr(
        sys, "argv",
        ["sync_duo_terms.py", "--governance-duo", str(root), "--output", str(out), "--source", "src"],
    )
    sync_duo_terms.main()
    with open(out, newline="") as f:
        rows = list(csv.DictReader(f))
    assert [r["id"] for r in rows] == ["DUO:0000026", "DUOPlus1", "Pending Annotation"]
    assert list(rows[0]) == sync_duo_terms.COLUMNS


def test_missing_enum_exits(tmp_path):
    bad = tmp_path / "mixins.yaml"
    bad.write_text("enums: {}\n")
    with pytest.raises(SystemExit):
        sync_duo_terms.load_enum(bad)


def test_empty_portal_text_exits():
    with pytest.raises(SystemExit):
        sync_duo_terms.build_rows({"DUOPlus9": {"description": "If providing this term, say so."}}, "src")
