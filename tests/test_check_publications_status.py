"""check_publications_status / update_pending_annotations: crosswalk-based header maps."""
import importlib.util
from pathlib import Path

import pandas as pd

import header_maps
import crosswalk

ROOT = Path(__file__).resolve().parent.parent


def _load(relpath, name):
    spec = importlib.util.spec_from_file_location(name, ROOT / relpath)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _portal_row():
    cols = [portal for _, portal, _ in crosswalk.CROSSWALK["PublicationView"]]
    row = {c: "x" for c in cols}
    row["pubMedId"] = 123
    row["grantNumber"] = ["CA1", "CA2"]
    return pd.DataFrame([row])


def test_to_manifest_uses_display_names_and_drops_derived():
    cps = _load("utils/check_publications_status.py", "cps")
    out = cps.to_manifest(_portal_row())
    assert "GrantView Key" in out.columns
    assert "Publication Grant Number" not in out.columns
    assert out.loc[0, "GrantView Key"] == "CA1,CA2"
    assert {"Assay", "Tumor Type", "Tissue", "Pubmed Id", "Publication Doi"} <= set(out.columns)
    for derived in header_maps.derived_portal_columns("PublicationView"):
        assert derived not in out.columns
    assert out.loc[0, "Component"] == "PublicationView"


def test_to_manifest_empty_frame():
    cps = _load("utils/check_publications_status.py", "cps")
    out = cps.to_manifest(_portal_row().iloc[0:0])
    assert out.empty and "GrantView Key" in out.columns


def test_dryrun_flag_still_present():
    cps = _load("utils/check_publications_status.py", "cps")
    import sys
    old = sys.argv
    sys.argv = ["x", "--dryrun", "--outdir", "o"]
    try:
        args = cps.get_args()
    finally:
        sys.argv = old
    assert args.dryrun and args.outdir == "o"


def test_pending_annotations_rename_matches_portal_columns():
    mapping = header_maps.display_to_portal("PublicationView")
    assert mapping["Pubmed Id"] == "pubMedId"
    assert mapping["GrantView Key"] == "grantNumber"
    assert mapping["Assay"] == "assay"
    assert "Publication Grant Number" not in mapping


def test_pending_annotations_accepts_legacy_manifest(tmp_path):
    upa = _load("annotations/update_pending_annotations.py", "upa")
    path = tmp_path / "m.csv"
    pd.DataFrame(
        [{"Component": "PublicationView", "Pubmed Id": 1, "Publication Assay": "a",
          "Publication Grant Number": "CA1"}]
    ).to_csv(path, index=False)
    df = upa.get_updated_df(str(path))
    assert {"Assay", "GrantView Key"} <= set(df.columns)
    renamed = df.rename(columns=header_maps.display_to_portal("PublicationView"))
    assert {"assay", "grantNumber", "pubMedId"} <= set(renamed.columns)
