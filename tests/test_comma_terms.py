"""Known comma-containing list terms (portal_tables/comma_terms.csv)."""

import sys
from pathlib import Path

import comma_terms

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "utils"))
import sync_comma_terms  # noqa: E402


def test_seed_file_holds_mesh_terms_with_commas():
    rows = comma_terms.load_terms()
    terms = {r["term"] for r in rows}
    assert {"Aged, 80 and over", "Mice, Inbred C57BL", "Antigens, CD"} <= terms
    assert all(", " in r["term"] and r["source"] and r["added"] for r in rows)


def test_load_terms_of_a_missing_file_is_empty(tmp_path):
    assert comma_terms.load_terms(tmp_path / "none.csv") == []


def test_descriptor_names_keep_only_comma_names_once():
    lines = ["*NEWRECORD", "MH = Humans", "MH = Aged, 80 and over", "MS = Aged, x", "MH = Aged, 80 and over"]
    assert sync_comma_terms.descriptor_names(lines) == ["Aged, 80 and over"]


def test_build_rows_keeps_other_sources_and_prior_added_dates():
    existing = [
        {"term": "Old, Term", "source": "MeSH 2024", "added": "2025-01-01"},
        {"term": "Mine, Own", "source": "upload X", "added": "2025-02-02"},
    ]
    rows = sync_comma_terms.build_rows(["Old, Term", "New, Term", "mine, own"], 2025, existing, "2026-01-01")
    assert rows == [
        {"term": "Old, Term", "source": "MeSH 2025", "added": "2025-01-01"},
        {"term": "New, Term", "source": "MeSH 2025", "added": "2026-01-01"},
        {"term": "Mine, Own", "source": "upload X", "added": "2025-02-02"},
    ]
