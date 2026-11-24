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


def test_learn_appends_only_unknown_terms_without_touching_existing_rows(tmp_path):
    path = tmp_path / "terms.csv"
    path.write_text('term,source,added\n"Aged, 80 and over",MeSH 2025,2026-01-01')  # no trailing newline
    before = path.read_text()
    new = comma_terms.learn(["aged, 80 and over", "Foo, bar", "Foo, bar", "plain", "Zed, y"], "upload PublicationView", path)
    assert new == ["Foo, bar", "Zed, y"]
    text = path.read_text()
    assert text.startswith(before + "\n")
    rows = comma_terms.load_terms(path)
    assert [r["term"] for r in rows] == ["Aged, 80 and over", "Foo, bar", "Zed, y"]
    assert rows[1]["source"] == "upload PublicationView" and rows[1]["added"]
    assert comma_terms.learn(["Foo, bar"], "upload PublicationView", path) == []
    assert path.read_text() == text


def test_learned_term_is_used_by_the_next_split(tmp_path):
    path = tmp_path / "terms.csv"
    path.write_text("term,source,added\n")
    assert comma_terms.split_keyword_cell("Foo, bar, baz", path=path) == ["Foo", "bar", "baz"]
    comma_terms.learn(["Foo, bar"], "upload X", path)
    assert comma_terms.split_keyword_cell("Foo, bar, baz", path=path) == ["Foo, bar", "baz"]


def test_learn_comma_terms_reads_json_keyword_items_of_a_parsed_upload(tmp_path):
    import pandas as pd
    import recordsets

    path = tmp_path / "terms.csv"
    path.write_text("term,source,added\n")
    up = pd.DataFrame({"PublicationViewId": ["a"], "PublicationKeywords": ['["Novel, Term", "Humans"]'], "Assay": ['["X, y"]']})
    parsed = recordsets.parse_upload(up, {"PublicationKeywords", "Assay"})
    assert recordsets.learn_comma_terms(parsed, "PublicationView", path) == ["Novel, Term"]
    assert [r["source"] for r in comma_terms.load_terms(path)] == ["upload PublicationView"]


def test_tests_never_touch_the_real_terms_file():
    assert comma_terms.CSV_PATH != Path(comma_terms.__file__).with_name("comma_terms.csv")
