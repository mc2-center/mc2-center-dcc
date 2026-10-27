"""portal_tables/value_maps.py: enum, DUO, license, DOI and override rules."""
from pathlib import Path

import pandas as pd
import pytest

import value_maps as vm

CODES = {"RTN": "DUO:0000029", "NPUNCU": "DUO:0000018", "GRU": "DUO:0000042"}


def normalizer(overrides=None):
    maps = {
        "Assay": vm.EnumMap(
            ["RNA Sequencing", "Mass Spectrometry"],
            {"rna-seq": "RNA Sequencing", "RNAseq": "RNA Sequencing"},
        ),
        "PublicationAccessibility": vm.EnumMap(["Open Access", "Restricted Access"], {"open": "Open Access"}),
        "License": vm.EnumMap(["MIT", "Apache-2.0"], {"Apache 2.0": "Apache-2.0", "mit license": "MIT"}),
    }
    return vm.Normalizer(
        "PublicationView", maps, CODES, overrides,
        list_cols={"Assay", "License", "DatasetDataUseCodes"},
    )


def run(norm, **cells):
    df = pd.DataFrame({"PublicationViewId": ["p1"], **{k: [v] for k, v in cells.items()}})
    out, changes, unmapped = norm.normalize_frame(df, "PublicationViewId")
    return out.iloc[0], changes, unmapped


def test_exact_value_is_kept_and_not_recorded():
    row, changes, unmapped = run(normalizer(), Assay="RNA Sequencing", PublicationAccessibility="Open Access")
    assert row["Assay"] == ["RNA Sequencing"] and row["PublicationAccessibility"] == "Open Access"
    assert changes.empty and unmapped.empty


def test_case_and_whitespace_match_becomes_the_valid_value():
    row, changes, _ = run(normalizer(), PublicationAccessibility="  open   ACCESS ")
    assert row["PublicationAccessibility"] == "Open Access"
    assert changes.iloc[0].to_dict() == {
        "key": "p1", "column": "PublicationAccessibility", "from": "open   ACCESS", "to": "Open Access", "rule": "enum_case"}


def test_nonpreferred_term_becomes_the_preferred_value():
    row, changes, _ = run(normalizer(), PublicationAccessibility="open")
    assert row["PublicationAccessibility"] == "Open Access"
    assert changes["rule"].tolist() == ["nonpreferred"]


def test_list_cells_are_handled_per_item_and_deduplicated():
    row, changes, unmapped = run(normalizer(), Assay="rna-seq, mass spectrometry, RNA Sequencing, Western blot")
    assert row["Assay"] == ["RNA Sequencing", "Mass Spectrometry", "Western blot"]
    assert sorted(changes["rule"]) == ["enum_case", "nonpreferred"]
    assert unmapped.to_dict("records") == [{"key": "p1", "column": "Assay", "value": "Western blot"}]


def test_repeated_items_are_kept_in_free_text_list_columns_and_dropped_in_set_like_ones():
    norm = normalizer()
    norm.list_cols.add("PublicationAuthors")
    row, _, _ = run(norm, PublicationAuthors="Wei Wang, Li Chen, Wei Wang", DatasetDataUseCodes="DUO:0000042, DUO:0000042")
    assert row["PublicationAuthors"] == ["Wei Wang", "Li Chen", "Wei Wang"]
    assert row["DatasetDataUseCodes"] == ["DUO:0000042"]


@pytest.mark.parametrize("fragment,ok", [
    ("Kidney", False), ("unclassified", False), ("conventional", False), ("Clear Cell", True),
    ("RNA-seq", True), ("snATAC", True), ("HCC", True), ("3C", True), ("Hepatoma", False), ("", False),
])
def test_acceptable_fragment(fragment, ok):
    assert vm.acceptable_fragment(fragment) is ok


@pytest.mark.parametrize("fragment,valid,ok", [
    ("Casfish", "CASFISH", True), ("Ligaments", "Ligament", True), ("Intestines", "Intestine", True),
    ("medulloblastoma", "Medulloblastoma", True), ("Cut&run", "CUT&RUN", True), ("cut run", "CUT&RUN", True),
    ("X ray", "X-Ray", True), ("Boxes", "Box", True), ("Box", "Boxes", True), ("Kidneys", "Kidney", True),
    ("Kidney", "Kidney Carcinoma", False), ("Method", "Epidemiological Method", False),
    ("Mass", "Mass Spectrometry", False), ("computational", "Computational Tool", False),
    ("Crystallography", "X-Ray Crystallography", False), ("unclassified", "Kidney Carcinoma", False),
    ("conventional", "Kidney Carcinoma", False), ("", "Kidney", False),
])
def test_is_variant(fragment, valid, ok):
    assert vm.is_variant(fragment, valid) is ok


def test_variant_fragments_of_their_own_value_map_and_other_fragments_stay_rejected(tmp_path, monkeypatch):
    write_model(tmp_path, monkeypatch, [
        ["tool_type", "Ligament", "Ligaments, Soft Tissue Band"],
        ["tool_type", "Medulloblastoma", "medulloblastoma, Brain Tumor, Cerebellar"],
        ["tool_type", "Mass Spectrometry", "MS Assay, Mass"],
    ])
    rejected = []
    enum = vm.load_enum_maps("ToolView", rejected)["ToolType"]
    assert enum.lookup("Ligaments") == ("Ligament", "enum_case") or enum.lookup("Ligaments") == ("Ligament", "nonpreferred")
    assert enum.lookup("ligaments")[0] == "Ligament"
    assert enum.lookup("Soft Tissue Band") == ("Ligament", "nonpreferred")
    assert enum.lookup("Cerebellar") == ("Cerebellar", "unmapped")
    assert enum.lookup("Mass") == ("Mass", "unmapped")
    assert sorted(r["fragment"] for r in rejected) == ["Cerebellar", "Mass"]


def write_model(tmp_path, monkeypatch, rows):
    """A mini data-models directory: one schema with one enum property and an all_valid_values.csv."""
    import json
    import shutil
    model = tmp_path / "model"
    (model / "json_schemas").mkdir(parents=True)
    shutil.copytree(Path(__file__).parent / "fixtures" / "modules", model / "modules")
    (model / "json_schemas" / "ToolView.json").write_text(json.dumps(
        {"properties": {"ToolType": {"type": "string", "enum": sorted({r[1] for r in rows})}}}))
    pd.DataFrame(rows, columns=["category", "valid_value", "nonpreferred_values"]).to_csv(model / "all_valid_values.csv", index=False)
    monkeypatch.setenv("MC2_MODEL_DIR", str(model))


def test_comma_split_fragments_that_are_not_terms_do_not_map_and_are_reported(tmp_path, monkeypatch):
    write_model(tmp_path, monkeypatch, [
        ["tool_type", "Kidney Carcinoma", "Clear Cell Adenocarcinoma, Kidney, unclassified, conventional, RNA-seq-like"],
        ["tool_type", "RNA Sequencing", "rna-seq"],
        ["tool_type", "Liver", "liver tissue"],
    ])
    rejected = []
    maps = vm.load_enum_maps("ToolView", rejected)
    enum = maps["ToolType"]
    for word in ("Kidney", "unclassified", "conventional"):
        assert enum.lookup(word) == (word, "unmapped"), word
    assert enum.lookup("Clear Cell Adenocarcinoma") == ("Kidney Carcinoma", "nonpreferred")
    assert enum.lookup("RNA-seq-like") == ("Kidney Carcinoma", "nonpreferred")  # identifier-like token kept
    assert enum.lookup("rna-seq") == ("RNA Sequencing", "nonpreferred")  # a single-term cell is not a fragment
    assert enum.lookup("liver tissue") == ("Liver", "nonpreferred")
    assert sorted(r["fragment"] for r in rejected) == ["Kidney", "conventional", "unclassified"]
    assert {r["valid_value"] for r in rejected} == {"Kidney Carcinoma"}


def test_for_component_exposes_the_rejected_synonyms_once_each(tmp_path, monkeypatch):
    write_model(tmp_path, monkeypatch, [["tool_type", "A Type", "Long Name, Kidney, Long Name, Kidney"]])
    norm = vm.Normalizer.for_component("ToolView")
    assert norm.rejected["fragment"].tolist() == ["Kidney"]
    assert norm.rejected.columns.tolist() == ["column", "valid_value", "fragment", "cell", "reason"]


def test_case_fold_collision_maps_to_neither_value_and_is_reported(tmp_path, monkeypatch, caplog):
    enum = vm.EnumMap(["Mouse", "mouse", "Rat"])
    assert enum.lookup("MOUSE") == ("MOUSE", "unmapped")
    assert enum.lookup("rat") == ("Rat", "enum_case")
    assert enum.lookup("Mouse") == ("Mouse", None)  # exact values still pass
    assert enum.collisions == [("Mouse", "mouse")]
    assert vm.EnumMap(["mouse", "Mouse"]).collisions == enum.collisions  # order-independent
    write_model(tmp_path, monkeypatch, [["tool_type", "Mouse", ""], ["tool_type", "mouse", ""]])
    norm = vm.Normalizer.for_component("ToolView")
    assert "case-fold collision" in norm.rejected["reason"].iloc[0] and "differ only by case" in caplog.text


def test_split_items_splits_on_commas_without_rejoining():
    assert vm.split_items("A, B,C") == ["A", "B", "C"]
    assert vm.split_items('["A, B", "C"]') == ["A, B", "C"]


def test_override_matches_the_whole_cell_before_its_items():
    norm = normalizer({("Assay", "rna-seq, Western blot"): "Mass Spectrometry",
                       ("Assay", "Western blot"): "RNA Sequencing"})
    row, changes, _ = run(norm, Assay="rna-seq, Western blot")
    assert row["Assay"] == ["Mass Spectrometry"]
    assert changes.to_dict("records") == [{"key": "p1", "column": "Assay", "from": "rna-seq, Western blot",
                                           "to": "Mass Spectrometry", "rule": "override"}]
    row, _, _ = run(norm, Assay="rna-seq, Western blot, RNAseq")  # not the whole cell: items one by one
    assert row["Assay"] == ["RNA Sequencing"]


def test_json_array_cells_are_accepted():
    row, _, _ = run(normalizer(), Assay='["rna-seq", "Mass Spectrometry"]')
    assert row["Assay"] == ["RNA Sequencing", "Mass Spectrometry"]


def test_unmapped_value_is_left_unchanged_and_reported():
    row, changes, unmapped = run(normalizer(), PublicationAccessibility="Embargoed")
    assert row["PublicationAccessibility"] == "Embargoed"
    assert changes.empty and len(unmapped) == 1


def test_data_use_codes_become_duo_ids():
    row, changes, unmapped = run(normalizer(), DatasetDataUseCodes="RTN, NPUNCU, DUO:0000042, Pending Annotation, XYZ")
    assert row["DatasetDataUseCodes"] == ["DUO:0000029", "DUO:0000018", "DUO:0000042", "Pending Annotation", "XYZ"]
    assert changes["rule"].tolist() == ["duo_code", "duo_code"]
    assert unmapped["value"].tolist() == ["XYZ"]


def test_license_maps_to_spdx_and_blanks_what_it_cannot_map():
    row, changes, unmapped = run(normalizer(), License="Apache 2.0, Our own terms, MIT License")
    assert row["License"] == ["Apache-2.0", "MIT"]
    assert sorted(changes["rule"]) == ["license_blanked", "nonpreferred", "nonpreferred"]
    assert changes[changes["rule"] == "license_blanked"].iloc[0]["from"] == "Our own terms"
    assert unmapped["value"].tolist() == ["Our own terms"]  # blanked and reported (O6)


@pytest.mark.parametrize("raw", [
    "10.1038/s41586-020-1234-5",
    "doi:10.1038/s41586-020-1234-5",
    "http://dx.doi.org/10.1038/s41586-020-1234-5",
    "HTTPS://DOI.ORG/10.1038/s41586-020-1234-5",
    "https://www.doi.org/10.1038/s41586-020-1234-5",
    "https://doi.org/10.1038/s41586-020-1234-5.",
    "10.1038/s41586-020-1234-5),",
    "doi: 10.1038/s41586-020-1234-5;",
])
def test_doi_is_normalized_to_https_doi_org(raw):
    row, changes, _ = run(normalizer(), PublicationDoi=raw)
    assert row["PublicationDoi"] == "https://doi.org/10.1038/s41586-020-1234-5"
    assert changes["rule"].tolist() == ["doi"]


def test_canonical_doi_is_unchanged():
    row, changes, _ = run(normalizer(), PublicationDoi="https://doi.org/10.1000/abc")
    assert row["PublicationDoi"] == "https://doi.org/10.1000/abc" and changes.empty


@pytest.mark.parametrize("placeholder", ["No DOI Listed", "DOI Not Available", "no doi listed", "https://doi.org/No DOI listed", "https://doi.org/"])
def test_doi_placeholders_become_blank(placeholder):
    row, changes, _ = run(normalizer(), PublicationDoi=placeholder)
    assert row["PublicationDoi"] == ""
    assert changes.iloc[0]["rule"] == "doi_placeholder" and changes.iloc[0]["to"] == ""


def test_text_without_a_doi_is_reported_unmapped():
    row, _, unmapped = run(normalizer(), PublicationDoi="pending")
    assert row["PublicationDoi"] == "pending" and len(unmapped) == 1


def test_overrides_take_precedence_over_every_rule():
    norm = normalizer({("PublicationAccessibility", "open"): "Restricted Access",
                       ("PublicationDoi", "No DOI Listed"): "https://doi.org/10.9/x",
                       ("Assay", "Western blot"): "Mass Spectrometry"})
    row, changes, unmapped = run(norm, PublicationAccessibility="open", PublicationDoi="No DOI Listed",
                                 Assay="rna-seq, Western blot")
    assert row["PublicationAccessibility"] == "Restricted Access"
    assert row["PublicationDoi"] == "https://doi.org/10.9/x"
    assert row["Assay"] == ["RNA Sequencing", "Mass Spectrometry"]
    assert sorted(changes["rule"]) == ["nonpreferred", "override", "override", "override"]
    assert unmapped.empty


def test_override_can_blank_a_value():
    norm = normalizer({("PublicationAccessibility", "Embargoed"): ""})
    row, changes, unmapped = run(norm, PublicationAccessibility="Embargoed")
    assert row["PublicationAccessibility"] == "" and changes["rule"].tolist() == ["override"]


def test_load_overrides_accepts_display_names_and_class_labels(tmp_path):
    path = tmp_path / "o.csv"
    pd.DataFrame({"column": ["Publication Title", "PublicationTitle"], "current_value": ["a", "b"],
                  "proposed_value": ["A", "B"]}).to_csv(path, index=False)
    assert vm.load_overrides(path) == {("PublicationTitle", "a"): "A", ("PublicationTitle", "b"): "B"}


def test_load_overrides_requires_its_columns(tmp_path):
    path = tmp_path / "o.csv"
    pd.DataFrame({"column": ["x"]}).to_csv(path, index=False)
    with pytest.raises(ValueError, match="lacks"):
        vm.load_overrides(path)


def test_ambiguous_nonpreferred_term_is_not_mapped():
    enum = vm.EnumMap(["A", "B"], [("x", "A"), ("x", "B")])
    assert enum.lookup("x") == ("x", "unmapped")


def test_load_enum_maps_from_the_model_files():
    maps = vm.load_enum_maps("ToolView")
    assert maps["License"].lookup("MIT") == ("MIT", None)
    assert maps["License"].lookup("mit") == ("MIT", "enum_case")
    assert "ToolType" in maps and "ToolLanguage" in maps
    pub = vm.load_enum_maps("PublicationView")
    assert pub["Assay"].lookup("RNA Sequencing") == ("RNA Sequencing", None)


def test_for_component_wires_the_duo_codes():
    norm = vm.Normalizer.for_component("DatasetView")
    row, _, _ = run(norm, DatasetDataUseCodes="RTN, NPUNCU")
    assert row["DatasetDataUseCodes"] == ["DUO:0000029", "DUO:0000018"]


def test_truncated_values_are_reported_unchanged_and_not_unmapped():
    row, changes, unmapped = run(normalizer(), Assay="Sin(Read more on Pubmed), rna-seq",
                                 PublicationAccessibility="Op(Read more on Pubmed)")
    assert row["Assay"] == ["Sin(Read more on Pubmed)", "RNA Sequencing"]
    assert row["PublicationAccessibility"] == "Op(Read more on Pubmed)"
    truncated = changes[changes["rule"] == "truncated"]
    assert truncated["from"].tolist() == truncated["to"].tolist() == ["Sin(Read more on Pubmed)", "Op(Read more on Pubmed)"]
    assert unmapped.empty


def test_truncated_suffix_in_a_free_text_column_is_ignored():
    _, changes, _ = run(normalizer(), PublicationTitle="A title (Read more on Pubmed)")
    assert changes.empty


def test_override_beats_truncated():
    norm = normalizer({("Assay", "Sin(Read more on Pubmed)"): "RNA Sequencing"})
    row, changes, _ = run(norm, Assay="Sin(Read more on Pubmed)")
    assert row["Assay"] == ["RNA Sequencing"] and changes["rule"].tolist() == ["override"]


@pytest.mark.parametrize("url", [
    "https://www.biorxiv.org/content/10.1101/2020.01.01.123456v2.full",
    "https://pubs.acs.org/doi/10.1021/acs.jmedchem.0c00001?ref=pdf",
    "https://www.nature.com/articles/10.1038/s41586-020-1234-5",
    "http://example.org/doi.org/10.1000/abc",
])
def test_other_urls_are_left_unchanged_and_reported_as_nonstandard(url):
    row, changes, unmapped = run(normalizer(), PublicationDoi=url)
    assert row["PublicationDoi"] == url and unmapped.empty
    assert changes.to_dict("records") == [{"key": "p1", "column": "PublicationDoi", "from": url, "to": url,
                                           "rule": "doi_nonstandard"}]
