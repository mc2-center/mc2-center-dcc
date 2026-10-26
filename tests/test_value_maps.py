"""portal_tables/value_maps.py: enum, DUO, license, DOI and override rules."""
import pandas as pd
import pytest

import value_maps as vm

CODES = {"RTN": "DUO:0000029", "NPUNCU": "DUO:0000018", "GRU": "DUO:0000042"}


def normalizer(overrides=None):
    maps = {
        "Assay": vm.EnumMap(
            ["RNA Sequencing", "Mass Spectrometry", "Hepatocellular Carcinoma, NOS"],
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


def test_items_that_join_to_a_known_term_stay_together():
    row, _, unmapped = run(normalizer(), Assay="Hepatocellular Carcinoma, NOS, RNAseq")
    assert row["Assay"] == ["Hepatocellular Carcinoma, NOS", "RNA Sequencing"]
    assert unmapped.empty


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
    assert unmapped.empty  # blanked, not unmapped: the blanking is itself the recorded change


@pytest.mark.parametrize("raw", [
    "10.1038/s41586-020-1234-5",
    "doi:10.1038/s41586-020-1234-5",
    "http://dx.doi.org/10.1038/s41586-020-1234-5",
    "HTTPS://DOI.ORG/10.1038/s41586-020-1234-5",
    " https://www.nature.com/doi/10.1038/s41586-020-1234-5",
])
def test_doi_is_normalized_to_https_doi_org(raw):
    row, changes, _ = run(normalizer(), PublicationDoi=raw)
    assert row["PublicationDoi"] == "https://doi.org/10.1038/s41586-020-1234-5"
    assert changes["rule"].tolist() == ["doi"]


def test_canonical_doi_is_unchanged():
    row, changes, _ = run(normalizer(), PublicationDoi="https://doi.org/10.1/abc")
    assert row["PublicationDoi"] == "https://doi.org/10.1/abc" and changes.empty


@pytest.mark.parametrize("placeholder", ["No DOI Listed", "DOI Not Available", "no doi listed"])
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
