import header_maps
import model_contract


def test_component_for_entity_type():
    assert header_maps.component_for_entity_type("Publication") == "PublicationView"
    assert header_maps.component_for_entity_type("Dataset") == "DatasetView"
    assert header_maps.component_for_entity_type("Resource") == "EducationalResource"


def test_every_rename_target_is_a_14_header():
    # schema_update.py sizes 14.0.0 columns via RENAMES_13_TO_14; the targets
    # must be real template headers.
    for component, renames in model_contract.RENAMES_13_TO_14.items():
        headers = set(model_contract.template_headers(component))
        assert set(renames.values()) <= headers, component


def test_cv_categories_cover_both_header_forms():
    cats = header_maps.cv_category_columns()
    assert cats["Assay"] == cats["Publication Assay"] == cats["Dataset Assay"] == "assay"
    assert cats["Species"] == cats["Dataset Species"] == "dataset_species"
    assert cats["Resource Media Accessibility"] == "ed_accessibility"
    assert "Tool License" not in cats and "License" not in cats
