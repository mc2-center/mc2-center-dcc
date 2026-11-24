"""CV category maps and entity-type names for the annotation scripts.

The display-name <-> portal-column maps live in ``crosswalk``. Nothing here is
hand-listed except the CV category overrides used by
``annotations/edit_legacy_annotations.py``.
"""

from __future__ import annotations

import crosswalk
import model_contract

# Portal column -> category in all_valid_values.csv, for the columns whose
# vocabularies edit_legacy_annotations.py standardizes.
PORTAL_COLUMN_TO_CV_CATEGORY = {
    "assay": "assay",
    "tumorType": "tumorType",
    "tissue": "tissue",
    "species": "dataset_species",
    "mediaAccessibility": "ed_accessibility",
}


def component_for_entity_type(entity_type: str) -> str:
    """``Publication`` -> ``PublicationView``; ``Resource`` -> ``EducationalResource``."""
    if entity_type in ("Resource", "EducationalResource"):
        return "EducationalResource"
    return entity_type if entity_type.endswith("View") else f"{entity_type}View"


def cv_category_columns() -> dict[str, str]:
    """Manifest header (14.0.0 or 13.1.0 display name) -> CV category.

    Covers every component, so a table of any type can be checked without
    knowing which component it holds.
    """
    out: dict[str, str] = {}
    for component in crosswalk.CROSSWALK:
        legacy = {new: old for old, new in model_contract.RENAMES_13_TO_14.get(component, {}).items()}
        for display, portal in crosswalk.display_to_portal(component).items():
            category = PORTAL_COLUMN_TO_CV_CATEGORY.get(portal)
            if category is None:
                continue
            out[display] = category
            if display in legacy:
                out[legacy[display]] = category
    return out
