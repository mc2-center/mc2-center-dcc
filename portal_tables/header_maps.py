"""Display-name <-> portal-column maps, derived from the crosswalk.

Replaces the hand-written ``annotations/attribute_dictionary.py``. A manifest
header (display name, e.g. ``Tumor Type``) is mapped to its class label with
``model_contract.label_map()`` and from there to the portal table's column
with ``crosswalk.CROSSWALK``. Nothing here is hand-listed except the CV
category overrides used by ``annotations/edit_legacy_annotations.py``.
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


def _key_to_display() -> dict[str, str]:
    reverse = {label: display for display, label in model_contract.label_map().items()}
    reverse["Component"] = "Component"
    return reverse


def display_to_portal(component: str) -> dict[str, str]:
    """14.0.0 display name -> portal column, for the crosswalk's non-derived columns."""
    reverse = _key_to_display()
    return {
        reverse[source]: portal
        for source, portal, _ in crosswalk.CROSSWALK[component]
        if not source.startswith(crosswalk.DERIVED) and source in reverse
    }


def portal_to_display(component: str) -> dict[str, str]:
    """Portal column -> 14.0.0 display name (inverse of ``display_to_portal``)."""
    return {portal: display for display, portal in display_to_portal(component).items()}


def derived_portal_columns(component: str) -> list[str]:
    """Portal columns the sync scripts compute; they are not manifest attributes."""
    return [
        portal
        for source, portal, _ in crosswalk.CROSSWALK[component]
        if source.startswith(crosswalk.DERIVED)
    ]


def cv_category_columns() -> dict[str, str]:
    """Manifest header (14.0.0 or 13.1.0 display name) -> CV category.

    Covers every component, so a table of any type can be checked without
    knowing which component it holds.
    """
    out: dict[str, str] = {}
    for component in crosswalk.CROSSWALK:
        legacy = {new: old for old, new in model_contract.RENAMES_13_TO_14.get(component, {}).items()}
        for display, portal in display_to_portal(component).items():
            category = PORTAL_COLUMN_TO_CV_CATEGORY.get(portal)
            if category is None:
                continue
            out[display] = category
            if display in legacy:
                out[legacy[display]] = category
    return out
