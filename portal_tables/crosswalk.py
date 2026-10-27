"""Crosswalk from data-model class labels to the portal tables' columns.

This is the only place that maps schema keys (``GrantViewKey``) to the portal
tables' camelCase column names (decision 1). ``CROSSWALK[component]`` is the
ordered list of ``(source, portal_column, kind)`` for one portal table:

* ``source`` is a class-label key, or ``derived:<name>`` for an enrichment
  column the sync script computes. The DataFrame handed to :func:`to_portal`
  holds a column called ``<name>`` for those.
* ``kind`` is ``string``, ``list`` or ``int``.

``check_crosswalk`` compares the crosswalk with the live table (read-only);
``to_portal`` renames, orders and converts a DataFrame for upload.
"""

from __future__ import annotations

import logging

import pandas as pd
from synapseclient import Synapse
from synapseclient.models import Table

import utils

logger = logging.getLogger(__name__)

DERIVED = "derived:"

# Portal columns the crosswalk lists that may not exist in the live table yet.
# They are skipped while absent, and checked in position once present.
OPTIONAL_COLUMNS: dict[str, set[str]] = {
    "ProjectView": {"projectShortName"},  # added by infrastructure step I4
}

# Component -> key into ``utils.CONFIG`` (for the portal table ID).
COMPONENT_RESOURCE = {
    "PublicationView": "publication",
    "DatasetView": "dataset",
    "ToolView": "tool",
    "PersonView": "people",
    "GrantView": "grant",
    "EducationalResource": "education",
    "ProjectView": "project",
}

CROSSWALK: dict[str, list[tuple[str, str, str]]] = {
    "PublicationView": [
        ("PublicationDoi", "doi", "string"),
        ("PublicationJournal", "journal", "string"),
        ("PubmedId", "pubMedId", "int"),
        ("PubmedUrl", "pubMedUrl", "string"),
        ("derived:link", "pubMedLink", "string"),
        ("PublicationTitle", "publicationTitle", "string"),
        ("PublicationYear", "publicationYear", "int"),
        ("PublicationKeywords", "keywords", "string"),
        ("PublicationAuthors", "authors", "string"),
        ("PublicationAbstract", "abstract", "string"),
        ("Assay", "assay", "list"),
        ("TumorType", "tumorType", "list"),
        ("Tissue", "tissue", "list"),
        ("derived:theme", "theme", "list"),
        ("derived:consortium", "consortium", "list"),
        ("GrantViewKey", "grantNumber", "list"),
        ("derived:grantName", "grantName", "list"),
        ("PublicationDatasetAlias", "dataset", "string"),
        ("PublicationAccessibility", "accessibility", "string"),
        ("derived:iconTags", "dataType", "list"),
    ],
    "DatasetView": [
        ("DatasetViewId", "datasetId", "string"),
        ("DatasetName", "datasetName", "string"),
        ("DatasetAlias", "datasetAlias", "string"),
        ("DatasetDescription", "description", "string"),
        ("DatasetDesign", "overallDesign", "string"),
        ("DatasetFileFormats", "fileFormats", "list"),
        ("Assay", "assay", "list"),
        ("Species", "species", "list"),
        ("Tissue", "tissue", "list"),
        ("derived:theme", "theme", "list"),
        ("TumorType", "tumorType", "list"),
        ("derived:consortium", "consortium", "list"),
        ("GrantViewKey", "grantNumber", "list"),
        ("derived:grantName", "grantName", "list"),
        ("PublicationViewKey", "pubMedId", "list"),
        ("derived:pub", "publicationTitle", "list"),
        ("derived:link", "externalLink", "string"),
        ("DatasetDoi", "doi", "string"),
        ("derived:iconTags", "dataType", "list"),
        ("derived:version", "version", "int"),
        ("DatasetDataUseCodes", "accessInformation", "list"),
        ("derived:sourceRepository", "sourceRepository", "string"),
        ("derived:downloadType", "downloadType", "string"),
        ("derived:downloadSynId", "downloadSynId", "string"),
    ],
    "ToolView": [
        ("ToolName", "toolName", "string"),
        ("ToolDescription", "description", "string"),
        ("ToolHomepage", "homepage", "string"),
        ("ToolVersion", "version", "string"),
        ("GrantViewKey", "grantNumber", "list"),
        ("derived:consortium", "consortium", "list"),
        ("derived:theme", "theme", "list"),
        ("PublicationViewKey", "pubMedId", "list"),
        ("DatasetViewKey", "datasets", "list"),
        ("ToolOperation", "operation", "list"),
        ("ToolInputData", "inputData", "list"),
        ("ToolOutputData", "outputData", "list"),
        ("ToolInputFormat", "inputFormat", "list"),
        ("ToolOutputFormat", "outputFormat", "list"),
        ("ToolFunctionNote", "functionNote", "string"),
        ("ToolCmd", "cmd", "string"),
        ("ToolType", "toolType", "list"),
        ("ToolTopic", "topic", "list"),
        ("ToolOperatingSystem", "operatingSystem", "list"),
        ("ToolLanguage", "language", "list"),
        ("License", "license", "list"),  # STRING live until I4, then STRING_LIST
        ("ToolCost", "cost", "string"),
        ("ToolAccessibility", "accessibility", "string"),
        ("ToolDownloadUrl", "downloadUrl", "string"),
        ("derived:link", "downloadLink", "string"),
        ("ToolDownloadType", "downloadType", "list"),
        ("ToolDownloadNote", "downloadNote", "string"),
        ("ToolDownloadVersion", "downloadVersion", "string"),
        ("ToolDocumentationUrl", "documentationUrl", "string"),
        ("ToolDocumentationType", "documentationType", "list"),
        ("ToolDocumentationNote", "documentationNote", "string"),
        ("ToolLinkUrl", "linkUrl", "string"),
        ("ToolLinkType", "linkType", "string"),
        ("ToolLinkNote", "linkNote", "string"),
        ("derived:portalDisplay", "portalDisplay", "string"),
        ("ToolDoi", "doi", "string"),
        ("derived:synapseLink", "synapseLink", "string"),
        ("ToolDateLastModified", "toolDateLastModified", "string"),
        ("ToolReleaseDate", "toolReleaseDate", "string"),
        ("ToolPackageDependencies", "toolPackageDependencies", "string"),
        ("ToolPackageDependenciesPresent", "toolPackageDependenciesPresent", "string"),
        ("ToolComputeRequirements", "toolComputeRequirements", "string"),
        ("ToolEntityName", "toolEntityName", "string"),
        ("ToolEntityType", "toolEntityType", "string"),
        ("ToolEntityRole", "toolEntityRole", "string"),
        ("derived:iconTags", "dataType", "list"),
    ],
    "PersonView": [
        ("Name", "name", "string"),
        ("AlternativeNames", "alternativeNames", "list"),
        ("Email", "email", "string"),
        ("SynapseProfileId", "synapseProfileId", "string"),
        ("derived:link", "synapseProfileLink", "string"),
        ("Url", "url", "string"),
        ("OrcidId", "orcidId", "string"),
        ("LastKnownInstitution", "lastKnownInstitution", "string"),
        ("derived:grantName", "grantName", "list"),
        ("GrantViewKey", "grantNumber", "list"),
        ("derived:consortium", "consortium", "list"),
        ("WorkingGroupParticipation", "workingGroupParticipation", "list"),
        ("ChairRoles", "chairRoles", "string"),
        ("PersonPublications", "publicationId", "list"),  # STRING live
        ("PersonDatasets", "datasetId", "list"),  # STRING live
        ("PersonTools", "toolId", "list"),  # STRING live
        ("ConsentForPortalDisplay", "consentForPortalDisplay", "string"),
        ("PortalDisplay", "portalDisplay", "string"),
    ],
    "GrantView": [
        ("derived:grantId", "grantId", "string"),
        ("GrantViewId", "grantViewId", "string"),
        ("GrantName", "grantName", "string"),
        ("GrantNumber", "grantNumber", "string"),
        ("GrantAbstract", "abstract", "string"),
        ("GrantType", "grantType", "string"),
        ("GrantThemeName", "theme", "list"),
        ("GrantInstitutionAlias", "institutionAlias", "list"),
        ("GrantInstitutionName", "grantInstitution", "list"),
        ("Investigator", "investigator", "list"),  # STRING live until I4
        ("derived:consortium", "consortium", "list"),
        ("GrantStartDate", "grantStartDate", "string"),
        ("NIHRePORTERLink", "nihReporterLink", "string"),
        ("DurationofFunding", "durationOfFunding", "int"),
        ("EmbargoEndDate", "embargoEndDate", "string"),
        ("derived:grantSynapseTeam", "grantSynapseTeam", "string"),
        ("derived:grantSynapseProject", "grantSynapseProject", "string"),
    ],
    "EducationalResource": [
        ("Component", "component", "string"),
        ("ResourceTitle", "title", "string"),
        ("ResourceLink", "link", "string"),
        ("ResourceTopic", "topic", "list"),
        ("ResourceActivityType", "activityType", "list"),
        ("ResourcePrimaryFormat", "primaryFormat", "list"),
        ("ResourceIntendedUse", "intendedUse", "list"),
        ("ResourcePrimaryAudience", "primaryAudience", "list"),
        ("ResourceEducationalLevel", "educationalLevel", "list"),
        ("ResourceDescription", "description", "string"),
        ("ResourceOriginInstitution", "originInstitution", "list"),
        ("ResourceLanguage", "language", "list"),
        ("ResourceContributors", "contributors", "list"),
        ("GrantViewKey", "grantNumber", "list"),
        ("ResourceSecondaryTopic", "secondaryTopic", "list"),
        ("License", "license", "list"),  # STRING live until I4
        ("ResourceUseRequirements", "useRequirements", "string"),
        ("ResourceAlias", "alias", "string"),
        ("ResourceInternalIdentifier", "internalIdentifier", "string"),
        ("ResourceMediaAccessibility", "mediaAccessibility", "list"),
        ("ResourceAccessHazard", "accessHazard", "list"),
        ("ResourceDatasetAlias", "datasetLink", "string"),
        ("ResourceToolLink", "toolLink", "string"),
        ("ResourceDoi", "doi", "string"),
        ("derived:synapseLink", "synapseLink", "string"),
        ("PublicationViewKey", "publicationId", "list"),
        ("derived:iconTags", "dataType", "list"),
    ],
    "ProjectView": [
        ("ProjectName", "projectName", "string"),
        ("ProjectType", "projectType", "string"),
        ("ProjectDescription", "description", "string"),
        ("Investigator", "investigator", "list"),  # STRING live until I4
        ("derived:theme", "theme", "list"),
        ("derived:consortium", "consortium", "list"),
        ("GrantViewKey", "grant", "list"),
        ("derived:grantName", "grantName", "list"),
        ("derived:grantType", "grantType", "list"),
        ("ProjectShortName", "projectShortName", "string"),
    ],
}


def is_derived(source: str) -> bool:
    return source.startswith(DERIVED)


def source_column(source: str) -> str:
    """Name of the DataFrame column holding ``source`` (drops ``derived:``)."""
    return source[len(DERIVED):] if is_derived(source) else source


def table_id(component: str) -> str:
    """Portal table ID of ``component``, from ``utils.CONFIG``."""
    return utils.CONFIG[COMPONENT_RESOURCE[component]]["portal_table"]


def _type_name(column_type) -> str:
    """``ColumnType.STRING_LIST`` / enum / ``"STRING_LIST"`` -> ``"STRING_LIST"``."""
    return str(getattr(column_type, "value", column_type)).split(".")[-1]


def _entries(component: str) -> list[tuple[str, str, str]]:
    try:
        return CROSSWALK[component]
    except KeyError:
        raise KeyError(
            f"No crosswalk for component {component!r}; known: {sorted(CROSSWALK)}"
        ) from None


def expected_columns(component: str, live_columns) -> list[str]:
    """Crosswalk portal columns, minus optional ones not in ``live_columns``."""
    optional = OPTIONAL_COLUMNS.get(component, set())
    live = set(live_columns)
    return [
        col for _, col, _ in _entries(component) if col not in optional or col in live
    ]


def check_live_columns(
    table_id: str, component: str, live_types: dict[str, str]
) -> dict[str, str]:
    """Assert live column names and order match the crosswalk.

    ``live_types`` maps live column name -> type, in live order. Trailing live
    columns in ``utils.OPTIONAL_TRAILING_COLUMNS`` are tolerated. Returns
    ``{portal_column: live_type}`` for the crosswalk's columns.
    """
    live = list(live_types)
    while live and live[-1] in utils.OPTIONAL_TRAILING_COLUMNS:
        live.pop()
    want = expected_columns(component, live)
    if live != want:
        raise ValueError(
            f"{table_id} ({component}) does not match the crosswalk.\n"
            f"  live only:      {[c for c in live if c not in want]}\n"
            f"  crosswalk only: {[c for c in want if c not in live]}\n"
            f"  live order:      {live}\n"
            f"  crosswalk order: {want}"
        )
    return {col: _type_name(live_types[col]) for col in want}


def live_column_types(syn: Synapse, table_id: str) -> dict[str, str]:
    """Live column name -> type name, in table order (read-only)."""
    table = Table(id=table_id).get(include_columns=True, synapse_client=syn)
    return {name: _type_name(col.column_type) for name, col in table.columns.items()}


def check_crosswalk(syn: Synapse, table_id: str, component: str) -> dict[str, str]:
    """Check the live table against the crosswalk; return its column types."""
    return check_live_columns(table_id, component, live_column_types(syn, table_id))


def to_portal(df: pd.DataFrame, component: str, live_types: dict[str, str]) -> pd.DataFrame:
    """Rename, order and convert ``df`` into the portal table's shape.

    ``df`` must hold a column for every crosswalk source (``derived:x`` is
    column ``x``); other columns are dropped. ``list``-kind columns become
    lists only where the live type is ``STRING_LIST``, and only string cells
    are converted: cells that are already lists pass through untouched.
    ``int``-kind columns become nullable integers where the live type is
    ``INTEGER`` (empty string means missing; any other non-integer raises).
    An optional column that exists live but has no source column in ``df``
    (the manifest does not carry it yet) is filled with empty strings.
    """
    optional = OPTIONAL_COLUMNS.get(component, set())
    entries = [e for e in _entries(component) if e[1] not in optional or e[1] in live_types]
    missing = [
        source_column(s)
        for s, col, _ in entries
        if source_column(s) not in df.columns and col not in optional
    ]
    if missing:
        raise KeyError(f"{component}: DataFrame is missing columns {missing}")

    out = pd.DataFrame(index=df.index)
    for source, portal_col, kind in entries:
        if source_column(source) in df.columns:
            col = df[source_column(source)]
        else:  # optional column that exists live, but the manifest lacks it
            logger.warning(
                "%s: no source column %r for optional column %r; leaving it empty",
                component, source_column(source), portal_col,
            )
            col = pd.Series("", index=df.index, dtype=object)
        live_type = _type_name(live_types.get(portal_col, "STRING"))
        if kind == "list" and live_type == "STRING_LIST":
            is_str = col.map(lambda v: isinstance(v, str))
            if is_str.any():
                col = col.astype(object).copy()
                col[is_str] = utils.convert_to_stringlist(col[is_str])
        elif kind == "int" and live_type == "INTEGER":
            col = pd.to_numeric(col.replace("", pd.NA), errors="raise").astype("Int64")
        if kind != "int" and not (kind == "list" and live_type == "STRING_LIST"):
            # A STRING column (a list kind live as STRING, or a string kind such as chairRoles) cannot hold a
            # list: list cells (RecordSet manifests) are joined with ", "; string cells stay.
            col = col.map(lambda v: ", ".join(str(i) for i in v) if isinstance(v, (list, tuple)) else v)
        out[portal_col] = col
    return out
