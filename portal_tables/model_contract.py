"""Single model-contract module: the pin to data-models and header conversion.

Every reference to the mc2-center/data-models repository goes through
``MODEL_REF``. Manifests and templates use display names (``GrantView Key``);
schemas and RecordSets use class labels (``GrantViewKey``). Convert between the
two only through :func:`normalize_headers`, which uses the display name <->
label pairs in ``mc2.model.jsonld`` rather than a camel-case rule.

Set ``MC2_MODEL_DIR`` to a local data-models checkout (or a fixture directory
with the same layout) to avoid any network access.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import urllib.error
import urllib.request
from functools import lru_cache
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

MODEL_REF = "v14.0.0"
RAW = f"https://raw.githubusercontent.com/mc2-center/data-models/{MODEL_REF}"
ALL_VALID_VALUES_URL = f"{RAW}/all_valid_values.csv"

CACHE_DIR = Path.home() / ".cache" / "mc2-center-dcc" / MODEL_REF

# Headers that are not model attributes and pass through unchanged.
PASSTHROUGH = frozenset({"Component", "Id", "entityId"})

# Old -> new display names, from the 13.1.0 vs 14.0.0 template diff.
# New in 14.0.0 with no 13.1.0 equivalent: GrantView "Institution Key" and
# "PersonView Key"; PersonView "Institution Key".
RENAMES_13_TO_14: dict[str, dict[str, str]] = {
    "PublicationView": {
        "Publication Assay": "Assay",
        "Publication Tumor Type": "Tumor Type",
        "Publication Tissue": "Tissue",
    },
    "DatasetView": {
        "Dataset Assay": "Assay",
        "Dataset Species": "Species",
        "Dataset Tumor Type": "Tumor Type",
        "Dataset Tissue": "Tissue",
        "Data Use Codes": "Dataset Data Use Codes",
    },
    "ToolView": {"Tool License": "License"},
    "EducationalResource": {"Resource License": "License"},
    "GrantView": {
        "Grant Investigator": "Investigator",
        # values also change: program.* IDs
        "Grant Consortium Name": "Consortium Key",
    },
    "PersonView": {"Person Consortium Name": "Consortium Key"},
}

# Today's admin manifests do not use 13.1.0 template headers. Map their live
# column names (read from Synapse) to 14.0.0 display names. Columns that are
# already valid 14.0.0 headers or passthrough (Component, Id, entityId,
# ProjectView_id, Project Name, ...) are not listed.
LEGACY_ADMIN_HEADERS: dict[str, dict[str, str]] = {
    # PersonView, syn38301033 (camelCase)
    "PersonView": {
        "component": "Component",
        "personGrantNumber": "GrantView Key",
        "personConsortiumName": "Consortium Key",
        "name": "Name",
        "alternativeNames": "Alternative Names",
        "email": "Email",
        "url": "Url",
        "orcidId": "Orcid Id",
        "synapseProfileId": "Synapse Profile Id",
        "lastKnownInstitution": "Last Known Institution",
        "workingGroupParticipation": "Working Group Participation",
        "chairRoles": "Chair Roles",
        "consentForPortalDisplay": "Consent For Portal Display",
        "portalDisplay": "Portal Display",
        "personPublications": "Person Publications",
        "personDatasets": "Person Datasets",
        "personTools": "Person Tools",
    },
    # Project, syn59074382
    "ProjectView": {
        "Project Investigator": "Investigator",
        "Project Consortium Name": "Consortium Key",
        "Project Grant Number": "GrantView Key",
    },
}


# Legacy spelling that pre-dates the 13.1.0 -> 14.0.0 rename table.
LEGACY_EXTRA_RENAMES = {"Publication TumorType": "Publication Tumor Type"}


def grant_column_legacy_name(component: str) -> str:
    """`<Resource> Grant Number` for a component, e.g. `Publication Grant Number`."""
    prefix = "Resource" if component == "EducationalResource" else component.removesuffix("View")
    return f"{prefix} Grant Number"


def rename_legacy_headers(df: pd.DataFrame, component: str) -> pd.DataFrame:
    """Rename 13.1.0 display-name headers to 14.0.0, warning for each.

    Covers ``RENAMES_13_TO_14[component]``, ``<Resource> Grant Number`` ->
    ``GrantView Key`` and ``LEGACY_EXTRA_RENAMES``. Lets manifests written
    before the 14.0.0 migration (infrastructure step I5) pass through
    ``normalize_headers``. Headers already in 14.0.0 form are untouched.
    """
    renames = dict(LEGACY_EXTRA_RENAMES)
    renames[grant_column_legacy_name(component)] = "GrantView Key"
    renames.update(RENAMES_13_TO_14.get(component, {}))
    applied = {}
    for old, new in renames.items():
        if old not in df.columns:
            continue
        if new in df.columns:
            logger.warning("Not renaming %r to %r: target already present", old, new)
            continue
        applied[old] = new
        logger.warning("Renaming legacy column %r to %r", old, new)
    return df.rename(columns=applied)


def fetch(path: str) -> Path:
    """Return a local path for ``path`` in data-models at ``MODEL_REF``.

    With ``MC2_MODEL_DIR`` set, resolve inside that directory. Otherwise
    download once into the cache and reuse it.
    """
    override = os.environ.get("MC2_MODEL_DIR")
    if override:
        local = Path(override) / path
        if not local.exists():
            raise FileNotFoundError(f"{path} not found under MC2_MODEL_DIR={override}")
        return local
    cached = CACHE_DIR / path
    if not cached.exists():
        cached.parent.mkdir(parents=True, exist_ok=True)
        tmp = cached.with_name(cached.name + ".part")
        with urllib.request.urlopen(f"{RAW}/{path}", timeout=60) as resp:
            tmp.write_bytes(resp.read())
        tmp.replace(cached)
    return cached


@lru_cache(maxsize=None)
def _load_model(jsonld_path: Path) -> tuple[dict, dict]:
    """Return (display -> label, label -> node) from the JSON-LD graph.

    The display -> label map holds attribute (property) nodes only: nodes that
    appear as an ``sms:requiresDependency`` of some other node (a template or
    component). Valid-value nodes such as ``Breast`` also carry a display name
    but are never dependencies, so they are not valid column headers. The
    second dict still holds every labelled node.
    """
    graph = json.loads(jsonld_path.read_text(encoding="utf-8"))["@graph"]
    nodes: dict[str, dict] = {}
    attributes: set[str] = set()
    for node in graph:
        label = node.get("rdfs:label")
        if not label:
            continue
        nodes[label] = node
        for dep in node.get("sms:requiresDependency", []):
            attributes.add(dep["@id"].split(":", 1)[-1])
    by_display: dict[str, set[str]] = {}
    for label in attributes:
        display = nodes.get(label, {}).get("sms:displayName")
        if display:
            by_display.setdefault(display, set()).add(label)
    ambiguous = {d: sorted(ls) for d, ls in by_display.items() if len(ls) > 1}
    if ambiguous:
        raise ValueError(f"Display names mapping to more than one label: {ambiguous}")
    return {d: next(iter(ls)) for d, ls in by_display.items()}, nodes


def _model() -> tuple[dict, dict]:
    return _load_model(fetch("mc2.model.jsonld"))


def label_map() -> dict[str, str]:
    """Display name -> class label, from ``mc2.model.jsonld``.

    Attribute nodes only (dependencies of some node); valid-value names such
    as ``Breast`` are excluded. Raises if one display name maps to more than
    one label. ``Component`` has no label and is not included.
    """
    return dict(_model()[0])


def normalize_headers(
    df: pd.DataFrame,
    to: str = "keys",
    passthrough=(),
    legacy_component: str | None = None,
) -> pd.DataFrame:
    """Rename columns between display names and class labels.

    ``to="keys"`` converts display names to class labels; ``to="display"``
    the reverse. Unknown headers raise ``ValueError``; ``Component``, ``Id``,
    ``entityId`` and any names in ``passthrough`` are left unchanged.
    ``legacy_component`` first applies ``LEGACY_ADMIN_HEADERS[component]`` for
    today's admin manifests.
    """
    if to not in ("keys", "display"):
        raise ValueError("to must be 'keys' or 'display'")
    if legacy_component is not None:
        legacy = LEGACY_ADMIN_HEADERS[legacy_component]
        df = df.rename(columns=legacy)
    mapping = label_map()
    if to == "display":
        mapping = {label: display for display, label in mapping.items()}
    keep = PASSTHROUGH | set(passthrough)
    unknown = [c for c in df.columns if c not in keep and c not in mapping]
    if unknown:
        raise ValueError(f"Unknown headers for to={to!r}: {unknown}")
    return df.rename(columns={c: mapping[c] for c in df.columns if c not in keep})


def template_headers(component: str) -> list[str]:
    """Ordered header row of ``templates/<component>.csv``.

    Components without a template (ProjectView, Consortium, Institution,
    Theme) fall back to the schema's property order, mapped back to display
    names, with ``Component`` first.
    """
    try:
        path = fetch(f"templates/{component}.csv")
    except FileNotFoundError:
        return _schema_headers(component)
    except urllib.error.HTTPError as err:
        if err.code != 404:
            raise
        return _schema_headers(component)
    with open(path, newline="", encoding="utf-8") as fh:
        return next(csv.reader(fh))


def _schema_headers(component: str) -> list[str]:
    _, nodes = _model()
    node = nodes.get(component)
    if node is None or "sms:requiresDependency" not in node:
        raise KeyError(f"No template or schema properties for {component!r}")
    headers = ["Component"]
    for dep in node["sms:requiresDependency"]:
        label = dep["@id"].split(":", 1)[-1]
        headers.append(nodes[label]["sms:displayName"])
    return headers


def duo_id_to_code() -> dict[str, str]:
    """DUO term ID (``DUO:0000042``) -> short code (``GRU``)."""
    df = pd.read_csv(fetch("modules/shared/duo.csv"), dtype=str).fillna("")
    df = df[df["Properties"] != ""]
    return dict(zip(df["Attribute"], df["Properties"]))
