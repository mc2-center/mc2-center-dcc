"""Value normalization for the grant-table migration (I5a / I8).

13.1.0 grant manifests hold display-name values; 14.0.x wants the controlled
vocabulary. :class:`Normalizer` rewrites one frame (class-label headers, string
cells; list columns are split into Python lists) and records every change as
``key, column, from, to, rule``:

* ``override``: a curated correction (``column, current_value, proposed_value``);
  it wins over every other rule.
* ``enum_case``: a case-insensitive or whitespace-trimmed match of a valid value.
* ``nonpreferred``: a nonpreferred term replaced by its preferred value
  (``all_valid_values.csv``).
* ``duo_code``: a short data use code (``RTN``) replaced by its DUO ID.
* ``license_blanked``: a License value with no SPDX match is dropped (O6).
* ``truncated``: an enum or list cell ending ``(Read more on Pubmed)``, cut off
  by an old export; reported with the value unchanged (not unmapped), it needs
  recovering from the source record.
* ``doi``: a DOI rewritten to ``https://doi.org/...`` (D-Q1).
* ``doi_placeholder``: ``No DOI Listed`` / ``DOI Not Available`` blanked (D-Q22).

An exact valid value is kept and not recorded. A value no rule can map is left
unchanged and reported as unmapped (except licenses, which are blanked).
List cells are handled per item; items are split on commas (no valid value
contains a comma). The nonpreferred terms of ``all_valid_values.csv`` are joined
with ", " too, so a term that itself holds a comma is split into fragments;
only fragments that look like a term are kept (:func:`acceptable_fragment`) and
the rest are reported in ``rejected_synonyms.csv``.
"""

from __future__ import annotations

import json
import logging
import re

import pandas as pd

import model_contract
import recordsets

logger = logging.getLogger(__name__)
CHANGE_COLUMNS = ["key", "column", "from", "to", "rule"]
REJECTED_COLUMNS = ["column", "valid_value", "fragment", "cell", "reason"]
UNMAPPED_COLUMNS = ["key", "column", "value"]

DOI_PLACEHOLDERS = frozenset({"no doi listed", "doi not available"})
TRUNCATED_SUFFIX = "(Read more on Pubmed)"
DOI_PREFIX = "https://doi.org/"
_DOI_RE = re.compile(r"(10\.\d{4,9}/\S+)", re.IGNORECASE)
_DUO_VALID = re.compile(r"^(DUO:\d{7}|DUOPlus\d+|Pending Annotation)$")


def fold(value: str) -> str:
    """Comparison form: case-folded with runs of whitespace collapsed."""
    return " ".join(str(value).split()).casefold()


def split_items(text: str) -> list[str]:
    """Split a list cell on commas (valid values contain none); a JSON-array cell is parsed instead."""
    text = "" if text is None else str(text)
    stripped = text.strip()
    if stripped.startswith("[") and stripped.endswith("]"):
        try:
            parsed = json.loads(stripped)
        except ValueError:
            parsed = None
        if isinstance(parsed, list):
            return [str(v).strip() for v in parsed if str(v).strip()]
    return [p.strip() for p in re.split(r",\s*", text) if p.strip()]


def acceptable_fragment(fragment: str) -> bool:
    """Whether a fragment of a split nonpreferred cell can stand as a synonym.

    At least two words, or a single token that looks like an identifier or
    acronym: it holds a digit, a hyphen, or an uppercase letter after its first
    character (``RNA-seq``, ``snATAC``, ``HCC``). Plain words such as ``Kidney``
    or ``unclassified`` are the tail of a comma-containing term, not synonyms.
    """
    words = fragment.split()
    if len(words) >= 2:
        return True
    token = fragment.strip()
    return bool(token) and (any(c.isdigit() for c in token) or "-" in token
                            or any(c.isupper() for c in token[1:]))


class EnumMap:
    """Valid values and nonpreferred terms of one controlled vocabulary."""

    def __init__(self, valid_values, nonpreferred=None):
        self.valid = set(valid_values)
        self.folded, self.collisions = {}, []
        for v in sorted(self.valid):  # deterministic; two values with one folded form map to neither
            f = fold(v)
            if f in self.folded and self.folded[f] != v:
                self.collisions.append((self.folded[f], v))
            self.folded.setdefault(f, v)
        for first, second in self.collisions:
            self.folded.pop(fold(first), None)
        # nonpreferred terms that name two different preferred values are ambiguous: not mapped
        seen: dict[str, set] = {}
        pairs = nonpreferred.items() if isinstance(nonpreferred, dict) else (nonpreferred or [])
        for term, preferred in pairs:
            seen.setdefault(fold(term), set()).add(preferred)
        self.ambiguous = {t for t, p in seen.items() if len(p) > 1}
        self.nonpreferred = {t: next(iter(p)) for t, p in seen.items() if len(p) == 1}

    def lookup(self, value: str):
        """``(new_value, rule)``; ``rule`` is None for a kept exact value, ``"unmapped"`` for no match."""
        if value in self.valid:
            return value, None
        key = fold(value)
        if key in self.folded:
            return self.folded[key], "enum_case"
        if key in self.nonpreferred:
            return self.nonpreferred[key], "nonpreferred"
        return value, "unmapped"


def load_enum_maps(component: str, rejected: list | None = None, ref: str | None = None) -> dict[str, EnumMap]:
    """``{column key: EnumMap}`` for the component's enum properties.

    Each ``enum`` / ``items.enum`` of the 14.0.x schema is matched to the
    ``all_valid_values.csv`` category that shares most of its values; a category
    covering under 80 % of the enum is ignored (the column is then unmapped).
    ``ref`` is the data-models tag the files are read at (default ``MODEL_REF``).
    Nonpreferred fragments that fail :func:`acceptable_fragment` are not mapped and
    are appended to ``rejected`` (``column, valid_value, fragment, cell, reason``).
    """
    values = pd.read_csv(model_contract.fetch("all_valid_values.csv", ref=ref), dtype=str).fillna("")
    categories = {}
    for category, group in values.groupby("category"):
        nonpreferred, rejected_here = [], []
        for valid, terms in zip(group["valid_value"], group["nonpreferred_values"]):
            if not terms:
                continue
            pieces = terms.split(", ")  # a term that itself holds ", " is split too (unrecoverable)
            for term in pieces:
                if len(pieces) > 1 and not acceptable_fragment(term):
                    rejected_here.append((valid, term, terms))
                    continue
                nonpreferred.append((term, valid))
        categories[category] = (set(group["valid_value"]), nonpreferred, rejected_here)
    with open(model_contract.fetch(f"json_schemas/{component}.json", ref=ref), encoding="utf-8") as fh:
        properties = json.load(fh)["properties"]
    maps = {}
    for column, prop in properties.items():
        enum = prop.get("enum") or prop.get("items", {}).get("enum")
        if not enum:
            continue
        enum = set(enum)
        best = max(categories, key=lambda c: len(categories[c][0] & enum))
        if len(categories[best][0] & enum) < 0.8 * len(enum):
            continue
        valid, nonpreferred, rejected_here = categories[best]
        if rejected is not None:
            rejected += [{"column": column, "valid_value": v, "fragment": f, "cell": cell,
                          "reason": "single word, not an identifier or acronym"} for v, f, cell in rejected_here]
        # Terms that are valid values of the same enum are never nonpreferred: a
        # fragment of a comma-containing nonpreferred term (``NOS``) must not map.
        folded_valid = {fold(v) for v in valid}
        nonpreferred = [(t, v) for t, v in nonpreferred if fold(t) not in folded_valid]
        maps[column] = EnumMap(enum, nonpreferred)
        for first, second in maps[column].collisions:
            logger.warning("%s: values %r and %r differ only by case; case-insensitive matching skips both", column, first, second)
            if rejected is not None:
                rejected.append({"column": column, "valid_value": first, "fragment": "", "cell": "",
                                 "reason": f"case-fold collision with {second!r}"})
    return maps


def normalize_doi(value: str):
    """``(new, rule)`` for a DOI cell; ``rule`` is ``"unmapped"`` when it holds no DOI."""
    text = value.strip()
    # a placeholder may already carry the prefix ("https://doi.org/No DOI listed"), or be only the prefix
    bare = re.sub(r"^https?://(dx\.)?doi\.org/", "", text, flags=re.IGNORECASE).strip().casefold()
    if bare in DOI_PLACEHOLDERS or (bare == "" and text != ""):
        return "", "doi_placeholder"
    match = _DOI_RE.search(text)
    if not match:
        return value, "unmapped"
    new = DOI_PREFIX + match.group(1)
    return new, (None if new == value else "doi")


def load_overrides(path) -> dict[tuple[str, str], str]:
    """``{(column key, current_value): proposed_value}`` from a curated CSV.

    The ``column`` may be a class label or a display name.
    """
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = {"column", "current_value", "proposed_value"} - set(df.columns)
    if missing:
        raise ValueError(f"{path} lacks column(s) {sorted(missing)}")
    labels = model_contract.label_map()
    return {
        (labels.get(row["column"].strip(), row["column"].strip()), row["current_value"].strip()):
        row["proposed_value"].strip()
        for _, row in df.iterrows()
    }


class Normalizer:
    """Apply every rule to a frame of one component."""

    def __init__(self, component, enum_maps, code_to_id, overrides=None, list_cols=()):
        self.component = component
        self.enum_maps = enum_maps
        self.code_to_id = code_to_id
        self.overrides = overrides or {}
        self.list_cols = set(list_cols)
        self.rejected = pd.DataFrame(columns=REJECTED_COLUMNS)

    @classmethod
    def for_component(cls, component: str, overrides=None, ref: str | None = None):
        import sync_datasets

        rejected: list = []
        norm = cls(
            component,
            load_enum_maps(component, rejected, ref),
            sync_datasets.duo_code_to_id(),
            overrides,
            recordsets.list_columns(component),
        )
        norm.rejected = pd.DataFrame(rejected, columns=REJECTED_COLUMNS).drop_duplicates()
        return norm

    # -- one value ---------------------------------------------------------

    def _value(self, column: str, value: str):
        """``(new, rule)``; ``rule`` None keeps ``value``, ``"unmapped"`` leaves it and reports it."""
        if (column, value) in self.overrides:
            return self.overrides[(column, value)], "override"
        if value.endswith(TRUNCATED_SUFFIX) and (column in self.enum_maps or column in self.list_cols):
            return value, "truncated"
        if column.endswith("Doi"):
            return normalize_doi(value)
        if column == "DatasetDataUseCodes":
            if _DUO_VALID.match(value):
                return value, None
            if value in self.code_to_id:
                return self.code_to_id[value], "duo_code"
            return value, "unmapped"
        enum = self.enum_maps.get(column)
        if enum is None:
            return value, None
        new, rule = enum.lookup(value)
        if rule == "unmapped" and column == "License":
            return "", "license_blanked"
        return new, rule

    def normalize_cell(self, key, column: str, cell, changes: list, unmapped: list):
        """The normalized cell: a list for list columns, else a string."""
        text = "" if cell is None or (not isinstance(cell, (str, list)) and pd.isna(cell)) else cell
        if column in self.list_cols:
            if isinstance(text, str) and (column, text.strip()) in self.overrides:
                # the whole cell is overridden before its items are looked at
                proposed = self.overrides[(column, text.strip())]
                changes.append({"key": key, "column": column, "from": text.strip(), "to": proposed, "rule": "override"})
                return split_items(proposed)
            items = text if isinstance(text, list) else split_items(text)
            out = []
            for item in items:
                new = self._item(key, column, str(item).strip(), changes, unmapped)
                if new and new not in out:
                    out.append(new)
            return out
        text = str(text)
        if not text.strip():
            return ""
        return self._item(key, column, text.strip(), changes, unmapped)

    def _item(self, key, column, value, changes, unmapped):
        if value == "":
            return ""
        new, rule = self._value(column, value)
        if rule == "license_blanked":
            unmapped.append({"key": key, "column": column, "value": value})  # blanked and reported (O6)
        if rule == "truncated":
            changes.append({"key": key, "column": column, "from": value, "to": value, "rule": rule})
            return value
        if rule == "unmapped":
            unmapped.append({"key": key, "column": column, "value": value})
            return value
        if rule is not None and new != value:
            changes.append({"key": key, "column": column, "from": value, "to": new, "rule": rule})
        return new

    # -- a frame -----------------------------------------------------------

    def normalize_frame(self, df: pd.DataFrame, key_col: str):
        """``(normalized frame, changes, unmapped)``; the last two are DataFrames."""
        changes: list[dict] = []
        unmapped: list[dict] = []
        out = df.copy().astype(object)
        for column in out.columns:
            if column == key_col:
                continue
            out[column] = [
                self.normalize_cell(str(k).strip(), column, cell, changes, unmapped)
                for k, cell in zip(df[key_col], df[column])
            ]
        return (
            out,
            pd.DataFrame(changes, columns=CHANGE_COLUMNS),
            pd.DataFrame(unmapped, columns=UNMAPPED_COLUMNS),
        )
