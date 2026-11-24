"""Generate portal_tables/duo_terms.csv from the governanceDUO LinkML model.

Dev-time tool: reads DataUseModifierEnum in governanceDUO's linkml/mixins.yaml
and writes the portal access text and short code for each data use term.
portal_tables/utils.py loads the CSV at runtime, so PyYAML is not needed there.
"""

import argparse
import csv
import re
import subprocess
import sys
from pathlib import Path

import yaml

MIXINS = Path("linkml") / "mixins.yaml"
COLUMNS = ["id", "shorthand", "label", "portal_text", "source"]
GUIDANCE = re.compile(r"\s*If providing this term\b.*$")


def load_enum(mixins_path: Path) -> dict:
    """Return the DataUseModifierEnum permissible values, or exit if absent."""
    with open(mixins_path) as f:
        schema = yaml.safe_load(f)
    try:
        return schema["enums"]["DataUseModifierEnum"]["permissible_values"]
    except (KeyError, TypeError):
        sys.exit(f"DataUseModifierEnum not found in {mixins_path}")


def build_rows(values: dict, source: str) -> list[dict]:
    """One row per permissible value, in enum order."""
    rows = []
    for term_id, spec in values.items():
        spec = spec or {}
        text = " ".join(str(spec.get("description", "")).split())
        text = GUIDANCE.sub("", text).strip()
        if not text:
            sys.exit(f"Empty portal_text for {term_id}")
        label = text.split(" - ", 1)[0] if " - " in text and str(term_id).startswith("DUO:") else ""
        rows.append(
            {
                "id": term_id,
                "shorthand": (spec.get("annotations") or {}).get("duo_shorthand", ""),
                "label": label,
                "portal_text": text,
                "source": source,
            }
        )
    return rows


def git_source(root: Path) -> str:
    """Provenance string: governanceDUO short commit plus the file path."""
    commit = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "--short", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    return f"governanceDUO@{commit} linkml/mixins.yaml"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--governance-duo", required=True, type=Path, help="governanceDUO checkout root")
    parser.add_argument("--output", type=Path, default=Path("portal_tables/duo_terms.csv"))
    parser.add_argument("--source", help="provenance text (default: from git rev-parse in the checkout)")
    args = parser.parse_args()

    values = load_enum(args.governance_duo / MIXINS)
    rows = build_rows(values, args.source or git_source(args.governance_duo))
    with open(args.output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} terms to {args.output}")


if __name__ == "__main__":
    main()
