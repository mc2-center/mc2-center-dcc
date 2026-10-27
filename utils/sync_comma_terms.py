"""Generate portal_tables/comma_terms.csv from the NLM MeSH descriptor file.

Dev-time tool: downloads the current MeSH ASCII descriptor file (public,
read-only) and writes every descriptor name (``MH =`` line) containing ", ".
Rows from other sources (e.g. terms learned on upload) are kept as they are.
"""

import argparse
import csv
import sys
from datetime import date
from pathlib import Path
from urllib.request import urlopen

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "portal_tables"))
import comma_terms  # noqa: E402

# NLM serves the yearly file under /<year>/asciimesh/; the MESH_FILES/ path returns an error page.
URLS = (
    "https://nlmpubs.nlm.nih.gov/projects/mesh/{y}/asciimesh/d{y}.bin",
    "https://nlmpubs.nlm.nih.gov/projects/mesh/MESH_FILES/asciimesh/d{y}.bin",
)


def descriptor_names(lines) -> list[str]:
    """The ``MH =`` names containing ", ", in file order without repeats."""
    names = [ln[5:].strip() for ln in lines if ln.startswith("MH = ")]
    return list(dict.fromkeys(n for n in names if ", " in n))


def fetch_lines(year: int) -> list[str] | None:
    """The descriptor file's lines for ``year``, or None if no URL serves it."""
    for url in URLS:
        try:
            with urlopen(url.format(y=year), timeout=120) as resp:
                lines = resp.read().decode("utf-8", "replace").splitlines()
        except OSError:
            continue
        if lines and lines[0].startswith("*NEWRECORD"):  # a missing year answers 200 with an HTML page
            return lines
    return None


def build_rows(names: list[str], year: int, existing: list[dict], today: str) -> list[dict]:
    """MeSH rows first (keeping ``added`` of terms already listed), then the other sources' rows as they were."""
    kept = [r for r in existing if not r["source"].startswith("MeSH ")]
    others = {r["term"].casefold() for r in kept}
    prior = {r["term"]: r["added"] for r in existing if r["source"].startswith("MeSH ")}
    mesh = [
        {"term": n, "source": f"MeSH {year}", "added": prior.get(n, today)}
        for n in names
        if n.casefold() not in others
    ]
    return mesh + kept


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=comma_terms.CSV_PATH)
    parser.add_argument("--year", type=int, default=date.today().year, help="first year to try (falls back to earlier years)")
    args = parser.parse_args()

    for year in (args.year, args.year - 1):
        lines = fetch_lines(year)
        if lines:
            break
    else:
        sys.exit(f"No MeSH descriptor file found for {args.year} or {args.year - 1}")
    names = descriptor_names(lines)
    rows = build_rows(names, year, comma_terms.load_terms(args.output), date.today().isoformat())
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=comma_terms.COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} terms to {args.output} (MeSH {year}: {len(names)} descriptor names)")


if __name__ == "__main__":
    main()
