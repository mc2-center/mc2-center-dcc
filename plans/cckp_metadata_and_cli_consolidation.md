# A metadata package for CCKP/MC2, plus CLI consolidation for mc2-center-dcc

## Context

This started as a script-consolidation cleanup for `mc2-center-dcc`, but the real need is bigger: a proper Python package for **managing, validating, and consuming CCKP/MC2 metadata** — the LinkML schema, controlled vocabularies, and the knowledge graph `data-models/kg-pipeline` builds — with purpose-built tools on top, not just deduplicated login helpers. `mc2-center-dcc`'s ~40 flat, duplicative scripts (13 separate copy-pasted Synapse-login implementations between the two repos, dead code, stale shell wrappers) still get consolidated into a real CLI, but now as a **consumer** of the new metadata package rather than the main event.

Neither repo has a test suite or lint/format enforcement today. Both are explicit preconditions here, not afterthoughts: the new package gets built test-first, and `ruff` is enforced going forward.

### Decisions (confirmed with user, this round)

- **The new package does full model lifecycle work**: absorbs `data-models`' own model-management scripts (`update_valid_values.py`, `convert_model_to_jsonld.py`, `create_json_from_model.py`) rather than just wrapping them, and adds new capabilities — loading/querying the built knowledge graph, and validating a manifest/CSV against the model. Ontology annotation (`ols_annotate.py`) is folded in too, but **modernized, not lifted as-is** — see the dedicated section below; the current script is genuinely stale against both the `ols-term-annotator` skill and this repo's own `kg-pipeline/scripts/suggest_mappings.py`.
- **It reads `kg-pipeline`'s output, it doesn't replace `kg-pipeline`**: the pipeline's own Makefile, 17 scripts, and 25 targets are completely untouched. The new package treats `kg-pipeline`'s built `.ttl` files as an input to load and query — same relationship as before, just now applying to a package with real substance instead of a login-helper stub.
- **Test suite is a precondition, scoped to the new package** — not a retrofit onto the ~60 existing scripts being touched elsewhere in this plan. `kg-pipeline/test/` (18 fixture-based pytest files, no live Synapse dependency) is the template to follow.
- **`ruff`** for linting + formatting, enforced on the new package and CLI from day one; existing script style is left alone unless a file is actively being rewritten.

### Judgment call from last round — confirmed

Hosting the new package **inside `data-models`** (not a new repo) is settled.

### `ols_annotate.py` is stale — three overlapping tools exist today, checked directly

Verified by diffing the two scripts and reading `kg-pipeline`'s architecture notes, not assumed:

| Tool | Where | What it actually does |
|---|---|---|
| `data-models/scripts/ols_annotate.py` (301 lines) | root scripts | Builds a worklist from a CV CSV (`Attribute`/`Description`/`Ontology Identifier`/`Ontology Url` columns) and applies human decisions back — but **never calls OLS, or any registry, itself.** No `search`/`term`/`ancestors` subcommands exist; a human runs their own lookups outside the tool entirely. |
| `ols-term-annotator` skill's `scripts/ols_annotate.py` (1380 lines) | `~/.claude/skills/ols-term-annotator/` | The mature version of this same workflow: live `search`/`term`/`ancestors` against OLS4, `status` healthcheck, `lov-vocab-search`/`lov-term-search` fallback for vocabularies OLS doesn't index, `spec-fetch` as a tertiary fallback, retry/backoff, response caching. Built for a **different schema shape** (LinkML `enums.yaml`/`namhub.yaml`), not this repo's CV CSVs — the process transfers, the I/O code doesn't. |
| `kg-pipeline/scripts/suggest_mappings.py` (530 lines) | already in `data-models` | Also does live registry queries — `choose_registry()` picks OLS4, ROR (institution names), or SPDX (license identifiers) per-CV automatically — and already operates on this repo's actual CV CSV shape. The closest existing match to what `ols_annotate.py` should be. |

So the absorbed capability in Phase 1 should be built from `suggest_mappings.py`'s registry-choice logic and live-query pattern (already the right shape and already in this repo) plus `ols-term-annotator`'s more complete failure-handling (status check, LOV/spec-fetch fallback, retry/backoff, caching) — not a straight port of the current `ols_annotate.py`, which is the least capable of the three.

### Naming (proposed, not load-bearing)

- Package: `cckp_metadata` (import name), living in `data-models` (e.g. `data-models/cckp_metadata/`, its own `pyproject.toml`).
- CLI command: `mc2model`, installed from the same package.
- `mc2-center-dcc`'s CLI: `mc2dcc`, depending on `cckp_metadata` via `pip install "git+https://github.com/mc2-center/data-models.git#subdirectory=cckp_metadata"` (or repo-root packaging — an implementation detail to settle when building Phase 1, not now).

## Phase 0 — test suite + lint/format, as a precondition

Before any new capability is built:

- Scaffold `pytest` for the new package's location in `data-models`, following `kg-pipeline/test/`'s existing fixture-based style (no live Synapse calls required to run the suite) — `conftest.py`, a `fixtures/` directory, one test file per module as the package grows.
- Add `ruff` (lint + format + import sort) with a repo config (`pyproject.toml` or `ruff.toml`), wired into CI for `data-models` so it runs on PRs touching the new package.
- This phase produces no user-facing functionality — it's the scaffolding Phase 1 gets built on top of.

## Phase 1 — `cckp_metadata`: the model/KG/validation package

Three capability areas, each with both a Python API and a `mc2model` CLI subcommand:

- **Model management** (absorbed, not just wrapped, from today's root-level scripts):
  - `update_valid_values` — from `update_valid_values.py`: reads `modules/mapping.yaml`, rewrites `Valid Values` columns.
  - `collate` — the collation step currently inlined in the root `Makefile` (concatenate `modules/*/annotationProperty.csv` → `mc2.model.csv`).
  - `convert_jsonld` — from `convert_model_to_jsonld.py`.
  - `generate_json_schemas` — from `create_json_from_model.py`.
  - `ols annotate prepare`/`apply` — rebuilt on `kg-pipeline/scripts/suggest_mappings.py`'s registry-choice logic and live OLS4/ROR/SPDX queries, with `ols-term-annotator`'s status-check/LOV-fallback/spec-fetch/retry/caching layered on top; see the dedicated section above. Not a straight port of the current `ols_annotate.py`.
  - The root `Makefile` (4 targets: `all`/`collate`/`convert`/`generate-json`) is deleted once these are real CLI commands — confirmed safe, since CI doesn't currently invoke it at all (`CLAUDE.md` documents a `build-jsonld.yml` workflow that runs `make all`, but that workflow doesn't actually exist in `.github/workflows/` — one of two stale `CLAUDE.md` claims found; both get corrected as part of this phase, along with the removed `make qc` target it still documents).
- **KG consumption** (new capability, doesn't exist anywhere today): a small `rdflib`-based API to load one or more of `kg-pipeline`'s built `.ttl` files (`cckp_kg.ttl`, `cckp_kg_full.ttl`, per-class files) and run lookups against them — e.g. "what ontology term is this Dataset's tumor type mapped to," without hand-writing SPARQL each time. Read-only against files `kg-pipeline` already produces; never triggers a rebuild itself.
- **Validation** (new capability) — broken down, per your question, rather than left as one vague bullet:

  **What `union_qc.py` does today, confirmed by reading it:** it shells out to the external `schematic` CLI (`schematic model ... validate -dt <Component> -mp <path>`) against whatever manifest it's pointed at, deriving `<Component>` (`DatasetView`, `PublicationView`, etc.) from the data's own `Component` column. **Relying on `schematic` at all is itself something to move off of** — `data-models/CLAUDE.md` documents that Sage Bionetworks has announced `schematicpy`'s retirement by end of 2026 in favor of `synapseclient.extensions.curator`, which this repo's own `curator_tools/`/`utils/create_curation_task.py` have already migrated to.

  **What can be validated against today, checked directly:** `data-models/json_schemas/` has one schema per MC2 model **View** class (`DatasetView.json`, `PublicationView.json`, `GrantView.json`, `ToolView.json`, `EducationalResource.json`, ...) — the **staging/raw manifest shape** curators submit. This is exactly what `union_qc.py` already (attempts to) validate against, and it's real: the new package can validate a staging manifest here today, just via the `jsonschema` library against these already-generated files instead of a `schematic` subprocess.

  **What can't be validated yet, and why:** there is **no schema anywhere in the repo for the cleaned/merged portal-table shape** (e.g. "Datasets - Merged" — the columns `sync_datasets.py`'s `clean_table()` renames/restructures into, like `DatasetAlias`/`sourceRepository`/`downloadType`) — confirmed by grepping the whole repo for any schema resource matching that shape. Validating the merged table meaningfully would need a new schema authored for it first — a data-modeling decision, not something this plan can build code for on its own. Flagging this as a real limitation rather than assuming it's in scope: **Phase 1 ships staging-manifest validation** (real, buildable now, replaces the `schematic` subprocess); **merged-portal-table validation is blocked on new model-authoring work**, which would be its own follow-up plan once you decide whether/how to model that shape.

  **Tool choice, corrected** — there is a third, native option worth being precise about: `synapseclient.models.mixins.json_schema.validate_entity_with_json_schema()` (exposed as `Folder.validate_schema()`/`get_invalid_validation()`/`get_schema_validation_statistics()`), backed by Synapse's own `GET /entity/{id}/schema/validation` REST endpoint — a real, first-class JSON-schema validator, checked directly, not dismissed on a guess. It validates an **entity's already-stored annotations against a schema bound to that entity** (`bind_schema()` first) — every validation path in the client, legacy and OOP, is entity/binding-shaped this way; none of them accept an arbitrary local CSV/JSON payload directly. That makes it the wrong fit for `union_qc.py`'s actual job (checking a **staging manifest CSV before it's ever uploaded**) — for that, `jsonschema` against the already-generated `json_schemas/*.json` files is still the right choice, and not a new dependency on `linkml`'s own validator (isolated to `kg-pipeline` deliberately) or `DataModelValidator` (validates the *schema itself* for structural issues, not a manifest's row data).

  **But it's a real candidate for the merged-portal-table side of your question**, once a schema for that shape exists (still the blocker above): if the merged/cleaned data is exposed through schema-bound entities — e.g. an EntityView with a schema bound the way `create_file_based_metadata_task.py` already sets up elsewhere in this ecosystem — `validate_entity_with_json_schema()` gives genuine post-sync validation against the *live* Synapse-side data, which `jsonschema`-against-a-local-file can't do. It fits less naturally if the merged data stays as bare Synapse Table rows with no per-entity annotations to bind a schema to. Worth deciding alongside the schema-authoring work itself, not now.

## Phase 2 — `mc2dcc`: package skeleton + highest-value commands, depending on `cckp_metadata`

Same shape as the original consolidation plan, now explicitly built as a consumer of Phase 1's package rather than owning its own login/helper code:

- `mc2-center-dcc` becomes an installable package (`pyproject.toml`, `console_scripts` entry point `mc2dcc`), depending on `cckp_metadata` for Synapse login (replacing all 7 of that repo's copy-pasted login implementations) and, where it adds real value, manifest validation before a sync (e.g. `mc2dcc sync datasets` can validate the manifest against the model before writing to the portal table — new capability, not just parity with today).
- Resolve the `utils/` naming collision: `portal_tables/utils.py`'s remaining domain logic (`update_table()`, `identify_download_type()`, `CONFIG`, DUO/repo lookup dicts — everything except login, which moves to `cckp_metadata`) becomes `mc2dcc/sync/_shared.py`.
- `mc2dcc sync {datasets,publications,tools,people,projects,education,grants}` — one subcommand per `portal_tables/sync_*.py`, `sync_grants.py` + `create_grant_projects.py`'s `process_new_grants()` folded together.
- `mc2dcc admin tally-themes`, `mc2dcc admin check-publications-status` — the other two CI-run scripts; `tally_themes.py`'s independent second copy of `update_table()` is deleted in favor of the shared one.
- Delete on sight: `portal_tables/unify_grant_tables.py` (already confirmed unused during the synapseclient migration review) and `utils/reset_wg_members.sh` (references a `python/reset_team_members.py` path that doesn't exist anywhere in the repo).
- Update the 3 CI workflows and `run_sync.sh` to call `mc2dcc` instead of `python portal_tables/sync_*.py`.

## Phase 3 — `mc2dcc`: everything else

The remaining ~30 scripts, grouped into command groups, with the duplication found during exploration resolved as part of moving each one — same content as originally scoped:

- `mc2dcc admin create-id-folders` dedupes `utils/create_id_folders.py` and `annotations/create_id_folders.py` (the `utils/` version is canonical; the `annotations/` copy has stale column-name assumptions and leftover debug prints).
- `mc2dcc curation ...` for `create_curation_task.py`/`delete_curation_task.py`/`list_curation_tasks.py`/`synapse_json_schema_bind.py`.
- **`curator_tools/` is deleted, reversing the earlier "leave untouched" call** — checked directly whether each of its 5 files' functionality is actually available elsewhere, per your question, rather than assuming:
  - `create_file_based_metadata_task.py`, `create_record_based_metadata_task.py`, `list_curation_task.py` — **fully redundant, confirmed live**: `utils/create_curation_task.py` and `utils/list_curation_tasks.py` already call the exact same functions directly from `synapseclient.extensions.curator` (`create_file_based_metadata_task`, `create_record_based_metadata_task`, `CurationTask.list()`) in production today.
  - `query_schema_registry.py`, `validate_json_schema.py` — the underlying calls (`synapseclient.extensions.curator.query_schema_registry`, the OOP `Folder.validate_schema()`) are real, available, one-line imports, but **checked and confirmed nothing in this repo currently calls either one** outside `curator_tools/`'s own demo scripts. Deleting these two loses a worked example, not a unique capability — the functions stay fully available to import directly whenever actually needed.
- `mc2dcc portal ...` for `build_table_view.py`, `merge_tables.py`, `add_datasets_to_pub.py`, `union_qc.py` (now calling `cckp_metadata`'s validation function instead of shelling out to `schematic`), `build_tag_lists.py`.
- `mc2dcc annotations ...` for the `annotations/` scripts — several (`gen-mp-csv.py`, `processing-splits.py`, `schema_update.py`) currently skip argparse and read raw `sys.argv`, so this is also where they gain real argument validation for the first time. `upload-workflow.sh`'s 5-step manual pipeline becomes one `mc2dcc annotations upload-workflow` command.
- Flagged, not auto-deleted: `annotations/add_cols.py` looks like a dead one-off CSV-migration helper — confirm with you before dropping it, the way `unify_grant_tables.py` was confirmed before deletion last time.

## Verification

1. Phase 0: `pytest` runs green (empty/scaffold suite) and `ruff check`/`ruff format --check` pass in CI before Phase 1 starts.
2. Phase 1: new tests for every absorbed/new `cckp_metadata` function; `mc2model all` output diffed against a fresh clone's current `make all` output for exact equivalence before the root Makefile is deleted; KG-consumption queries spot-checked against `kg-pipeline`'s documented example queries (`kg-pipeline/README.md`'s "Verified against live data" section); the modernized `ols annotate` run once against a CV with known-good existing mappings to confirm its verdicts agree with the current curated state before it's used to touch anything new; staging-manifest validation spot-checked against a manifest already known to pass (and one known to fail) `schematic`'s current validation, confirming the `jsonschema`-based replacement agrees on both.
3. Phase 2/3: every migrated `mc2dcc` command run with `--dryrun` against live Synapse, diffed against the old script's output — same approach used for the synapseclient migration. The 3 updated CI workflows run once via `workflow_dispatch` before merging. Before `curator_tools/` is deleted, `utils/create_curation_task.py` and `utils/list_curation_tasks.py` are re-run once live to reconfirm they still exercise the same `synapseclient.extensions.curator` functions `curator_tools/` demonstrated, so nothing is deleted on a stale assumption.

Granular commits per phase, implementation report saved to each repo's `plans/` folder — same convention as the synapseclient migration.
