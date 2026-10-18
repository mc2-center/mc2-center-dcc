# Pipeline Orchestration Plan

**Status: planning only — nothing in this directory is implemented yet.** The
scripts referenced below are expected to change soon, so this document
describes the *stages and checkpoints* the eventual orchestrator needs to
cover, not a pinned implementation against today's exact script names/args.

## Goal

Today, getting a new batch of publications/datasets/tools from
[`ai-curation-pipeline`](https://github.com/mc2-center/ai-curation-pipeline)
all the way onto the live CCKP portal is a three-phase, multi-repo, entirely
manual process, run by hand across ~15 scripts with no single entry point and
no automatic record of what ran, when, or with what output. This plan is for
a single-command orchestrator that chains all three phases together, saves
every stage's output locally as it runs, and pauses at the points that
genuinely need a human.

## The three phases

### Phase 1 — `ai-curation-pipeline` (no changes needed)

Already a self-contained CLI (`run-publication-pipeline`). Produces, per run,
in `output/<date>/`:

- `publication_metadata.csv`
- `datasets_publication_metadata.csv`
- `tools_publication_metadata.xlsx`

Manual QC against these (the `review_app.py` Streamlit tool) happens here,
before Phase 2 starts. This is itself a checkpoint, just one that already has
tooling built for it.

### Phase 2 — upload-workflow (`mc2-center-dcc/annotations/`)

Gets new rows from Phase 1's output into Synapse's **per-grant source
tables**. Run once per component (PublicationView / DatasetView / ToolView):

1. `split_manifest_grants.py` — split the manifest by grant number into
   per-grant CSVs.
2. `gen-mp-csv.py` — read-only Synapse query; resolves each grant number to
   its target folder/project Synapse ID, produces a paths CSV.
3. `processing-splits.py` — reformat each split file to match the target
   table's column order/names.
4. *(`schema_update.py` is **not** part of the default flow — it only
   standardizes column types/sizes on already-existing columns in a
   per-grant table, writes directly to shared table schemas, and its own
   docs say it doesn't need to run every time. Only invoke it if a later
   step reports a schema-type mismatch.)*
5. `create_entity_links.py` — creates real Synapse entities (for
   DatasetView, full GEO/SRA Dataset entities via the `geo_synapse`
   pipeline; for ToolView, plain File link entities) and writes the
   resulting entity IDs back into each split file's primary-key column.
   Required for DatasetView/ToolView; not needed for PublicationView.
6. `upload-manifests.py` — validates each split file against the model via
   `schematic`, then submits (upserts) valid ones into the per-grant table.

### Phase 3 — union-table sync (`mc2-center-dcc/portal_tables/` + `utils/`)

Gets data from the per-grant source tables (as updated by Phase 2, or
anything else that landed there independently) into the **live portal
tables**. Run once per component:

1. `merge_tables.py` — merges per-grant source tables into the component's
   UNION table.
2. `union_qc.py -m -s -db` — validates the UNION table against the live
   portal table and the QC model variant, producing `<component>_updated.csv`
   (new/changed rows) and a validation log.
3. **Human review and correction** of `_updated.csv` against the validation
   log, then a re-validation pass (`union_qc.py -tp <updated.csv>` or
   `schematic model validate`) to confirm the fixes land.
4. `merge_and_correct_manifests.py` — folds `_updated.csv` into
   `<component>_merged.csv`, producing `<component>_merged_corrected.csv`.
5. `build_tag_lists.py` — PublicationView and DatasetView only. Adds content
   tags for the CCKP's card-icon feature.
6. Upload the tagged/corrected CSV to the `release_validation` Synapse
   folder (`syn53461903`), replacing the existing file for that component.
7. `run_sync.sh` — syncs `release_validation` into the live portal tables.

## Required manual checkpoints

These can't be automated away; the orchestrator should pause and print
exactly what to look at, rather than silently continuing or silently
failing:

1. **After Phase 1**: curator QC via `review_app.py`, before any Synapse
   upload happens at all.
2. **After Phase 3 step 2** (`union_qc.py`): review `_updated.csv` +
   validation log, apply fixes. This is real judgment work — e.g. deciding
   whether a flagged `Pan-cancer` value is a genuine cap-collapse or an
   actual gap, or whether a term needs backpopulating vs. remapping to an
   existing vocab entry.
3. **Before Phase 3 step 6** (upload to `release_validation`): this is
   effectively "publish" — final human sign-off before the next
   `run_sync.sh` makes it live.

## Known gaps to design around

Found while running this manually end-to-end on 2026-10-05:

- **Environment**: scripts that shell out to `schematic` (e.g.
  `upload-manifests.py`) need it on `PATH`. Invoking a venv's `python` by
  absolute path does *not* put the venv's `bin/` on `PATH` for subprocesses —
  only `source venv/bin/activate` does. The orchestrator must guarantee this
  for every subprocess it spawns, not just the top-level process.
- **Credentials for cross-manifest validation**: `schematic model validate`
  needs real Synapse credentials to check cross-manifest references (e.g. a
  DatasetView row's `PublicationView Key` actually existing). An interactive
  `synapseclient.login()` in one process does not reliably propagate to a
  `subprocess.run(["schematic", ...])` call in another. The orchestrator
  needs to pass credentials through explicitly (e.g. `SYNAPSE_AUTH_TOKEN` in
  the subprocess environment), not rely on ambient login state.
- **Empty link columns crash entity creation**: `create_entity_links.py`'s
  plain-link path (`create_links()`) has no handling for an empty
  `Tool Homepage` / link column — it will try to create a
  `File(path="", ...)` entity, which is unlikely to behave well and isn't
  wrapped in error handling, risking a partial-batch failure. Rows with an
  empty link column need to be filtered out before this step, and any
  per-grant split file that becomes empty as a result needs to be dropped
  entirely rather than left as a header-only CSV (`schematic` is known to
  crash on empty manifests).
- **GEO dataset entities are not idempotent by content, only by name within
  a run**: `geo_synapse.create_synapse_dataset()` always creates a new
  Dataset entity; it doesn't check Synapse for an existing one with the same
  accession first. Synapse's own `Dataset.store()` appears to dedupe
  same-named entities created *within the same script invocation* (verified:
  a GEO accession linked to two different grants in the same run correctly
  reused one entity), but there's no check against entities from a *prior*
  run. Confirmed this wasn't an issue for the 2026-10-05 batch (checked all
  45 target accessions against the existing `datasets` folder in the central
  project before running — zero overlap), but the orchestrator should make
  this check automatic rather than manual.
- **`merge_and_correct_manifests.py` had two correctness bugs** (fixed in
  [mc2-center/mc2-center-dcc#168](https://github.com/mc2-center/mc2-center-dcc/pull/168)):
  it was keeping the stale `Database`-sourced row over the corrected
  `Updated`-sourced row whenever both existed for a key, and reading the
  base database CSV without `dtype=str` caused `DataFrame.update()` to never
  align on a numeric-vs-string primary key. Together, these meant **every**
  correction made to an existing publication/dataset/tool's row was silently
  discarded on every run, for as long as both bugs were present. The
  orchestrator's Phase 3 step 4 must run against a version of this script
  with both fixes merged, and should ideally assert afterward that a sample
  of known corrections actually persisted (the way this was manually
  verified on 2026-10-05) rather than trusting a clean exit code alone.

## Suggested shape (not yet built)

A stage-based driver, not a monolithic script — each phase/step is a named,
independently callable unit (subprocess call or function) so individual
scripts can be swapped out as they change without rewriting the orchestration
glue. Each stage writes its output to a dated local folder (mirroring
`ai-curation-pipeline`'s own `output/<date>/` convention) so every run is
independently inspectable and a failed run can resume from the last
completed stage rather than starting over.
