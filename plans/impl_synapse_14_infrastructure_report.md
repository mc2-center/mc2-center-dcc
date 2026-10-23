# Implementation report: Synapse and infrastructure changes for data model 14.0.0

Running log for `plans/impl_synapse_14_infrastructure.md`.

## I0. Holds and communication
Done by the owner before 2026-10-23.

## I2. Register the 14.0.0 schemas
The owner registered 14.0.0 for every `MC2Center` schema on 2026-10-23 (about 22:30 UTC). Not yet compared against `json_schemas/` at the `v14.0.0` tag.

## I1. Registry cleanup (2026-10-23)
**Owner instructions, 2026-10-23:**
- Keep `SequencingRNALevel1` 15.0.1 until I7. Two data folders in CA293853 are bound to it: `syn75299050` (`CA293853_aml_mouse_mRNA_Level_1`, 539 files) and `syn75299771` (`CA293853_aml_mouse_miRNA_Level_1`, 142 files).
- Don't delete any 13.x version. That keeps `StudyAR` 13.0.0, 13.1.0, 13.2.0 and 13.3.0, which narrows O9 ("all of `StudyAR`").

**Binding check before deleting:** checked 2,938 entities: 155 grant projects, their 2,783 direct children, and the 538 tables in syn52750482. The only hit on the delete list was the `SequencingRNALevel1` 15.0.1 binding above. Bindings deeper in the tree weren't covered, and Synapse's own check later found some (see the refusals below).

**Result: 30 of 36 versions deleted.**

| Schema | Deleted | Refused by Synapse |
|---|---|---|
| `PublicationView` | — | 15.0.0 (bound to an object) |
| `Biospecimen` | 15.0.0, 15.0.1 | — |
| `Model` | 15.0.0, 15.0.1 | — |
| `SequencingRNALevel1` | — | 15.0.0 (bound to an object) |
| `AccessRequirementCA123` | — | 17.0.0 (bound to an object); 18.0.0 (referenced by another schema) |
| `StudyAR` | 1.0.0–8.0.0, 10.0.0–12.0.0, 14.0.0, 16.0.0–24.0.0, 26.0.0–30.0.0 | 9.0.0, 15.0.0 (bound to an object) |

**Left in the registry after I1:**
- `PublicationView` 15.0.0
- `SequencingRNALevel1` 15.0.0 and 15.0.1
- `AccessRequirementCA123` 17.0.0 and 18.0.0
- `StudyAR` 9.0.0 and 15.0.0, plus the kept 13.x versions

None of these are 14.0.0 or 13.x versions targeted by I1. The refused versions go to I7: find the bound entities, rebind them, then delete the versions.

Scripts (scratchpad, not committed): `i1_dryrun.py` (registry listing and binding check), `i1_delete.py` (guarded delete). Guards: no 13.x, nothing registered on 2026-10-23, no `AccessRequirementCA261841`, not `SequencingRNALevel1` 15.0.1.
