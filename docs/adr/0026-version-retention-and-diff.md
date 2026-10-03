# 0026. Per-path retention and chunk-level version diff

Status: accepted
Date: 2026-10-03

## Context

A new version retires the one before it. Retired versions stay for `RetainEpochs` GC epochs (3 by default, 90 s at the demo setting) so undelete and `chunkd get -version` work, then `hardDelete` drops them and their chunk references (ADR-0016). One cluster-wide number is too coarse: a path that holds a document under edit wants days, a scratch path wants none. `chunkd log` shows what is retained, but not how two versions differ.

## Options considered

| Question | Options | Chosen |
|---|---|---|
| Where retention lives | A config file on the metadata server; a rule per prefix; **a logged per-path setting** | A logged op on the path. Every peer applies it in log order, so GC at every peer drops the same versions in the same epoch. It is in the snapshot with the file |
| Unit | Wall-clock duration; **GC epochs** | Epochs. Time never decides correctness here (the epoch is logical); the operator multiplies by `EpochEvery` to get a duration. `chunkd log` already reports each version's expiry epoch |
| Default and reset | No reset; **0 means the cluster default** | 0 restores the default, so an override can be removed |
| Path must exist | Allow setting before the first write; **refuse unknown paths** | Refuse (`not_found`). A setting on a path with no history would outlive nothing and could be set by any key on any name |
| Diff granularity | A byte diff of the files; **chunks by index and content ID** | Chunks. Two manifests answer it without reading data, and it shows exactly what a rewrite would send given dedup. A fixed-size chunker finds in-place edits; an insertion shifts every later chunk and shows as all changed (content-defined chunking, as in restic or borg, would find it) |

## Decision

- `SetRetentionOp{path, retain_epochs}`; `File.Retain`; `State.RetainFor(path, default)` is used by `hardDelete`, by the expiry the log shows for each version, and by the "deleted, restorable" list.
- Validate refuses a path with no versions. Apply sets the value, so a retry is a no-op.
- Client `SetRetention`; gateway `PUT /retention/{path}?epochs=N` (scoped to the key's namespace like every path route); `chunkd retain <path> <epochs>`.
- `chunkd diff <path> <from> <to>` compares the two manifests (`StatVersion`) chunk by chunk and reports same, changed, added and removed chunks and the bytes a write of `to` over `from` would send.
- `chunkd restore <path> <version>` is `undelete` with the version required.
- CLI polish: exit codes by error class (3 not found, 4 conflict, 5 quota, 6 denied, 7 unavailable, 8 corrupt, 2 usage), a byte counter on `get`, `chunkd bench`, `chunkd completion bash|zsh|powershell`, `chunkd keygen`. `--json` already existed on every reading command.

## Consequences

- Retention can only be shortened below the cluster default for new retirements to matter: a version already past its new, shorter limit is dropped at the next epoch, which is what the setting says.
- A longer retention keeps the chunks of retired versions pinned, so it costs bytes the namespace quota does not count (ADR-0025).
- SIMPLIFIED: S3 and GCS express this as lifecycle rules by prefix and age, evaluated by a background service; here it is one number per exact path, with no prefix rules and no maximum count of versions.
- SIMPLIFIED: the dashboard shows each path's versions with their expiry epoch and restores one, as before; it does not show a diff.
- Tests: `TestRetentionPerPath`, `TestRetentionSurvivesSnapshot` (state), `TestRetentionIsPerNamespace` (gateway), `TestDiffManifests` and `TestDiffAgainstACluster` (CLI).

## At 100x scale

Rules by prefix, age and version count, evaluated per epoch in slices so one tick does not walk every file. Diff would use content-defined chunk boundaries so a shifted file shares most chunks, and the dashboard would draw the changed chunks on the chunk grid.
