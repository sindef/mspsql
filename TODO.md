# TODO

## E2E reliability

The latest failure was on a docs-only change. Unit, lint, manifest, race, image,
and Pages gates passed; E2E failed in four-cluster conformance after
`MultiSitePostgres/orders` was applied:

```text
Error from server (Timeout): the server was unable to return a response in the time allotted
client rate limiter Wait returned an error: context deadline exceeded - error from a previous attempt: EOF
```

This points to KIND/API-server pressure or an unbounded wait path, not a docs
regression.

Recommended changes:

1. Add `timeout-minutes` to `.github/workflows/test-e2e.yml`.
   The job should fail fast enough to preserve logs and runner capacity.

2. Split conformance into focused jobs:
   registration/connectivity, initial provision, tenant CRs, restore, minor
   upgrade, major upgrade, deletion/finalizers.

3. Add phase markers in `test/kind/run.sh`.
   Print a single `E2E_PHASE=<name>` line before each major step so failures are
   searchable in GitHub logs.

4. Replace raw `kubectl wait/get` loops with a shared retry helper.
   It should retry transient `Timeout`, `EOF`, and rate-limiter errors, and fail
   with the last command, namespace, resource, and phase.

5. Make diagnostics bounded.
   `kind export logs` and every diagnostic `kubectl` call already has a timeout;
   add an overall diagnostics budget so cleanup cannot run for hours.

6. Reduce runner load.
   Consider serializing expensive cluster setup, limiting controller replicas in
   E2E, or splitting the four KIND clusters across jobs where test boundaries
   allow it.

7. Do not skip E2E for docs-only changes until the suite is stable.
   Docs often include install examples and release image tags; they must still
   prove against a real manifest flow.
