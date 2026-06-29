# Processor Tests

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- `cosign` and `skopeo` on `$PATH` (for live tests)
- `RHOAI_QUAY_API_TOKEN` env var (for bundle processor live tests)
- Registry auth for `registry.redhat.io` via `skopeo login` (for live SBOM tests)

### Getting the Quay API token

```bash
vault-login  # if not already authenticated
set -gx RHOAI_QUAY_API_TOKEN (vault kv get -mount=rhoai -field=oauth_token creds/quay/quay-devops-app)
```

## Running tests

From `utils/processors/`:

```bash
# All tests (mocked + live)
uv run --extra test pytest test/ -v

# Mocked tests only (no tokens or network needed)
uv run --extra test pytest test/ -v -m 'not live'

# Live tests only
uv run --extra test pytest test/ -v -m live

# Specific test file
uv run --extra test pytest test/test_sbom.py -v
```

## Test files

| File | Scope | Markers |
|------|-------|---------|
| `test_sbom.py` | `utils/sbom.py` — SBOM download, package lookup, tag stripping | Mocked + `live` |
| `test_bundle_extract_sbom_metadata.py` | `bundle-processor.py` — `extract_sbom_metadata()` config parsing, lookup, error handling, conflict detection | Mocked only |
| `test_bundle_processor.py` | `bundle-processor.py` — full `process()` end-to-end with minimal fixtures and regression tests against real RHOAI-Build-Config commits | `live` |
| `test_catalog_validator.py` | `validator/catalog_validator.py` — catalog validation logic | Mocked only |

## Markers

- **`live`** — hits real registries (Quay.io, GitHub, registry.redhat.io). Requires network, tokens, and CLI tools.
- No marker — fully mocked, runs offline in ~0.2s.

## Regression tests

`TestBundleProcessorRegression` in `test_bundle_processor.py` replays real bundle processor runs against pinned RHOAI-Build-Config commits. Each test case is defined by:

- `input_sha` — the commit with pre-processor inputs
- `output_sha` — the commit with the expected processor output
- `rhoai_version` — the release branch

The test clones RHOAI-Build-Config once (blobless, cached in `/tmp/rhoai-test-rbc-cache`), creates worktrees for each commit pair, and compares the processor output against the known-good CSV. The operator tag resolution is pinned so tests are deterministic despite mutable Quay tags.

To add a new regression test case, find a "Updating the bundle-csv" commit on the target branch and add its input/output SHA pair to `RELEASE_TEST_CASES`.
