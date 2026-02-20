# Experiment Reporting Policy

## Goal

Ensure every performance experiment is reproducible, auditable, and reversible.

## Mandatory Rules

1. Use one directory per experiment under:
   - `docs/performance/experiments/<YYYY-MM-DD>/<experiment-slug>/`
2. Each experiment directory must include:
   - `README.md`
   - lightweight artifacts when possible (`comparison.txt`, `summary.txt`, `runs.csv`, log excerpts)
   - `diff.patch` (or an explicit reason why it is unavailable)
3. Record commit lineage in `README.md`:
   - `before_commit`
   - `after_commit`
   - `revert_commit` when reverted
4. Record exact execution parameters:
   - command line
   - env vars (`THREADS`, `CONNS`, `REQUESTS`, `PIPELINE`, shard/owner settings, etc.)
5. Record both throughput and latency metrics when available:
   - `median_set_ops`, `median_get_ops`
   - `median_set_p99_ms`, `median_get_p99_ms`
6. Keep benchmark validation strict:
   - reject runs with `Connection error:`
   - reject runs with `handle error response:`

## Commit Workflow (Required)

1. Create an experiment commit for code under test.
2. Run A/B and store artifacts.
3. If experiment is rejected, create a revert commit.
4. Record all hashes (`before/after/revert`) in experiment `README.md`.

## Current Gap Notes

Some older experiments were executed as working-tree-only changes before this policy.
Those reports must explicitly call out missing hashes and why they are missing.
New experiments must not skip the commit workflow above.
