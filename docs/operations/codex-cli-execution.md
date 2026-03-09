# Codex CLI Execution Runbook (MCP Timeout Fallback)

## Policy

- For this repository, use local `codex exec` for long-running work.
- Do not rely on Codex MCP execution for heavy runs (tests, compatibility probes, benchmarks, large refactors).
- If an MCP path times out once, switch to `codex exec` immediately.

## Quick Start

```bash
codex exec -C /Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet -s danger-full-access "<your prompt>"
```

## Recommended Execution Pattern (Logs + Reproducibility)

1. Save the prompt in a file.
2. Create a timestamped output directory.
3. Keep full stdout/stderr logs.

```bash
REPO=/Users/kazuki-matsuda/dev/src/github.com/microsoft/garnet
TS=$(date +%Y%m%d-%H%M%S)
OUT="$REPO/.codex-runs/$TS"
mkdir -p "$OUT"

cat > "$OUT/prompt.md" <<'EOF'
<your prompt>
EOF

codex exec \
  -C "$REPO" \
  -s danger-full-access \
  "$(cat "$OUT/prompt.md")" \
  | tee "$OUT/codex-exec.log"
```

## Long-Run Control

- Prefer explicit wall-time guards for expensive scripts you ask Codex to run.
- For existing compatibility workflows, set timeout environment variables in the prompt, e.g.:
  - `RUNTEXT_TIMEOUT_SECONDS=<per-test-seconds>`
  - `RUNTEXT_WALL_TIMEOUT_SECONDS=<total-seconds>`
- If your shell has `timeout`, wrap `codex exec`:

```bash
timeout 2h codex exec -C "$REPO" -s danger-full-access "$(cat "$OUT/prompt.md")" | tee "$OUT/codex-exec.log"
```

## Resume / Continue

- Continue the most recent session:

```bash
codex exec resume --last "<follow-up prompt>"
```

- Continue a specific session:

```bash
codex exec resume <SESSION_ID> "<follow-up prompt>"
```

## Health Checks

- Check active Codex processes:

```bash
ps -ef | rg "codex exec"
```

- Follow current log:

```bash
tail -f "$OUT/codex-exec.log"
```

## Practical Rules

- Keep one run per output directory.
- Keep prompt and logs together for reproducibility.
- For benchmark or compatibility work, always retain command parameters in the prompt/log pair.
