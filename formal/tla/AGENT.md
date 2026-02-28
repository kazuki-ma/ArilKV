# AGENT.md

Before editing specs in this directory, read `formal/tla/README.md`.

## Required workflow

1. Keep models bounded and small by default.
2. Re-run TLC after each spec/config update:
   - `./tools/tla/run_tlc.sh formal/tla/specs/<Spec>.tla formal/tla/specs/<Spec>.cfg`
3. If a counterexample appears, record reproduction notes and map it to a deterministic Rust test case.
4. When a model is mapped back into Rust:
   - Add a file-level comment indicating the related TLA+ model/spec.
   - Add per-critical-section comments in source with exact action labels:
     - `// TLA+ : <ActionName>`
