# Compatibility TODO Execution Playbook

This document defines the execution loop for closing Redis compatibility TODOs.
The goal is to keep implementation changes rigorous, test-first, and traceable.

## Loop Protocol (One TODO per loop)

0. Write unit tests first.
   - Add test cases before implementation.
   - Prefer a 1:1 mapping to original upstream tests.
   - Add comments with upstream references (file + test title).
1. Define target behavior.
   - Write down expected semantics for the TODO scope.
2. Define target implementation shape.
   - Choose data flow and responsibilities before touching code.
3. Read current implementation.
   - Identify divergence between current and target behavior.
4. Refactor for clarity (test safety net required).
   - If needed, replace complex logic with cleaner structure.
5. Read Garnet original (.NET) implementation.
   - Extract behavior and architecture intent.
6. Read Valkey implementation.
   - Confirm behavior details and edge-case semantics.
7. Integrate good essence.
   - Apply what improves correctness/maintainability.
   - If it creates local inconsistency, keep improvement and leave explicit TODO.
8. Execute tests.
   - Run focused unit tests, then wider compatibility checks.
   - Verify test counts and failed-case absence, not only exit code.
9. Model concurrency risk when needed.
   - If race/order concerns appear, write/update TLA+ model and check invariants.

## Outer Loop Plan

- Repeat the loop approximately 10 times.
- Default granularity:
  - `1 TODO = 1 loop = 1 commit`
- Required artifacts per loop:
  - Updated tests
  - Updated TODO status
  - Brief loop log with:
    - behavior decision
    - implementation decision
    - upstream references consulted
    - validation commands/results

## Loop Log

Track each loop in:

- `docs/compatibility/compatibility-todo-loop-log-2026-02.md`
