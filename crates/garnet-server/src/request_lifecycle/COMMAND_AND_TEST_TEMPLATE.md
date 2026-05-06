# Command And Test Template

Use this as a copy template when adding a new Redis command.

## 1. Handler template (`*_commands.rs`)

```rust
pub(super) fn handle_mycommand(
    &self,
    args: &[ArgSlice],
    response_out: &mut Vec<u8>,
) -> Result<(), RequestExecutionError> {
    require_exact_arity(args, 3, "MYCOMMAND", "MYCOMMAND key value")?;

    let key = arg_slice_bytes(&args[1]);
    let value = arg_slice_bytes(&args[2]);

    // Command logic...
    append_simple_string(response_out, b"OK");
    Ok(())
}
```

Available common helpers (from `command_helpers.rs`):

- `require_exact_arity`
- `ensure_min_arity`
- `ensure_ranged_arity`
- `ensure_paired_arity_after`
- `parse_scan_match_count_options`

## 2. Unit test template (`request_lifecycle/tests.rs`)

```rust
#[test]
fn mycommand_roundtrip() {
    let processor = RequestProcessor::new().unwrap();
    assert_command_response(&processor, "MYCOMMAND key value", b"+OK\r\n");
}
```

For integer replies:

```rust
assert_command_integer(&processor, "DEL key", 1);
```

## 3. Wiring checklist

1. Add `CommandId` and spec in `command_spec.rs`.
2. Add dispatch mapping in `command_dispatch.rs`.
3. Add execute routing in `request_lifecycle.rs`.
4. Add lifecycle tests in `request_lifecycle/tests.rs`.
5. Run required checks in `request_lifecycle/AGENT.md`.
