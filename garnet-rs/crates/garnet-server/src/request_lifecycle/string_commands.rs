use super::*;
use tsavorite::{RmwInfo, RmwOperationError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BitopOperation {
    And,
    Or,
    Xor,
    Not,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BitfieldSignedness {
    Signed,
    Unsigned,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BitfieldEncoding {
    signedness: BitfieldSignedness,
    bits: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BitfieldOverflowMode {
    Wrap,
    Sat,
    Fail,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BitfieldIncrOutcome {
    Value { raw: u64, value: i64 },
    OverflowFail,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LcsResponseMode {
    Sequence,
    LengthOnly,
    Indexes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LcsOptions {
    mode: LcsResponseMode,
    min_match_len: usize,
    with_match_len: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LcsMatchSegment {
    start_left: usize,
    end_left: usize,
    start_right: usize,
    end_right: usize,
    length: usize,
}

const PF_STRING_PREFIX: &[u8] = b"\x00garnet-pf-v1\x00";
const PFDEBUG_HELP_LINES: [&[u8]; 8] = [
    b"PFDEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"ENCODING <key>",
    b"    Return HyperLogLog internal encoding for key.",
    b"TODENSE <key>",
    b"    Force sparse-to-dense representation (no-op in garnet-rs).",
    b"HELP",
    b"    Print this help.",
    b"DECODE <key>",
];

impl RequestProcessor {
    pub(super) fn handle_get(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        crate::debug_sync_point!("request_processor.handle_get.enter");
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "GET",
                expected: "GET key",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let shard_index = self.string_store_shard_index_for_key(&key);
        self.expire_key_if_needed_in_shard(&key, shard_index)?;
        crate::debug_sync_point!("request_processor.handle_get.before_store_lock");

        let mut store = self.lock_string_store_for_shard(shard_index);
        crate::debug_sync_point!("request_processor.handle_get.after_store_lock");
        let mut session = store.session(&self.functions);
        let mut output = Vec::new();
        let status = session
            .read(&key, &Vec::new(), &mut output, &ReadInfo::default())
            .map_err(map_read_error)?;

        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => {
                append_bulk_string(response_out, &output);
                Ok(())
            }
            ReadOperationStatus::NotFound => {
                if self.object_key_exists(&key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                append_null_bulk_string(response_out);
                Ok(())
            }
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    pub(super) fn handle_strlen(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "STRLEN",
                expected: "STRLEN key",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let shard_index = self.string_store_shard_index_for_key(&key);
        self.expire_key_if_needed_in_shard(&key, shard_index)?;

        let mut store = self.lock_string_store_for_shard(shard_index);
        let mut session = store.session(&self.functions);
        let mut output = Vec::new();
        let status = session
            .read(&key, &Vec::new(), &mut output, &ReadInfo::default())
            .map_err(map_read_error)?;

        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => {
                append_integer(response_out, output.len() as i64);
                Ok(())
            }
            ReadOperationStatus::NotFound => {
                if self.object_key_exists(&key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                append_integer(response_out, 0);
                Ok(())
            }
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    pub(super) fn handle_getrange(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_getrange_like(args, response_out, false)
    }

    pub(super) fn handle_substr(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_getrange_like(args, response_out, true)
    }

    fn handle_getrange_like(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
        substr_alias: bool,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if substr_alias {
            ("SUBSTR", "SUBSTR key start end")
        } else {
            ("GETRANGE", "GETRANGE key start end")
        };
        if args.len() != 4 {
            return Err(RequestExecutionError::WrongArity { command, expected });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let start = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let end = parse_i64_ascii(unsafe { args[3].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let shard_index = self.string_store_shard_index_for_key(&key);
        self.expire_key_if_needed_in_shard(&key, shard_index)?;

        let mut store = self.lock_string_store_for_shard(shard_index);
        let mut session = store.session(&self.functions);
        let mut output = Vec::new();
        let status = session
            .read(&key, &Vec::new(), &mut output, &ReadInfo::default())
            .map_err(map_read_error)?;

        match status {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => {
                if let Some((start_index, end_index)) =
                    normalize_string_range(output.len(), start, end)
                {
                    append_bulk_string(response_out, &output[start_index..=end_index]);
                } else {
                    append_bulk_string(response_out, b"");
                }
                Ok(())
            }
            ReadOperationStatus::NotFound => {
                if self.object_key_exists(&key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                append_bulk_string(response_out, b"");
                Ok(())
            }
            ReadOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
        }
    }

    pub(super) fn handle_getbit(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "GETBIT",
                expected: "GETBIT key offset",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let offset = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        if offset < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        let offset = usize::try_from(offset).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;
        let Some(value) = self.read_string_value(&key)? else {
            if self.object_key_exists(&key)? {
                return Err(RequestExecutionError::WrongType);
            }
            append_integer(response_out, 0);
            return Ok(());
        };

        let byte_index = offset / 8;
        let old_bit = if byte_index < value.len() {
            let bit_index = 7usize - (offset % 8);
            i64::from((value[byte_index] >> bit_index) & 1)
        } else {
            0
        };
        append_integer(response_out, old_bit);
        Ok(())
    }

    pub(super) fn handle_setbit(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 4 {
            return Err(RequestExecutionError::WrongArity {
                command: "SETBIT",
                expected: "SETBIT key offset value",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let offset = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        if offset < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        let offset = usize::try_from(offset).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let bit_value = parse_i64_ascii(unsafe { args[3].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        let bit_value = match bit_value {
            0 => 0u8,
            1 => 1u8,
            _ => return Err(RequestExecutionError::ValueOutOfRange),
        };
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;
        let expiration_unix_millis = self.expiration_unix_millis_for_key(&key);
        let mut value = match self.read_string_value(&key)? {
            Some(value) => value,
            None => {
                if self.object_key_exists(&key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                Vec::new()
            }
        };

        let byte_index = offset / 8;
        let bit_index = 7usize - (offset % 8);
        let old_bit = if byte_index < value.len() {
            i64::from((value[byte_index] >> bit_index) & 1)
        } else {
            0
        };
        let required_len = byte_index
            .checked_add(1)
            .ok_or(RequestExecutionError::ValueOutOfRange)?;
        if value.len() < required_len {
            value.resize(required_len, 0);
        }

        if bit_value == 1 {
            value[byte_index] |= 1u8 << bit_index;
        } else {
            value[byte_index] &= !(1u8 << bit_index);
        }

        self.upsert_string_value_with_expiration_unix_millis(&key, &value, expiration_unix_millis)?;
        self.track_string_key(&key);
        self.bump_watch_version(&key);
        append_integer(response_out, old_bit);
        Ok(())
    }

    pub(super) fn handle_setrange(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 4 {
            return Err(RequestExecutionError::WrongArity {
                command: "SETRANGE",
                expected: "SETRANGE key offset value",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let offset = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        if offset < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        let offset = usize::try_from(offset).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let new_segment = unsafe { args[3].as_slice() };
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;

        let expiration_unix_millis = self.expiration_unix_millis_for_key(&key);
        let mut value = match self.read_string_value(&key)? {
            Some(value) => value,
            None => {
                if self.object_key_exists(&key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                Vec::new()
            }
        };
        if new_segment.is_empty() {
            append_integer(response_out, value.len() as i64);
            return Ok(());
        }

        let new_len = offset
            .checked_add(new_segment.len())
            .ok_or(RequestExecutionError::ValueOutOfRange)?;
        if value.len() < new_len {
            value.resize(new_len, 0);
        }
        value[offset..offset + new_segment.len()].copy_from_slice(new_segment);

        self.upsert_string_value_with_expiration_unix_millis(&key, &value, expiration_unix_millis)?;
        self.track_string_key(&key);
        self.bump_watch_version(&key);
        append_integer(response_out, value.len() as i64);
        Ok(())
    }

    pub(super) fn handle_bitcount(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 && args.len() != 4 && args.len() != 5 {
            return Err(RequestExecutionError::WrongArity {
                command: "BITCOUNT",
                expected: "BITCOUNT key [start end [BYTE|BIT]]",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;
        let Some(value) = self.read_string_value(&key)? else {
            if self.object_key_exists(&key)? {
                return Err(RequestExecutionError::WrongType);
            }
            append_integer(response_out, 0);
            return Ok(());
        };
        if value.is_empty() {
            append_integer(response_out, 0);
            return Ok(());
        }
        if args.len() == 2 {
            let bits = value.iter().map(|byte| i64::from(byte.count_ones())).sum();
            append_integer(response_out, bits);
            return Ok(());
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let start = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let end = parse_i64_ascii(unsafe { args[3].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        let bit_mode = if args.len() == 5 {
            // SAFETY: caller guarantees argument backing memory validity.
            let mode = unsafe { args[4].as_slice() };
            if ascii_eq_ignore_case(mode, b"BYTE") {
                false
            } else if ascii_eq_ignore_case(mode, b"BIT") {
                true
            } else {
                return Err(RequestExecutionError::SyntaxError);
            }
        } else {
            false
        };

        let count = if bit_mode {
            let total_bits = value
                .len()
                .checked_mul(8)
                .ok_or(RequestExecutionError::ValueOutOfRange)?;
            if let Some((start_bit, end_bit)) = normalize_string_range(total_bits, start, end) {
                let mut count = 0i64;
                for bit_index in start_bit..=end_bit {
                    let byte = value[bit_index / 8];
                    let shift = 7usize - (bit_index % 8);
                    count += i64::from((byte >> shift) & 1);
                }
                count
            } else {
                0
            }
        } else if let Some((start_byte, end_byte)) = normalize_string_range(value.len(), start, end)
        {
            value[start_byte..=end_byte]
                .iter()
                .map(|byte| i64::from(byte.count_ones()))
                .sum()
        } else {
            0
        };
        append_integer(response_out, count);
        Ok(())
    }

    pub(super) fn handle_bitpos(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 || args.len() > 6 {
            return Err(RequestExecutionError::WrongArity {
                command: "BITPOS",
                expected: "BITPOS key bit [start [end [BYTE|BIT]]]",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let bit = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        if bit != 0 && bit != 1 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        let target_bit = bit as u8;

        self.expire_key_if_needed(&key)?;
        let Some(value) = self.read_string_value(&key)? else {
            if self.object_key_exists(&key)? {
                return Err(RequestExecutionError::WrongType);
            }
            let missing_result = if target_bit == 0 { 0 } else { -1 };
            append_integer(response_out, missing_result);
            return Ok(());
        };
        if value.is_empty() {
            append_integer(response_out, -1);
            return Ok(());
        }

        let mut mode_is_bit = false;
        if args.len() == 6 {
            // SAFETY: caller guarantees argument backing memory validity.
            let mode = unsafe { args[5].as_slice() };
            if ascii_eq_ignore_case(mode, b"BYTE") {
                mode_is_bit = false;
            } else if ascii_eq_ignore_case(mode, b"BIT") {
                mode_is_bit = true;
            } else {
                return Err(RequestExecutionError::SyntaxError);
            }
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let start = if args.len() >= 4 {
            parse_i64_ascii(unsafe { args[3].as_slice() })
                .ok_or(RequestExecutionError::ValueNotInteger)?
        } else {
            0
        };
        // SAFETY: caller guarantees argument backing memory validity.
        let end = if args.len() >= 5 {
            parse_i64_ascii(unsafe { args[4].as_slice() })
                .ok_or(RequestExecutionError::ValueNotInteger)?
        } else if mode_is_bit {
            let total_bits = value
                .len()
                .checked_mul(8)
                .ok_or(RequestExecutionError::ValueOutOfRange)?;
            (total_bits as i64) - 1
        } else {
            (value.len() as i64) - 1
        };
        let has_explicit_end = args.len() >= 5;

        let result = if mode_is_bit {
            let total_bits = value
                .len()
                .checked_mul(8)
                .ok_or(RequestExecutionError::ValueOutOfRange)?;
            if let Some((start_bit, end_bit)) = normalize_string_range(total_bits, start, end) {
                let mut found = None;
                for bit_index in start_bit..=end_bit {
                    let byte = value[bit_index / 8];
                    let shift = 7usize - (bit_index % 8);
                    if ((byte >> shift) & 1) == target_bit {
                        found = Some(bit_index as i64);
                        break;
                    }
                }
                found.unwrap_or(-1)
            } else {
                -1
            }
        } else if let Some((start_byte, end_byte)) = normalize_string_range(value.len(), start, end)
        {
            let mut found = None;
            for byte_index in start_byte..=end_byte {
                let byte = value[byte_index];
                if target_bit == 1 {
                    if byte == 0 {
                        continue;
                    }
                    let first_set = byte.leading_zeros() as usize;
                    found = Some(((byte_index * 8) + first_set) as i64);
                    break;
                }
                if byte == 0xFF {
                    continue;
                }
                let first_zero = (!byte).leading_zeros() as usize;
                found = Some(((byte_index * 8) + first_zero) as i64);
                break;
            }
            if let Some(position) = found {
                position
            } else if target_bit == 0 && !has_explicit_end {
                (value.len() * 8) as i64
            } else {
                -1
            }
        } else {
            -1
        };

        append_integer(response_out, result);
        Ok(())
    }

    pub(super) fn handle_bitop(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 4 {
            return Err(RequestExecutionError::WrongArity {
                command: "BITOP",
                expected: "BITOP operation destkey key [key ...]",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let operation = parse_bitop_operation(unsafe { args[1].as_slice() })
            .ok_or(RequestExecutionError::SyntaxError)?;
        if operation == BitopOperation::Not && args.len() != 4 {
            return Err(RequestExecutionError::WrongArity {
                command: "BITOP",
                expected: "BITOP NOT destkey key",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let destination = unsafe { args[2].as_slice() }.to_vec();
        let source_keys = args[3..]
            .iter()
            .map(|key| {
                // SAFETY: caller guarantees argument backing memory validity.
                unsafe { key.as_slice() }.to_vec()
            })
            .collect::<Vec<_>>();

        let mut source_values = Vec::with_capacity(source_keys.len());
        for key in &source_keys {
            self.expire_key_if_needed(key)?;
            let value = match self.read_string_value(key)? {
                Some(value) => value,
                None => {
                    if self.object_key_exists(key)? {
                        return Err(RequestExecutionError::WrongType);
                    }
                    Vec::new()
                }
            };
            source_values.push(value);
        }

        let result = apply_bitop(operation, &source_values);
        if result.is_empty() {
            self.delete_string_key_for_migration(&destination)?;
            let _ = self.object_delete(&destination)?;
        } else {
            let _ = self.object_delete(&destination)?;
            self.upsert_string_value_for_migration(&destination, &result, None)?;
        }
        append_integer(response_out, result.len() as i64);
        Ok(())
    }

    pub(super) fn handle_bitfield(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_bitfield_impl(args, response_out, false)
    }

    pub(super) fn handle_bitfield_ro(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_bitfield_impl(args, response_out, true)
    }

    fn handle_bitfield_impl(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
        read_only: bool,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 2 {
            return Err(RequestExecutionError::WrongArity {
                command: if read_only { "BITFIELD_RO" } else { "BITFIELD" },
                expected: if read_only {
                    "BITFIELD_RO key GET encoding offset [GET encoding offset ...]"
                } else {
                    "BITFIELD key [GET|SET|INCRBY|OVERFLOW ...]"
                },
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;
        let expiration_unix_millis = self.expiration_unix_millis_for_key(&key);
        let mut value = match self.read_string_value(&key)? {
            Some(value) => value,
            None => {
                if self.object_key_exists(&key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                Vec::new()
            }
        };

        let mut overflow_mode = BitfieldOverflowMode::Wrap;
        let mut index = 2usize;
        let mut operation_count = 0usize;
        let mut wrote = false;
        let mut responses: Vec<Option<i64>> = Vec::new();

        while index < args.len() {
            // SAFETY: caller guarantees argument backing memory validity.
            let subcommand = unsafe { args[index].as_slice() };

            if ascii_eq_ignore_case(subcommand, b"OVERFLOW") {
                if read_only {
                    return Err(RequestExecutionError::SyntaxError);
                }
                let mode_index = index
                    .checked_add(1)
                    .ok_or(RequestExecutionError::ValueOutOfRange)?;
                if mode_index >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                // SAFETY: caller guarantees argument backing memory validity.
                let mode = unsafe { args[mode_index].as_slice() };
                overflow_mode =
                    parse_bitfield_overflow_mode(mode).ok_or(RequestExecutionError::SyntaxError)?;
                index = mode_index + 1;
                continue;
            }

            if ascii_eq_ignore_case(subcommand, b"GET") {
                let encoding_index = index
                    .checked_add(1)
                    .ok_or(RequestExecutionError::ValueOutOfRange)?;
                let offset_index = index
                    .checked_add(2)
                    .ok_or(RequestExecutionError::ValueOutOfRange)?;
                if offset_index >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }

                // SAFETY: caller guarantees argument backing memory validity.
                let encoding_token = unsafe { args[encoding_index].as_slice() };
                // SAFETY: caller guarantees argument backing memory validity.
                let offset_token = unsafe { args[offset_index].as_slice() };
                let encoding = parse_bitfield_encoding(encoding_token)?;
                let offset = parse_bitfield_offset(offset_token, usize::from(encoding.bits))?;
                let raw = read_unsigned_bits(&value, offset, usize::from(encoding.bits))?;
                responses.push(Some(decode_bitfield_raw(raw, encoding)));
                operation_count += 1;
                index = offset_index + 1;
                continue;
            }

            if ascii_eq_ignore_case(subcommand, b"SET") {
                if read_only {
                    return Err(RequestExecutionError::SyntaxError);
                }
                let encoding_index = index
                    .checked_add(1)
                    .ok_or(RequestExecutionError::ValueOutOfRange)?;
                let offset_index = index
                    .checked_add(2)
                    .ok_or(RequestExecutionError::ValueOutOfRange)?;
                let value_index = index
                    .checked_add(3)
                    .ok_or(RequestExecutionError::ValueOutOfRange)?;
                if value_index >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }

                // SAFETY: caller guarantees argument backing memory validity.
                let encoding_token = unsafe { args[encoding_index].as_slice() };
                // SAFETY: caller guarantees argument backing memory validity.
                let offset_token = unsafe { args[offset_index].as_slice() };
                // SAFETY: caller guarantees argument backing memory validity.
                let value_token = unsafe { args[value_index].as_slice() };
                let encoding = parse_bitfield_encoding(encoding_token)?;
                let offset = parse_bitfield_offset(offset_token, usize::from(encoding.bits))?;
                let set_value =
                    parse_i64_ascii(value_token).ok_or(RequestExecutionError::ValueNotInteger)?;
                let raw = read_unsigned_bits(&value, offset, usize::from(encoding.bits))?;
                responses.push(Some(decode_bitfield_raw(raw, encoding)));
                let new_raw = encode_bitfield_value(set_value, encoding);
                write_unsigned_bits(&mut value, offset, usize::from(encoding.bits), new_raw)?;
                wrote = true;
                operation_count += 1;
                index = value_index + 1;
                continue;
            }

            if ascii_eq_ignore_case(subcommand, b"INCRBY") {
                if read_only {
                    return Err(RequestExecutionError::SyntaxError);
                }
                let encoding_index = index
                    .checked_add(1)
                    .ok_or(RequestExecutionError::ValueOutOfRange)?;
                let offset_index = index
                    .checked_add(2)
                    .ok_or(RequestExecutionError::ValueOutOfRange)?;
                let increment_index = index
                    .checked_add(3)
                    .ok_or(RequestExecutionError::ValueOutOfRange)?;
                if increment_index >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }

                // SAFETY: caller guarantees argument backing memory validity.
                let encoding_token = unsafe { args[encoding_index].as_slice() };
                // SAFETY: caller guarantees argument backing memory validity.
                let offset_token = unsafe { args[offset_index].as_slice() };
                // SAFETY: caller guarantees argument backing memory validity.
                let increment_token = unsafe { args[increment_index].as_slice() };
                let encoding = parse_bitfield_encoding(encoding_token)?;
                let offset = parse_bitfield_offset(offset_token, usize::from(encoding.bits))?;
                let increment = parse_i64_ascii(increment_token)
                    .ok_or(RequestExecutionError::ValueNotInteger)?;
                let raw = read_unsigned_bits(&value, offset, usize::from(encoding.bits))?;

                match apply_bitfield_incrby(raw, encoding, increment, overflow_mode)? {
                    BitfieldIncrOutcome::Value { raw, value: result } => {
                        write_unsigned_bits(&mut value, offset, usize::from(encoding.bits), raw)?;
                        responses.push(Some(result));
                        wrote = true;
                    }
                    BitfieldIncrOutcome::OverflowFail => responses.push(None),
                }

                operation_count += 1;
                index = increment_index + 1;
                continue;
            }

            return Err(RequestExecutionError::SyntaxError);
        }

        if operation_count == 0 {
            return Err(RequestExecutionError::SyntaxError);
        }

        if wrote {
            self.upsert_string_value_with_expiration_unix_millis(
                &key,
                &value,
                expiration_unix_millis,
            )?;
            self.track_string_key(&key);
            self.bump_watch_version(&key);
        }

        response_out.extend_from_slice(format!("*{}\r\n", responses.len()).as_bytes());
        for entry in responses {
            match entry {
                Some(value) => append_integer(response_out, value),
                None => append_null_bulk_string(response_out),
            }
        }
        Ok(())
    }

    pub(super) fn handle_lcs(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "LCS",
                expected: "LCS key1 key2 [LEN | IDX [MINMATCHLEN min-match-len] [WITHMATCHLEN]]",
            });
        }

        let options = parse_lcs_options(args)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let left_key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let right_key = unsafe { args[2].as_slice() }.to_vec();

        self.expire_key_if_needed(&left_key)?;
        self.expire_key_if_needed(&right_key)?;
        let left = match self.read_string_value(&left_key)? {
            Some(value) => value,
            None => {
                if self.object_key_exists(&left_key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                Vec::new()
            }
        };
        let right = match self.read_string_value(&right_key)? {
            Some(value) => value,
            None => {
                if self.object_key_exists(&right_key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                Vec::new()
            }
        };

        let (lcs_len, sequence, mut matches) = lcs_sequence_and_matches(&left, &right);
        if options.mode == LcsResponseMode::LengthOnly {
            append_integer(response_out, lcs_len as i64);
            return Ok(());
        }
        if options.mode == LcsResponseMode::Sequence {
            append_bulk_string(response_out, &sequence);
            return Ok(());
        }

        if options.min_match_len > 0 {
            matches.retain(|entry| entry.length >= options.min_match_len);
        }
        append_lcs_idx_response(response_out, &matches, lcs_len, options.with_match_len);
        Ok(())
    }

    fn upsert_string_value_with_expiration_unix_millis(
        &self,
        key: &[u8],
        value: &[u8],
        expiration_unix_millis: Option<u64>,
    ) -> Result<(), RequestExecutionError> {
        let mut upsert_info = UpsertInfo::default();
        if expiration_unix_millis.is_some() {
            upsert_info.user_data |= UPSERT_USER_DATA_HAS_EXPIRATION;
        }
        let stored_value = encode_stored_value(value, expiration_unix_millis);
        let mut output = Vec::new();
        let mut store = self.lock_string_store_for_key(key);
        let mut session = store.session(&self.functions);
        session
            .upsert(&key.to_vec(), &stored_value, &mut output, &mut upsert_info)
            .map_err(map_upsert_error)?;
        Ok(())
    }

    pub(super) fn handle_append(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "APPEND",
                expected: "APPEND key value",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let append_value = unsafe { args[2].as_slice() };
        let shard_index = self.string_store_shard_index_for_key(&key);
        self.expire_key_if_needed_in_shard(&key, shard_index)?;

        let mut store = self.lock_string_store_for_shard(shard_index);
        let mut session = store.session(&self.functions);
        let mut object_store = self.lock_object_store_for_shard(shard_index);
        let mut object_session = object_store.session(&self.object_functions);

        let mut current_value = Vec::new();
        let string_exists = match session
            .read(&key, &Vec::new(), &mut current_value, &ReadInfo::default())
            .map_err(map_read_error)?
        {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => true,
            ReadOperationStatus::NotFound => false,
            ReadOperationStatus::RetryLater => return Err(RequestExecutionError::StorageBusy),
        };

        let mut object_output = Vec::new();
        let object_exists = match object_session
            .read(&key, &Vec::new(), &mut object_output, &ReadInfo::default())
            .map_err(map_read_error)?
        {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => true,
            ReadOperationStatus::NotFound => false,
            ReadOperationStatus::RetryLater => return Err(RequestExecutionError::StorageBusy),
        };
        if object_exists {
            return Err(RequestExecutionError::WrongType);
        }

        if !string_exists {
            current_value.clear();
        }
        current_value.extend_from_slice(append_value);
        let expiration_unix_millis = self.expiration_unix_millis_for_key(&key);

        let mut upsert_info = UpsertInfo::default();
        if expiration_unix_millis.is_some() {
            upsert_info.user_data |= UPSERT_USER_DATA_HAS_EXPIRATION;
        }
        let stored_value = encode_stored_value(&current_value, expiration_unix_millis);
        let mut upsert_output = Vec::new();
        session
            .upsert(&key, &stored_value, &mut upsert_output, &mut upsert_info)
            .map_err(map_upsert_error)?;
        drop(object_session);
        drop(object_store);
        drop(session);
        drop(store);

        self.track_string_key_in_shard(&key, shard_index);
        self.bump_watch_version(&key);
        append_integer(response_out, current_value.len() as i64);
        Ok(())
    }

    pub(super) fn handle_getex(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let action = parse_getex_action(args)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;

        let Some(value) = self.read_string_value(&key)? else {
            if self.object_key_exists(&key)? {
                return Err(RequestExecutionError::WrongType);
            }
            append_null_bulk_string(response_out);
            return Ok(());
        };

        match action {
            GetExAction::KeepTtl => {}
            GetExAction::Persist => {
                if self.string_expiration_deadline(&key).is_some() {
                    self.set_string_expiration_deadline(&key, None);
                    if !self.rewrite_existing_value_expiration(&key, None)? {
                        return Err(storage_failure(
                            "getex",
                            "string key disappeared while clearing expiration",
                        ));
                    }
                    self.bump_watch_version(&key);
                }
            }
            GetExAction::SetExpiration(expiration) => {
                self.set_string_expiration_deadline(&key, Some(expiration.deadline));
                if !self.rewrite_existing_value_expiration(&key, Some(expiration.unix_millis))? {
                    self.set_string_expiration_deadline(&key, None);
                    return Err(storage_failure(
                        "getex",
                        "string key disappeared while rewriting expiration",
                    ));
                }
                self.bump_watch_version(&key);
            }
            GetExAction::DeleteNow => {
                let mut store = self.lock_string_store_for_key(&key);
                let mut session = store.session(&self.functions);
                let mut info = DeleteInfo::default();
                let status = session.delete(&key, &mut info).map_err(map_delete_error)?;
                match status {
                    DeleteOperationStatus::TombstonedInPlace
                    | DeleteOperationStatus::AppendedTombstone => {
                        self.remove_string_key_metadata(&key);
                        self.bump_watch_version(&key);
                    }
                    DeleteOperationStatus::NotFound => {}
                    DeleteOperationStatus::RetryLater => {
                        return Err(RequestExecutionError::StorageBusy);
                    }
                }
            }
        }

        append_bulk_string(response_out, &value);
        Ok(())
    }

    pub(super) fn handle_incrbyfloat(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "INCRBYFLOAT",
                expected: "INCRBYFLOAT key increment",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let increment = parse_f64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotFloat)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;

        let (current, expiration_unix_millis) = match self.read_string_value(&key)? {
            Some(value) => (
                parse_f64_ascii(&value).ok_or(RequestExecutionError::ValueNotFloat)?,
                self.expiration_unix_millis_for_key(&key),
            ),
            None => {
                if self.object_key_exists(&key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                (0.0, None)
            }
        };

        let updated = current + increment;
        if !updated.is_finite() {
            return Err(RequestExecutionError::ValueNotFloat);
        }
        let updated_text = updated.to_string().into_bytes();

        let mut upsert_info = UpsertInfo::default();
        if expiration_unix_millis.is_some() {
            upsert_info.user_data |= UPSERT_USER_DATA_HAS_EXPIRATION;
        }
        let stored_value = encode_stored_value(&updated_text, expiration_unix_millis);
        let mut upsert_output = Vec::new();
        let mut store = self.lock_string_store_for_key(&key);
        let mut session = store.session(&self.functions);
        session
            .upsert(&key, &stored_value, &mut upsert_output, &mut upsert_info)
            .map_err(map_upsert_error)?;
        self.track_string_key(&key);
        self.bump_watch_version(&key);
        append_bulk_string(response_out, &updated_text);
        Ok(())
    }

    pub(super) fn handle_set(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        crate::debug_sync_point!("request_processor.handle_set.enter");
        if args.len() < 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "SET",
                expected: "SET key value",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let value = unsafe { args[2].as_slice() }.to_vec();
        let shard_index = self.string_store_shard_index_for_key(&key);
        let options = parse_set_options(args)?;
        self.expire_key_if_needed_in_shard(&key, shard_index)?;
        crate::debug_sync_point!("request_processor.handle_set.before_store_lock");

        let mut store = self.lock_string_store_for_shard(shard_index);
        crate::debug_sync_point!("request_processor.handle_set.after_store_lock");
        let mut session = store.session(&self.functions);
        let mut object_store = self.lock_object_store_for_shard(shard_index);
        let mut object_session = object_store.session(&self.object_functions);

        let mut existence_output = Vec::new();
        let string_exists = match session
            .read(
                &key,
                &Vec::new(),
                &mut existence_output,
                &ReadInfo::default(),
            )
            .map_err(map_read_error)?
        {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => true,
            ReadOperationStatus::NotFound => false,
            ReadOperationStatus::RetryLater => return Err(RequestExecutionError::StorageBusy),
        };

        existence_output.clear();
        let object_exists = match object_session
            .read(
                &key,
                &Vec::new(),
                &mut existence_output,
                &ReadInfo::default(),
            )
            .map_err(map_read_error)?
        {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => true,
            ReadOperationStatus::NotFound => false,
            ReadOperationStatus::RetryLater => return Err(RequestExecutionError::StorageBusy),
        };

        if options.only_if_absent || options.only_if_present {
            let exists_any = string_exists || object_exists;
            if options.only_if_absent && exists_any {
                append_null_bulk_string(response_out);
                return Ok(());
            }
            if options.only_if_present && !exists_any {
                append_null_bulk_string(response_out);
                return Ok(());
            }
        }

        let mut output = Vec::new();
        let mut info = UpsertInfo::default();
        let stored_value = encode_stored_value(
            &value,
            options.expiration.map(|expiration| expiration.unix_millis),
        );
        if options.expiration.is_some() {
            info.user_data |= UPSERT_USER_DATA_HAS_EXPIRATION;
        }
        session
            .upsert(&key, &stored_value, &mut output, &mut info)
            .map_err(map_upsert_error)?;
        if object_exists {
            let mut delete_info = DeleteInfo::default();
            let status = object_session
                .delete(&key, &mut delete_info)
                .map_err(map_delete_error)?;
            if matches!(status, DeleteOperationStatus::RetryLater) {
                return Err(RequestExecutionError::StorageBusy);
            }
        }
        drop(object_session);
        drop(object_store);
        drop(session);
        drop(store);
        crate::debug_sync_point!("request_processor.handle_set.before_metadata_locks");

        if object_exists {
            self.untrack_object_key_in_shard(&key, shard_index);
        }
        self.set_string_expiration_deadline_in_shard(
            &key,
            shard_index,
            options.expiration.map(|e| e.deadline),
        );
        self.track_string_key_in_shard(&key, shard_index);
        self.bump_watch_version(&key);

        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_setnx(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "SETNX",
                expected: "SETNX key value",
            });
        }
        let set_command = ArgSlice::from_slice(b"SET")
            .expect("static SET command name must fit within ArgSlice length");
        let nx_option =
            ArgSlice::from_slice(b"NX").expect("static NX option must fit within ArgSlice length");
        let translated = [set_command, args[1], args[2], nx_option];
        let mut set_response = Vec::new();
        self.handle_set(&translated, &mut set_response)?;
        if set_response == b"+OK\r\n" {
            append_integer(response_out, 1);
        } else {
            append_integer(response_out, 0);
        }
        Ok(())
    }

    pub(super) fn handle_setex(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 4 {
            return Err(RequestExecutionError::WrongArity {
                command: "SETEX",
                expected: "SETEX key seconds value",
            });
        }
        let set_command = ArgSlice::from_slice(b"SET")
            .expect("static SET command name must fit within ArgSlice length");
        let ex_option =
            ArgSlice::from_slice(b"EX").expect("static EX option must fit within ArgSlice length");
        let translated = [set_command, args[1], args[3], ex_option, args[2]];
        self.handle_set(&translated, response_out)
    }

    pub(super) fn handle_psetex(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 4 {
            return Err(RequestExecutionError::WrongArity {
                command: "PSETEX",
                expected: "PSETEX key milliseconds value",
            });
        }
        let set_command = ArgSlice::from_slice(b"SET")
            .expect("static SET command name must fit within ArgSlice length");
        let px_option =
            ArgSlice::from_slice(b"PX").expect("static PX option must fit within ArgSlice length");
        let translated = [set_command, args[1], args[3], px_option, args[2]];
        self.handle_set(&translated, response_out)
    }

    pub(super) fn handle_getset(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "GETSET",
                expected: "GETSET key value",
            });
        }

        let get_command = ArgSlice::from_slice(b"GET")
            .expect("static GET command name must fit within ArgSlice length");
        let set_command = ArgSlice::from_slice(b"SET")
            .expect("static SET command name must fit within ArgSlice length");

        let mut previous_value_response = Vec::new();
        let get_args = [get_command, args[1]];
        self.handle_get(&get_args, &mut previous_value_response)?;

        let set_args = [set_command, args[1], args[2]];
        let mut set_response = Vec::new();
        self.handle_set(&set_args, &mut set_response)?;

        response_out.extend_from_slice(&previous_value_response);
        Ok(())
    }

    pub(super) fn handle_getdel(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "GETDEL",
                expected: "GETDEL key",
            });
        }

        let get_command = ArgSlice::from_slice(b"GET")
            .expect("static GET command name must fit within ArgSlice length");
        let del_command = ArgSlice::from_slice(b"DEL")
            .expect("static DEL command name must fit within ArgSlice length");

        let get_args = [get_command, args[1]];
        let mut previous_value_response = Vec::new();
        self.handle_get(&get_args, &mut previous_value_response)?;
        if previous_value_response.as_slice() == b"$-1\r\n" {
            response_out.extend_from_slice(&previous_value_response);
            return Ok(());
        }

        let mut del_response = Vec::new();
        let del_args = [del_command, args[1]];
        self.handle_del(&del_args, &mut del_response)?;
        response_out.extend_from_slice(&previous_value_response);
        Ok(())
    }

    pub(super) fn handle_del(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "DEL",
                expected: "DEL key [key ...]",
            });
        }

        let keys: Vec<Vec<u8>> = args[1..]
            .iter()
            .map(|arg| {
                // SAFETY: caller guarantees argument backing memory validity.
                unsafe { arg.as_slice() }.to_vec()
            })
            .collect();

        for key in &keys {
            self.expire_key_if_needed(key)?;
        }

        let mut deleted = 0i64;
        for key in keys {
            let mut string_deleted = false;
            let mut store = self.lock_string_store_for_key(&key);
            let mut session = store.session(&self.functions);
            let mut info = DeleteInfo::default();
            let status = session.delete(&key, &mut info).map_err(map_delete_error)?;
            match status {
                DeleteOperationStatus::TombstonedInPlace
                | DeleteOperationStatus::AppendedTombstone => {
                    string_deleted = true;
                    self.remove_string_key_metadata(&key);
                }
                DeleteOperationStatus::NotFound => {}
                DeleteOperationStatus::RetryLater => {
                    return Err(RequestExecutionError::StorageBusy);
                }
            }

            let object_deleted = self.object_delete(&key)?;
            if string_deleted || object_deleted {
                deleted += 1;
            }
            if string_deleted && !object_deleted {
                self.bump_watch_version(&key);
            }
        }

        append_integer(response_out, deleted);
        Ok(())
    }

    pub(super) fn handle_touch(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "TOUCH",
                expected: "TOUCH key [key ...]",
            });
        }

        let mut touched = 0i64;
        for arg in &args[1..] {
            // SAFETY: caller guarantees argument backing memory validity.
            let key = unsafe { arg.as_slice() }.to_vec();
            self.expire_key_if_needed(&key)?;
            if self.key_exists_any(&key)? {
                touched += 1;
            }
        }
        append_integer(response_out, touched);
        Ok(())
    }

    pub(super) fn handle_unlink(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "UNLINK",
                expected: "UNLINK key [key ...]",
            });
        }
        self.handle_del(args, response_out)
    }

    pub(super) fn handle_rename(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_rename_internal(args, false, response_out)
    }

    pub(super) fn handle_renamenx(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_rename_internal(args, true, response_out)
    }

    fn handle_rename_internal(
        &self,
        args: &[ArgSlice],
        only_if_absent: bool,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity {
                command: if only_if_absent { "RENAMENX" } else { "RENAME" },
                expected: if only_if_absent {
                    "RENAMENX key newkey"
                } else {
                    "RENAME key newkey"
                },
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let source = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let destination = unsafe { args[2].as_slice() }.to_vec();

        self.expire_key_if_needed(&source)?;
        self.expire_key_if_needed(&destination)?;

        let Some(mut source_entry) = self.export_migration_entry(&source)? else {
            return Err(RequestExecutionError::NoSuchKey);
        };

        if source == destination {
            if only_if_absent {
                append_integer(response_out, 0);
            } else {
                append_simple_string(response_out, b"OK");
            }
            return Ok(());
        }

        if only_if_absent && self.key_exists_any(&destination)? {
            append_integer(response_out, 0);
            return Ok(());
        }

        source_entry.key = destination.clone();
        self.import_migration_entry(&source_entry)?;
        self.delete_string_key_for_migration(&source)?;
        let _ = self.object_delete(&source)?;

        if only_if_absent {
            append_integer(response_out, 1);
        } else {
            append_simple_string(response_out, b"OK");
        }
        Ok(())
    }

    pub(super) fn handle_copy(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "COPY",
                expected: "COPY source destination [DB destination-db] [REPLACE]",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let source = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let destination = unsafe { args[2].as_slice() }.to_vec();

        let mut replace = false;
        let mut destination_db = 0u64;
        let mut index = 3usize;
        while index < args.len() {
            // SAFETY: caller guarantees argument backing memory validity.
            let option = unsafe { args[index].as_slice() };
            if ascii_eq_ignore_case(option, b"REPLACE") {
                replace = true;
                index += 1;
                continue;
            }
            if ascii_eq_ignore_case(option, b"DB") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                // SAFETY: caller guarantees argument backing memory validity.
                let db_value = unsafe { args[index + 1].as_slice() };
                destination_db =
                    parse_u64_ascii(db_value).ok_or(RequestExecutionError::ValueNotInteger)?;
                index += 2;
                continue;
            }
            return Err(RequestExecutionError::SyntaxError);
        }

        if destination_db != 0 {
            return Err(RequestExecutionError::ValueNotInteger);
        }

        self.expire_key_if_needed(&source)?;
        self.expire_key_if_needed(&destination)?;

        let Some(mut source_entry) = self.export_migration_entry(&source)? else {
            append_integer(response_out, 0);
            return Ok(());
        };

        if source == destination {
            append_integer(response_out, 0);
            return Ok(());
        }

        if !replace && self.key_exists_any(&destination)? {
            append_integer(response_out, 0);
            return Ok(());
        }

        source_entry.key = destination;
        self.import_migration_entry(&source_entry)?;
        append_integer(response_out, 1);
        Ok(())
    }

    pub(super) fn handle_incr_decr(
        &self,
        args: &[ArgSlice],
        delta: i64,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: if delta > 0 { "INCR" } else { "DECR" },
                expected: if delta > 0 { "INCR key" } else { "DECR key" },
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.apply_incr_decr_delta(&key, delta, response_out)
    }

    pub(super) fn handle_incrby_decrby(
        &self,
        args: &[ArgSlice],
        decrement: bool,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if decrement {
            ("DECRBY", "DECRBY key decrement")
        } else {
            ("INCRBY", "INCRBY key increment")
        };
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity { command, expected });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let amount = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        let delta = if decrement {
            amount
                .checked_neg()
                .ok_or(RequestExecutionError::ValueNotInteger)?
        } else {
            amount
        };
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.apply_incr_decr_delta(&key, delta, response_out)
    }

    fn apply_incr_decr_delta(
        &self,
        key: &[u8],
        delta: i64,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.expire_key_if_needed(&key)?;
        if !self.key_exists(&key)? && self.object_key_exists(&key)? {
            return Err(RequestExecutionError::WrongType);
        }
        let input = delta.to_string().into_bytes();
        let mut output = Vec::new();
        let mut info = RmwInfo::default();
        let mut store = self.lock_string_store_for_key(&key);
        let mut session = store.session(&self.functions);
        let status = session.rmw(&key.to_vec(), &input, &mut output, &mut info);

        match status {
            Ok(RmwOperationStatus::InPlaceUpdated)
            | Ok(RmwOperationStatus::CopiedToTail)
            | Ok(RmwOperationStatus::Inserted) => {
                let parsed =
                    parse_i64_ascii(&output).ok_or(RequestExecutionError::ValueNotInteger)?;
                self.track_string_key(&key);
                self.bump_watch_version(&key);
                append_integer(response_out, parsed);
                Ok(())
            }
            Ok(RmwOperationStatus::RetryLater) => Err(RequestExecutionError::StorageBusy),
            Ok(RmwOperationStatus::NotFound) => {
                if self.object_key_exists(&key)? {
                    return Err(RequestExecutionError::WrongType);
                }
                let mut upsert_info = UpsertInfo::default();
                let mut upsert_output = Vec::new();
                let stored_value = encode_stored_value(&input, None);
                session
                    .upsert(
                        &key.to_vec(),
                        &stored_value,
                        &mut upsert_output,
                        &mut upsert_info,
                    )
                    .map_err(map_upsert_error)?;
                self.track_string_key(&key);
                self.bump_watch_version(&key);
                append_integer(response_out, delta);
                Ok(())
            }
            Err(RmwOperationError::OperationCancelled) => {
                Err(RequestExecutionError::ValueNotInteger)
            }
            Err(error) => Err(map_rmw_error(error)),
        }
    }

    pub(super) fn handle_exists(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "EXISTS",
                expected: "EXISTS key [key ...]",
            });
        }

        let mut exists = 0i64;
        for arg in &args[1..] {
            // SAFETY: caller guarantees argument backing memory validity.
            let key = unsafe { arg.as_slice() }.to_vec();
            self.expire_key_if_needed(&key)?;
            if self.key_exists_any(&key)? {
                exists += 1;
            }
        }

        append_integer(response_out, exists);
        Ok(())
    }

    pub(super) fn handle_type(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "TYPE",
                expected: "TYPE key",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;
        if self.key_exists(&key)? {
            append_simple_string(response_out, b"string");
            return Ok(());
        }

        let value_type = match self.object_read(&key)? {
            Some((object_type, _)) => object_type_name(object_type).ok_or_else(|| {
                storage_failure("type", "unknown object type tag in object store")
            })?,
            None => b"none",
        };
        append_simple_string(response_out, value_type);
        Ok(())
    }

    pub(super) fn handle_mget(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "MGET",
                expected: "MGET key [key ...]",
            });
        }

        response_out.push(b'*');
        response_out.extend_from_slice((args.len() - 1).to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for arg in &args[1..] {
            // SAFETY: caller guarantees argument backing memory validity.
            let key = unsafe { arg.as_slice() }.to_vec();
            self.expire_key_if_needed(&key)?;
            if let Some(value) = self.read_string_value(&key)? {
                append_bulk_string(response_out, &value);
            } else {
                append_null_bulk_string(response_out);
            }
        }
        Ok(())
    }

    pub(super) fn handle_mset(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 || args.len() % 2 == 0 {
            return Err(RequestExecutionError::WrongArity {
                command: "MSET",
                expected: "MSET key value [key value ...]",
            });
        }

        for pair in args[1..].chunks_exact(2) {
            // SAFETY: caller guarantees argument backing memory validity.
            let key = unsafe { pair[0].as_slice() }.to_vec();
            // SAFETY: caller guarantees argument backing memory validity.
            let value = unsafe { pair[1].as_slice() }.to_vec();
            self.mset_single_pair(&key, &value)?;
        }

        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_msetnx(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 || args.len() % 2 == 0 {
            return Err(RequestExecutionError::WrongArity {
                command: "MSETNX",
                expected: "MSETNX key value [key value ...]",
            });
        }

        let key_value_pairs: Vec<(Vec<u8>, Vec<u8>)> = args[1..]
            .chunks_exact(2)
            .map(|pair| {
                // SAFETY: caller guarantees argument backing memory validity.
                (
                    unsafe { pair[0].as_slice() }.to_vec(),
                    // SAFETY: caller guarantees argument backing memory validity.
                    unsafe { pair[1].as_slice() }.to_vec(),
                )
            })
            .collect();

        for (key, _) in &key_value_pairs {
            self.expire_key_if_needed(key)?;
            if self.key_exists_any(key)? {
                append_integer(response_out, 0);
                return Ok(());
            }
        }

        for (key, value) in &key_value_pairs {
            self.mset_single_pair(key, value)?;
        }
        append_integer(response_out, 1);
        Ok(())
    }

    pub(super) fn handle_pfadd(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "PFADD",
                expected: "PFADD key element [element ...]",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut set = load_pf_set_for_key(self, &key)?.unwrap_or_default();
        let original_len = set.len();
        for element in &args[2..] {
            // SAFETY: caller guarantees argument backing memory validity.
            set.insert(unsafe { element.as_slice() }.to_vec());
        }
        let changed = if set.len() != original_len { 1 } else { 0 };
        self.upsert_string_value_for_migration(&key, &encode_pf_set(&set), None)?;
        append_integer(response_out, changed);
        Ok(())
    }

    pub(super) fn handle_pfcount(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "PFCOUNT",
                expected: "PFCOUNT key [key ...]",
            });
        }
        let mut cardinality_union = BTreeSet::new();
        for key_arg in &args[1..] {
            // SAFETY: caller guarantees argument backing memory validity.
            let key = unsafe { key_arg.as_slice() }.to_vec();
            if let Some(set) = load_pf_set_for_key(self, &key)? {
                cardinality_union.extend(set);
            }
        }
        append_integer(response_out, cardinality_union.len() as i64);
        Ok(())
    }

    pub(super) fn handle_pfmerge(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "PFMERGE",
                expected: "PFMERGE destkey sourcekey [sourcekey ...]",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let destination = unsafe { args[1].as_slice() }.to_vec();
        let _ = load_pf_set_for_key(self, &destination)?;

        let mut merged = BTreeSet::new();
        for source_arg in &args[2..] {
            // SAFETY: caller guarantees argument backing memory validity.
            let source = unsafe { source_arg.as_slice() }.to_vec();
            if let Some(set) = load_pf_set_for_key(self, &source)? {
                merged.extend(set);
            }
        }
        self.upsert_string_value_for_migration(&destination, &encode_pf_set(&merged), None)?;
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_pfdebug(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "PFDEBUG",
                expected: "PFDEBUG <ENCODING|TODENSE|HELP> [key]",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let subcommand = unsafe { args[1].as_slice() };
        if ascii_eq_ignore_case(subcommand, b"HELP") {
            if args.len() != 2 {
                return Err(RequestExecutionError::WrongArity {
                    command: "PFDEBUG",
                    expected: "PFDEBUG HELP",
                });
            }
            append_bulk_array(response_out, &PFDEBUG_HELP_LINES);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"ENCODING") {
            if args.len() != 3 {
                return Err(RequestExecutionError::WrongArity {
                    command: "PFDEBUG",
                    expected: "PFDEBUG ENCODING key",
                });
            }
            append_bulk_string(response_out, b"sparse");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"TODENSE") {
            if args.len() != 3 {
                return Err(RequestExecutionError::WrongArity {
                    command: "PFDEBUG",
                    expected: "PFDEBUG TODENSE key",
                });
            }
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        Err(RequestExecutionError::UnknownCommand)
    }

    pub(super) fn handle_pfselftest(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 1 {
            return Err(RequestExecutionError::WrongArity {
                command: "PFSELFTEST",
                expected: "PFSELFTEST",
            });
        }
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    fn mset_single_pair(&self, key: &[u8], value: &[u8]) -> Result<(), RequestExecutionError> {
        let shard_index = self.string_store_shard_index_for_key(key);
        self.expire_key_if_needed_in_shard(key, shard_index)?;

        let key_vec = key.to_vec();
        let mut store = self.lock_string_store_for_shard(shard_index);
        let mut session = store.session(&self.functions);
        let mut object_store = self.lock_object_store_for_shard(shard_index);
        let mut object_session = object_store.session(&self.object_functions);

        let mut existence_output = Vec::new();
        let object_exists = match object_session
            .read(
                &key_vec,
                &Vec::new(),
                &mut existence_output,
                &ReadInfo::default(),
            )
            .map_err(map_read_error)?
        {
            ReadOperationStatus::FoundInMemory | ReadOperationStatus::FoundOnDisk => true,
            ReadOperationStatus::NotFound => false,
            ReadOperationStatus::RetryLater => return Err(RequestExecutionError::StorageBusy),
        };

        let mut output = Vec::new();
        let mut info = UpsertInfo::default();
        let stored_value = encode_stored_value(value, None);
        session
            .upsert(&key_vec, &stored_value, &mut output, &mut info)
            .map_err(map_upsert_error)?;
        if object_exists {
            let mut delete_info = DeleteInfo::default();
            let status = object_session
                .delete(&key_vec, &mut delete_info)
                .map_err(map_delete_error)?;
            if matches!(status, DeleteOperationStatus::RetryLater) {
                return Err(RequestExecutionError::StorageBusy);
            }
        }
        drop(object_session);
        drop(object_store);
        drop(session);
        drop(store);

        if object_exists {
            self.untrack_object_key_in_shard(&key_vec, shard_index);
        }
        self.set_string_expiration_deadline_in_shard(&key_vec, shard_index, None);
        self.track_string_key_in_shard(&key_vec, shard_index);
        self.bump_watch_version(&key_vec);
        Ok(())
    }

    pub(super) fn handle_expire(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_expire_like(args, response_out, false)
    }

    pub(super) fn handle_pexpire(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_expire_like(args, response_out, true)
    }

    pub(super) fn handle_expireat(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_expireat_like(args, response_out, false)
    }

    pub(super) fn handle_pexpireat(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_expireat_like(args, response_out, true)
    }

    fn handle_expire_like(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
        milliseconds: bool,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if milliseconds {
            ("PEXPIRE", "PEXPIRE key milliseconds")
        } else {
            ("EXPIRE", "EXPIRE key seconds")
        };
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity { command, expected });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let amount = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();

        self.expire_key_if_needed(&key)?;
        let string_exists = self.key_exists(&key)?;
        let object_exists = self.object_key_exists(&key)?;
        if !string_exists && !object_exists {
            append_integer(response_out, 0);
            return Ok(());
        }

        if amount <= 0 {
            return self.expire_existing_key_immediately(
                &key,
                string_exists,
                object_exists,
                response_out,
            );
        }

        let duration = if milliseconds {
            Duration::from_millis(amount as u64)
        } else {
            Duration::from_secs(amount as u64)
        };
        let expiration = expiration_metadata_from_duration(duration)
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        self.set_string_expiration_deadline(&key, Some(expiration.deadline));
        if string_exists
            && !self.rewrite_existing_value_expiration(&key, Some(expiration.unix_millis))?
        {
            self.set_string_expiration_deadline(&key, None);
            append_integer(response_out, 0);
            return Ok(());
        }
        self.bump_watch_version(&key);
        append_integer(response_out, 1);
        Ok(())
    }

    fn handle_expireat_like(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
        milliseconds: bool,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if milliseconds {
            ("PEXPIREAT", "PEXPIREAT key milliseconds-unix-time")
        } else {
            ("EXPIREAT", "EXPIREAT key seconds-unix-time")
        };
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity { command, expected });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let amount = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();

        self.expire_key_if_needed(&key)?;
        let string_exists = self.key_exists(&key)?;
        let object_exists = self.object_key_exists(&key)?;
        if !string_exists && !object_exists {
            append_integer(response_out, 0);
            return Ok(());
        }

        let target_unix_millis = if milliseconds {
            i128::from(amount)
        } else {
            i128::from(amount)
                .checked_mul(1000)
                .ok_or(RequestExecutionError::ValueNotInteger)?
        };
        let now_unix_millis =
            i128::from(current_unix_time_millis().ok_or(RequestExecutionError::ValueNotInteger)?);

        if target_unix_millis <= now_unix_millis {
            return self.expire_existing_key_immediately(
                &key,
                string_exists,
                object_exists,
                response_out,
            );
        }

        let unix_millis = u64::try_from(target_unix_millis)
            .map_err(|_| RequestExecutionError::ValueNotInteger)?;
        let deadline =
            instant_from_unix_millis(unix_millis).ok_or(RequestExecutionError::ValueNotInteger)?;
        self.set_string_expiration_deadline(&key, Some(deadline));
        if string_exists && !self.rewrite_existing_value_expiration(&key, Some(unix_millis))? {
            self.set_string_expiration_deadline(&key, None);
            append_integer(response_out, 0);
            return Ok(());
        }
        self.bump_watch_version(&key);
        append_integer(response_out, 1);
        Ok(())
    }

    fn expire_existing_key_immediately(
        &self,
        key: &[u8],
        string_exists: bool,
        object_exists: bool,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let key_vec = key.to_vec();
        let mut string_deleted = false;
        if string_exists {
            let mut store = self.lock_string_store_for_key(key);
            let mut session = store.session(&self.functions);
            let mut info = DeleteInfo::default();
            let status = session
                .delete(&key_vec, &mut info)
                .map_err(map_delete_error)?;
            match status {
                DeleteOperationStatus::TombstonedInPlace
                | DeleteOperationStatus::AppendedTombstone => {
                    string_deleted = true;
                }
                DeleteOperationStatus::NotFound => {}
                DeleteOperationStatus::RetryLater => {
                    return Err(RequestExecutionError::StorageBusy);
                }
            }
            self.remove_string_key_metadata(key);
        }

        let object_deleted = if object_exists {
            self.object_delete(key)?
        } else {
            false
        };
        if string_deleted || object_deleted {
            if string_deleted && !object_deleted {
                self.bump_watch_version(key);
            }
            append_integer(response_out, 1);
        } else {
            append_integer(response_out, 0);
        }
        Ok(())
    }

    pub(super) fn handle_ttl(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_ttl_like(args, response_out, false)
    }

    pub(super) fn handle_pttl(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_ttl_like(args, response_out, true)
    }

    pub(super) fn handle_expiretime(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_expiretime_like(args, response_out, false)
    }

    pub(super) fn handle_pexpiretime(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_expiretime_like(args, response_out, true)
    }

    fn handle_ttl_like(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
        milliseconds: bool,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if milliseconds {
            ("PTTL", "PTTL key")
        } else {
            ("TTL", "TTL key")
        };
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity { command, expected });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;

        if !self.key_exists_any(&key)? {
            append_integer(response_out, -2);
            return Ok(());
        }

        let deadline = self.string_expiration_deadline(&key);
        match deadline {
            None => append_integer(response_out, -1),
            Some(deadline) => {
                let now = Instant::now();
                if deadline <= now {
                    self.expire_key_if_needed(&key)?;
                    append_integer(response_out, -2);
                } else {
                    let remaining = deadline.duration_since(now);
                    let ttl = if milliseconds {
                        remaining.as_millis().min(i64::MAX as u128) as i64
                    } else {
                        remaining.as_secs().min(i64::MAX as u64) as i64
                    };
                    append_integer(response_out, ttl);
                }
            }
        }
        Ok(())
    }

    fn handle_expiretime_like(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
        milliseconds: bool,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if milliseconds {
            ("PEXPIRETIME", "PEXPIRETIME key")
        } else {
            ("EXPIRETIME", "EXPIRETIME key")
        };
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity { command, expected });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;

        if !self.key_exists_any(&key)? {
            append_integer(response_out, -2);
            return Ok(());
        }

        match self.string_expiration_deadline(&key) {
            None => {
                append_integer(response_out, -1);
                Ok(())
            }
            Some(deadline) => {
                let now = Instant::now();
                if deadline <= now {
                    self.expire_key_if_needed(&key)?;
                    append_integer(response_out, -2);
                    return Ok(());
                }

                let now_unix_millis = u128::from(
                    current_unix_time_millis().ok_or(RequestExecutionError::ValueNotInteger)?,
                );
                let expiration_unix_millis = now_unix_millis
                    .checked_add(deadline.duration_since(now).as_millis())
                    .ok_or(RequestExecutionError::ValueNotInteger)?;
                let value = if milliseconds {
                    expiration_unix_millis.min(i64::MAX as u128) as i64
                } else {
                    (expiration_unix_millis / 1000).min(i64::MAX as u128) as i64
                };
                append_integer(response_out, value);
                Ok(())
            }
        }
    }

    pub(super) fn handle_persist(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "PERSIST",
                expected: "PERSIST key",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;
        let string_exists = self.key_exists(&key)?;
        let object_exists = self.object_key_exists(&key)?;
        if !string_exists && !object_exists {
            append_integer(response_out, 0);
            return Ok(());
        }

        let shard_index = self.string_store_shard_index_for_key(&key);
        let removed_deadline = {
            let mut expirations = self.lock_string_expirations_for_shard(shard_index);
            let removed = expirations.remove(&key).is_some();
            if removed {
                self.decrement_string_expiration_count(shard_index);
            }
            removed
        };
        if !removed_deadline {
            append_integer(response_out, 0);
            return Ok(());
        }

        if string_exists && !self.rewrite_existing_value_expiration(&key, None)? {
            append_integer(response_out, 0);
            return Ok(());
        }

        self.bump_watch_version(&key);
        append_integer(response_out, 1);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum GetExAction {
    KeepTtl,
    Persist,
    SetExpiration(ExpirationMetadata),
    DeleteNow,
}

fn parse_getex_action(args: &[ArgSlice]) -> Result<GetExAction, RequestExecutionError> {
    if args.len() < 2 {
        return Err(RequestExecutionError::WrongArity {
            command: "GETEX",
            expected: "GETEX key [EX seconds|PX milliseconds|EXAT unix-time-seconds|PXAT unix-time-milliseconds|PERSIST]",
        });
    }
    if args.len() == 2 {
        return Ok(GetExAction::KeepTtl);
    }
    if args.len() == 3 {
        // SAFETY: caller guarantees argument backing memory validity.
        let option = unsafe { args[2].as_slice() };
        if ascii_eq_ignore_case(option, b"PERSIST") {
            return Ok(GetExAction::Persist);
        }
        return Err(RequestExecutionError::SyntaxError);
    }
    if args.len() != 4 {
        return Err(RequestExecutionError::WrongArity {
            command: "GETEX",
            expected: "GETEX key [EX seconds|PX milliseconds|EXAT unix-time-seconds|PXAT unix-time-milliseconds|PERSIST]",
        });
    }

    // SAFETY: caller guarantees argument backing memory validity.
    let option = unsafe { args[2].as_slice() };
    // SAFETY: caller guarantees argument backing memory validity.
    let value = unsafe { args[3].as_slice() };

    if ascii_eq_ignore_case(option, b"EX") || ascii_eq_ignore_case(option, b"PX") {
        let amount = parse_u64_ascii(value).ok_or(RequestExecutionError::InvalidGetExExpireTime)?;
        if amount == 0 {
            return Err(RequestExecutionError::InvalidGetExExpireTime);
        }
        let duration = if ascii_eq_ignore_case(option, b"EX") {
            Duration::from_secs(amount)
        } else {
            Duration::from_millis(amount)
        };
        let expiration = expiration_metadata_from_duration(duration)
            .ok_or(RequestExecutionError::InvalidGetExExpireTime)?;
        return Ok(GetExAction::SetExpiration(expiration));
    }

    if ascii_eq_ignore_case(option, b"EXAT") || ascii_eq_ignore_case(option, b"PXAT") {
        let amount = parse_u64_ascii(value).ok_or(RequestExecutionError::InvalidGetExExpireTime)?;
        let unix_millis = if ascii_eq_ignore_case(option, b"EXAT") {
            amount
                .checked_mul(1000)
                .ok_or(RequestExecutionError::InvalidGetExExpireTime)?
        } else {
            amount
        };
        let now_unix_millis =
            current_unix_time_millis().ok_or(RequestExecutionError::InvalidGetExExpireTime)?;
        if unix_millis <= now_unix_millis {
            return Ok(GetExAction::DeleteNow);
        }
        let deadline = instant_from_unix_millis(unix_millis)
            .ok_or(RequestExecutionError::InvalidGetExExpireTime)?;
        return Ok(GetExAction::SetExpiration(ExpirationMetadata {
            deadline,
            unix_millis,
        }));
    }

    Err(RequestExecutionError::SyntaxError)
}

fn parse_bitop_operation(token: &[u8]) -> Option<BitopOperation> {
    if ascii_eq_ignore_case(token, b"AND") {
        return Some(BitopOperation::And);
    }
    if ascii_eq_ignore_case(token, b"OR") {
        return Some(BitopOperation::Or);
    }
    if ascii_eq_ignore_case(token, b"XOR") {
        return Some(BitopOperation::Xor);
    }
    if ascii_eq_ignore_case(token, b"NOT") {
        return Some(BitopOperation::Not);
    }
    None
}

fn parse_bitfield_overflow_mode(token: &[u8]) -> Option<BitfieldOverflowMode> {
    if ascii_eq_ignore_case(token, b"WRAP") {
        return Some(BitfieldOverflowMode::Wrap);
    }
    if ascii_eq_ignore_case(token, b"SAT") {
        return Some(BitfieldOverflowMode::Sat);
    }
    if ascii_eq_ignore_case(token, b"FAIL") {
        return Some(BitfieldOverflowMode::Fail);
    }
    None
}

fn parse_bitfield_encoding(token: &[u8]) -> Result<BitfieldEncoding, RequestExecutionError> {
    if token.len() < 2 {
        return Err(RequestExecutionError::SyntaxError);
    }
    let signedness = match token[0].to_ascii_uppercase() {
        b'I' => BitfieldSignedness::Signed,
        b'U' => BitfieldSignedness::Unsigned,
        _ => return Err(RequestExecutionError::SyntaxError),
    };
    let bits = parse_u64_ascii(&token[1..]).ok_or(RequestExecutionError::SyntaxError)?;
    let valid = match signedness {
        BitfieldSignedness::Signed => (1..=64).contains(&bits),
        BitfieldSignedness::Unsigned => (1..=63).contains(&bits),
    };
    if !valid {
        return Err(RequestExecutionError::ValueOutOfRange);
    }
    Ok(BitfieldEncoding {
        signedness,
        bits: bits as u8,
    })
}

fn parse_bitfield_offset(token: &[u8], bits: usize) -> Result<usize, RequestExecutionError> {
    let (is_type_relative, raw_token) = match token.split_first() {
        Some((b'#', rest)) => (true, rest),
        _ => (false, token),
    };
    let offset = parse_i64_ascii(raw_token).ok_or(RequestExecutionError::ValueNotInteger)?;
    if offset < 0 {
        return Err(RequestExecutionError::ValueOutOfRange);
    }
    let base = usize::try_from(offset).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
    if is_type_relative {
        base.checked_mul(bits)
            .ok_or(RequestExecutionError::ValueOutOfRange)
    } else {
        Ok(base)
    }
}

fn read_unsigned_bits(
    value: &[u8],
    bit_offset: usize,
    bit_width: usize,
) -> Result<u64, RequestExecutionError> {
    let _ = bit_offset
        .checked_add(bit_width)
        .ok_or(RequestExecutionError::ValueOutOfRange)?;
    let mut raw = 0u64;
    for bit_delta in 0..bit_width {
        let index = bit_offset
            .checked_add(bit_delta)
            .ok_or(RequestExecutionError::ValueOutOfRange)?;
        let byte_index = index / 8;
        let bit_index = 7usize - (index % 8);
        let bit = if byte_index < value.len() {
            (value[byte_index] >> bit_index) & 1
        } else {
            0
        };
        raw = (raw << 1) | u64::from(bit);
    }
    Ok(raw)
}

fn write_unsigned_bits(
    value: &mut Vec<u8>,
    bit_offset: usize,
    bit_width: usize,
    raw: u64,
) -> Result<(), RequestExecutionError> {
    let end_bit = bit_offset
        .checked_add(bit_width)
        .ok_or(RequestExecutionError::ValueOutOfRange)?;
    let required_len = end_bit
        .checked_add(7)
        .ok_or(RequestExecutionError::ValueOutOfRange)?
        / 8;
    if value.len() < required_len {
        value.resize(required_len, 0);
    }

    for bit_delta in 0..bit_width {
        let index = bit_offset
            .checked_add(bit_delta)
            .ok_or(RequestExecutionError::ValueOutOfRange)?;
        let byte_index = index / 8;
        let bit_index = 7usize - (index % 8);
        let shift = bit_width - 1 - bit_delta;
        let bit = ((raw >> shift) & 1) as u8;
        if bit == 1 {
            value[byte_index] |= 1u8 << bit_index;
        } else {
            value[byte_index] &= !(1u8 << bit_index);
        }
    }
    Ok(())
}

fn bitfield_mask(bits: usize) -> u64 {
    if bits == 64 {
        u64::MAX
    } else {
        (1u64 << bits) - 1
    }
}

fn decode_bitfield_raw(raw: u64, encoding: BitfieldEncoding) -> i64 {
    let bits = usize::from(encoding.bits);
    match encoding.signedness {
        BitfieldSignedness::Unsigned => raw as i64,
        BitfieldSignedness::Signed => {
            if bits == 64 {
                raw as i64
            } else {
                let sign_bit = 1u64 << (bits - 1);
                if raw & sign_bit == 0 {
                    raw as i64
                } else {
                    (i128::from(raw) - (1i128 << bits)) as i64
                }
            }
        }
    }
}

fn encode_bitfield_value(value: i64, encoding: BitfieldEncoding) -> u64 {
    let bits = usize::from(encoding.bits);
    (value as u64) & bitfield_mask(bits)
}

fn bitfield_bounds(encoding: BitfieldEncoding) -> (i128, i128) {
    let bits = usize::from(encoding.bits);
    match encoding.signedness {
        BitfieldSignedness::Unsigned => (0, (1i128 << bits) - 1),
        BitfieldSignedness::Signed => {
            if bits == 64 {
                (i64::MIN as i128, i64::MAX as i128)
            } else {
                (-(1i128 << (bits - 1)), (1i128 << (bits - 1)) - 1)
            }
        }
    }
}

fn apply_bitfield_incrby(
    raw: u64,
    encoding: BitfieldEncoding,
    increment: i64,
    overflow_mode: BitfieldOverflowMode,
) -> Result<BitfieldIncrOutcome, RequestExecutionError> {
    let bits = usize::from(encoding.bits);
    let current_value = i128::from(decode_bitfield_raw(raw, encoding));
    let increment_value = i128::from(increment);
    let target = current_value + increment_value;
    let (min, max) = bitfield_bounds(encoding);
    if target >= min && target <= max {
        let result = target as i64;
        return Ok(BitfieldIncrOutcome::Value {
            raw: encode_bitfield_value(result, encoding),
            value: result,
        });
    }

    match overflow_mode {
        BitfieldOverflowMode::Wrap => {
            let modulus = 1i128 << bits;
            let wrapped_raw = ((i128::from(raw) + increment_value).rem_euclid(modulus)) as u64;
            Ok(BitfieldIncrOutcome::Value {
                raw: wrapped_raw,
                value: decode_bitfield_raw(wrapped_raw, encoding),
            })
        }
        BitfieldOverflowMode::Sat => {
            let saturated = if target < min { min } else { max } as i64;
            Ok(BitfieldIncrOutcome::Value {
                raw: encode_bitfield_value(saturated, encoding),
                value: saturated,
            })
        }
        BitfieldOverflowMode::Fail => Ok(BitfieldIncrOutcome::OverflowFail),
    }
}

fn parse_lcs_options(args: &[ArgSlice]) -> Result<LcsOptions, RequestExecutionError> {
    let mut mode = LcsResponseMode::Sequence;
    let mut min_match_len = 0usize;
    let mut with_match_len = false;
    let mut index = 3usize;

    while index < args.len() {
        // SAFETY: caller guarantees argument backing memory validity.
        let token = unsafe { args[index].as_slice() };
        if ascii_eq_ignore_case(token, b"LEN") {
            if mode != LcsResponseMode::Sequence {
                return Err(RequestExecutionError::SyntaxError);
            }
            mode = LcsResponseMode::LengthOnly;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"IDX") {
            if mode != LcsResponseMode::Sequence {
                return Err(RequestExecutionError::SyntaxError);
            }
            mode = LcsResponseMode::Indexes;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"MINMATCHLEN") {
            if mode != LcsResponseMode::Indexes {
                return Err(RequestExecutionError::SyntaxError);
            }
            let value_index = index
                .checked_add(1)
                .ok_or(RequestExecutionError::ValueOutOfRange)?;
            if value_index >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            // SAFETY: caller guarantees argument backing memory validity.
            let min_token = unsafe { args[value_index].as_slice() };
            let parsed =
                parse_i64_ascii(min_token).ok_or(RequestExecutionError::ValueNotInteger)?;
            if parsed < 0 {
                return Err(RequestExecutionError::ValueOutOfRange);
            }
            min_match_len =
                usize::try_from(parsed).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
            index = value_index + 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"WITHMATCHLEN") {
            if mode != LcsResponseMode::Indexes {
                return Err(RequestExecutionError::SyntaxError);
            }
            with_match_len = true;
            index += 1;
            continue;
        }
        return Err(RequestExecutionError::SyntaxError);
    }

    Ok(LcsOptions {
        mode,
        min_match_len,
        with_match_len,
    })
}

fn lcs_sequence_and_matches(left: &[u8], right: &[u8]) -> (usize, Vec<u8>, Vec<LcsMatchSegment>) {
    let left_len = left.len();
    let right_len = right.len();
    let mut dp = vec![vec![0usize; right_len + 1]; left_len + 1];
    for i in (0..left_len).rev() {
        for j in (0..right_len).rev() {
            dp[i][j] = if left[i] == right[j] {
                dp[i + 1][j + 1] + 1
            } else {
                dp[i + 1][j].max(dp[i][j + 1])
            };
        }
    }

    let mut sequence = Vec::with_capacity(dp[0][0]);
    let mut matches = Vec::<LcsMatchSegment>::new();
    let mut i = 0usize;
    let mut j = 0usize;
    while i < left_len && j < right_len {
        if left[i] == right[j] && dp[i][j] == dp[i + 1][j + 1] + 1 {
            sequence.push(left[i]);
            if let Some(last) = matches.last_mut() {
                if i == last.end_left + 1 && j == last.end_right + 1 {
                    last.end_left = i;
                    last.end_right = j;
                    last.length += 1;
                } else {
                    matches.push(LcsMatchSegment {
                        start_left: i,
                        end_left: i,
                        start_right: j,
                        end_right: j,
                        length: 1,
                    });
                }
            } else {
                matches.push(LcsMatchSegment {
                    start_left: i,
                    end_left: i,
                    start_right: j,
                    end_right: j,
                    length: 1,
                });
            }
            i += 1;
            j += 1;
            continue;
        }
        if dp[i + 1][j] >= dp[i][j + 1] {
            i += 1;
        } else {
            j += 1;
        }
    }

    (dp[0][0], sequence, matches)
}

fn append_resp_array_len(response_out: &mut Vec<u8>, len: usize) {
    response_out.extend_from_slice(format!("*{}\r\n", len).as_bytes());
}

fn append_lcs_match_entry(
    response_out: &mut Vec<u8>,
    entry: &LcsMatchSegment,
    with_match_len: bool,
) {
    append_resp_array_len(response_out, if with_match_len { 3 } else { 2 });

    append_resp_array_len(response_out, 2);
    append_integer(response_out, entry.start_left as i64);
    append_integer(response_out, entry.end_left as i64);

    append_resp_array_len(response_out, 2);
    append_integer(response_out, entry.start_right as i64);
    append_integer(response_out, entry.end_right as i64);

    if with_match_len {
        append_integer(response_out, entry.length as i64);
    }
}

fn append_lcs_idx_response(
    response_out: &mut Vec<u8>,
    matches: &[LcsMatchSegment],
    lcs_len: usize,
    with_match_len: bool,
) {
    append_resp_array_len(response_out, 4);
    append_bulk_string(response_out, b"matches");
    append_resp_array_len(response_out, matches.len());
    for entry in matches {
        append_lcs_match_entry(response_out, entry, with_match_len);
    }
    append_bulk_string(response_out, b"len");
    append_integer(response_out, lcs_len as i64);
}

fn apply_bitop(operation: BitopOperation, source_values: &[Vec<u8>]) -> Vec<u8> {
    if source_values.is_empty() {
        return Vec::new();
    }
    match operation {
        BitopOperation::Not => source_values[0].iter().map(|byte| !*byte).collect(),
        BitopOperation::And | BitopOperation::Or | BitopOperation::Xor => {
            let max_len = source_values.iter().map(Vec::len).max().unwrap_or(0);
            let mut result = vec![0u8; max_len];
            for index in 0..max_len {
                let mut value = match operation {
                    BitopOperation::And => 0xFFu8,
                    BitopOperation::Or | BitopOperation::Xor => 0u8,
                    BitopOperation::Not => unreachable!(),
                };
                for source in source_values {
                    let source_byte = source.get(index).copied().unwrap_or(0);
                    value = match operation {
                        BitopOperation::And => value & source_byte,
                        BitopOperation::Or => value | source_byte,
                        BitopOperation::Xor => value ^ source_byte,
                        BitopOperation::Not => unreachable!(),
                    };
                }
                result[index] = value;
            }
            result
        }
    }
}

fn load_pf_set_for_key(
    processor: &RequestProcessor,
    key: &[u8],
) -> Result<Option<BTreeSet<Vec<u8>>>, RequestExecutionError> {
    processor.expire_key_if_needed(key)?;
    let Some(value) = processor.read_string_value(key)? else {
        if processor.object_key_exists(key)? {
            return Err(RequestExecutionError::WrongType);
        }
        return Ok(None);
    };
    decode_pf_set(&value)
        .map(Some)
        .ok_or(RequestExecutionError::WrongType)
}

fn encode_pf_set(values: &BTreeSet<Vec<u8>>) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(PF_STRING_PREFIX.len() + values.len() * 8);
    encoded.extend_from_slice(PF_STRING_PREFIX);
    encoded.extend_from_slice(&serialize_set_object_payload(values));
    encoded
}

fn decode_pf_set(raw: &[u8]) -> Option<BTreeSet<Vec<u8>>> {
    let payload = raw.strip_prefix(PF_STRING_PREFIX)?;
    deserialize_set_object_payload(payload)
}

fn normalize_string_range(len: usize, start: i64, end: i64) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }

    let len_i = len as i128;
    let mut start_i = i128::from(start);
    let mut end_i = i128::from(end);

    if start_i < 0 {
        start_i += len_i;
    }
    if end_i < 0 {
        end_i += len_i;
    }

    if start_i < 0 {
        start_i = 0;
    }
    if end_i < 0 {
        return None;
    }

    if start_i >= len_i {
        return None;
    }
    if end_i >= len_i {
        end_i = len_i - 1;
    }
    if start_i > end_i {
        return None;
    }

    Some((start_i as usize, end_i as usize))
}

fn object_type_name(object_type: u8) -> Option<&'static [u8]> {
    match object_type {
        HASH_OBJECT_TYPE_TAG => Some(b"hash"),
        LIST_OBJECT_TYPE_TAG => Some(b"list"),
        SET_OBJECT_TYPE_TAG => Some(b"set"),
        ZSET_OBJECT_TYPE_TAG => Some(b"zset"),
        STREAM_OBJECT_TYPE_TAG => Some(b"stream"),
        _ => None,
    }
}
