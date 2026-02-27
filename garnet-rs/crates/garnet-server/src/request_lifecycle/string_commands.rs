use super::*;
use tsavorite::RmwInfo;
use tsavorite::RmwOperationError;
use tsavorite::UpsertOperationError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BitopOperation {
    And,
    Or,
    Xor,
    Not,
    Diff,
    Diff1,
    AndOr,
    One,
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
enum BitfieldSetOutcome {
    Value { raw: u64 },
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct SortOptions {
    by_pattern: Option<Vec<u8>>,
    limit_offset: usize,
    limit_count: Option<usize>,
    get_patterns: Vec<Vec<u8>>,
    desc: bool,
    alpha: bool,
    store_key: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SortElement {
    value: Vec<u8>,
    rank: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ExpireConditionOptions {
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
}

const PF_STRING_PREFIX_SPARSE: &[u8] = b"\x00garnet-pf-v1\x00";
const PF_STRING_PREFIX_DENSE: &[u8] = b"\x00garnet-pf-vD\x00";
const PF_REDIS_HLL_PREFIX: &[u8] = b"HYLL";
const PFDEBUG_REGISTER_COUNT: usize = 16_384;
const PF_REGISTER_INDEX_BITS: u32 = 14;
const PF_REGISTER_INDEX_MASK: u64 = (1u64 << PF_REGISTER_INDEX_BITS) - 1;
const PF_REGISTER_MAX_VALUE: u8 = (64 - PF_REGISTER_INDEX_BITS + 1) as u8;
const PF_MURMUR64A_SEED: u64 = 0xadc8_3b19;
const PF_SPARSE_MAX_BYTES_DEFAULT: usize = 3_000;
const PFDEBUG_HELP_LINES: [&[u8]; 11] = [
    b"PFDEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"ENCODING <key>",
    b"    Return HyperLogLog internal encoding for key.",
    b"TODENSE <key>",
    b"    Force sparse-to-dense representation.",
    b"GETREG <key>",
    b"    Return raw HyperLogLog registers.",
    b"SIMD <ON|OFF>",
    b"    Toggle SIMD mode (no-op in garnet-rs).",
    b"HELP",
    b"    Print this help.",
];

#[derive(Clone, Debug)]
struct PfSetState {
    registers: [u8; PFDEBUG_REGISTER_COUNT],
    encoding: HllEncoding,
    cache_dirty: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum HllEncoding {
    #[default]
    Sparse,
    Dense,
}

impl Default for PfSetState {
    fn default() -> Self {
        Self {
            registers: [0u8; PFDEBUG_REGISTER_COUNT],
            encoding: HllEncoding::Sparse,
            cache_dirty: false,
        }
    }
}

impl RequestProcessor {
    pub(super) fn handle_get(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        crate::debug_sync_point!("request_processor.handle_get.enter");
        require_exact_arity(args, 2, "GET", "GET key")?;

        let key = RedisKey::from(args[1]);
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
                self.record_key_access(&key, false);
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "STRLEN", "STRLEN key")?;

        let key = RedisKey::from(args[1]);
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
                append_integer(
                    response_out,
                    string_value_len_for_keysizes(self, &output) as i64,
                );
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_getrange_like(args, response_out, false)
    }

    pub(super) fn handle_substr(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_getrange_like(args, response_out, true)
    }

    fn handle_getrange_like(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
        substr_alias: bool,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if substr_alias {
            ("SUBSTR", "SUBSTR key start end")
        } else {
            ("GETRANGE", "GETRANGE key start end")
        };
        require_exact_arity(args, 4, command, expected)?;

        let start = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let end = parse_i64_ascii(args[3]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let key = RedisKey::from(args[1]);
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
                if let Some(range) = normalize_string_range(output.len(), start, end) {
                    append_bulk_string(response_out, &output[range.start..=range.end_inclusive]);
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "GETBIT", "GETBIT key offset")?;
        let offset = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        if offset < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        let offset = usize::try_from(offset).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
        let key = RedisKey::from(args[1]);
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "SETBIT", "SETBIT key offset value")?;
        let offset = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        if offset < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        let offset = usize::try_from(offset).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
        let bit_value = parse_i64_ascii(args[3]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let bit_value = match bit_value {
            0 => 0u8,
            1 => 1u8,
            _ => return Err(RequestExecutionError::ValueOutOfRange),
        };
        let key = RedisKey::from(args[1]);
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
        let old_len = value.len();
        let changed = old_bit != i64::from(bit_value) || old_len < required_len;
        if changed {
            if value.len() < required_len {
                value.resize(required_len, 0);
            }
            if bit_value == 1 {
                value[byte_index] |= 1u8 << bit_index;
            } else {
                value[byte_index] &= !(1u8 << bit_index);
            }
            self.upsert_string_value_with_expiration_unix_millis(
                &key,
                &value,
                expiration_unix_millis,
            )?;
            self.track_string_key(&key);
            self.bump_watch_version(&key);
        }
        append_integer(response_out, old_bit);
        Ok(())
    }

    pub(super) fn handle_setrange(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "SETRANGE", "SETRANGE key offset value")?;
        let offset = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        if offset < 0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        let offset = usize::try_from(offset).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
        let new_segment = args[3];
        let key = RedisKey::from(args[1]);
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "BITCOUNT", "BITCOUNT key [start end [BYTE|BIT]]")?;
        if args.len() != 2 && args.len() != 4 && args.len() != 5 {
            return Err(RequestExecutionError::SyntaxError);
        }

        let parsed_range = if args.len() == 2 {
            None
        } else {
            let start = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
            let end = parse_i64_ascii(args[3]).ok_or(RequestExecutionError::ValueNotInteger)?;
            let bit_mode = if args.len() == 5 {
                let mode = args[4];
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
            Some((start, end, bit_mode))
        };

        let key = RedisKey::from(args[1]);
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
        if parsed_range.is_none() {
            let bits = value.iter().map(|byte| i64::from(byte.count_ones())).sum();
            append_integer(response_out, bits);
            return Ok(());
        }

        let Some((start, end, bit_mode)) = parsed_range else {
            return Err(RequestExecutionError::SyntaxError);
        };

        let count = if bit_mode {
            let total_bits = value
                .len()
                .checked_mul(8)
                .ok_or(RequestExecutionError::ValueOutOfRange)?;
            if let Some(range) = normalize_string_range(total_bits, start, end) {
                let mut count = 0i64;
                for bit_index in range.start..=range.end_inclusive {
                    let byte = value[bit_index / 8];
                    let shift = 7usize - (bit_index % 8);
                    count += i64::from((byte >> shift) & 1);
                }
                count
            } else {
                0
            }
        } else if let Some(range) = normalize_string_range(value.len(), start, end) {
            value[range.start..=range.end_inclusive]
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_ranged_arity(
            args,
            3,
            6,
            "BITPOS",
            "BITPOS key bit [start [end [BYTE|BIT]]]",
        )?;
        let key = RedisKey::from(args[1]);
        let bit = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        if bit != 0 && bit != 1 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        let target_bit = bit as u8;

        let mut mode_is_bit = false;
        if args.len() == 6 {
            let mode = args[5];
            if ascii_eq_ignore_case(mode, b"BYTE") {
                mode_is_bit = false;
            } else if ascii_eq_ignore_case(mode, b"BIT") {
                mode_is_bit = true;
            } else {
                return Err(RequestExecutionError::SyntaxError);
            }
        }

        let start = if args.len() >= 4 {
            parse_i64_ascii(args[3]).ok_or(RequestExecutionError::ValueNotInteger)?
        } else {
            0
        };
        let end = if args.len() >= 5 {
            parse_i64_ascii(args[4]).ok_or(RequestExecutionError::ValueNotInteger)?
        } else {
            i64::MAX
        };
        let has_explicit_end = args.len() >= 5;

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

        let end = if has_explicit_end {
            end
        } else if mode_is_bit {
            let total_bits = value
                .len()
                .checked_mul(8)
                .ok_or(RequestExecutionError::ValueOutOfRange)?;
            (total_bits as i64) - 1
        } else {
            (value.len() as i64) - 1
        };

        let result = if mode_is_bit {
            let total_bits = value
                .len()
                .checked_mul(8)
                .ok_or(RequestExecutionError::ValueOutOfRange)?;
            if let Some(range) = normalize_string_range(total_bits, start, end) {
                let mut found = None;
                for bit_index in range.start..=range.end_inclusive {
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
        } else if let Some(range) = normalize_string_range(value.len(), start, end) {
            let mut found = None;
            for byte_index in range.start..=range.end_inclusive {
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 4, "BITOP", "BITOP operation destkey key [key ...]")?;

        let operation = parse_bitop_operation(args[1]).ok_or(RequestExecutionError::SyntaxError)?;
        if operation == BitopOperation::Not && args.len() != 4 {
            append_error(
                response_out,
                b"ERR BITOP NOT must be called with a single source key.",
            );
            return Ok(());
        }
        if matches!(
            operation,
            BitopOperation::Diff | BitopOperation::Diff1 | BitopOperation::AndOr
        ) && args.len() < 5
        {
            let operation_name = std::str::from_utf8(args[1]).unwrap_or("DIFF");
            let error_message = format!(
                "ERR BITOP {} must be called with multiple source keys.",
                operation_name.to_ascii_uppercase()
            );
            append_error(response_out, error_message.as_bytes());
            return Ok(());
        }

        let destination = RedisKey::from(args[2]);
        let source_keys = args[3..].iter().map(|key| key.to_vec()).collect::<Vec<_>>();

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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_bitfield_impl(args, response_out, false)
    }

    pub(super) fn handle_bitfield_ro(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_bitfield_impl(args, response_out, true)
    }

    fn handle_bitfield_impl(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
        read_only: bool,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if read_only {
            (
                "BITFIELD_RO",
                "BITFIELD_RO key GET encoding offset [GET encoding offset ...]",
            )
        } else {
            ("BITFIELD", "BITFIELD key [GET|SET|INCRBY|OVERFLOW ...]")
        };
        ensure_min_arity(args, 2, command, expected)?;

        let key = RedisKey::from(args[1]);
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
        let mut saw_overflow_directive = false;

        while index < args.len() {
            let subcommand = args[index];

            if ascii_eq_ignore_case(subcommand, b"OVERFLOW") {
                if read_only {
                    append_error(
                        response_out,
                        b"ERR BITFIELD_RO only supports the GET subcommand",
                    );
                    return Ok(());
                }
                let mode_index = index
                    .checked_add(1)
                    .ok_or(RequestExecutionError::ValueOutOfRange)?;
                if mode_index >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                let mode = args[mode_index];
                overflow_mode =
                    parse_bitfield_overflow_mode(mode).ok_or(RequestExecutionError::SyntaxError)?;
                saw_overflow_directive = true;
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

                let encoding_token = args[encoding_index];
                let offset_token = args[offset_index];
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
                    append_error(
                        response_out,
                        b"ERR BITFIELD_RO only supports the GET subcommand",
                    );
                    return Ok(());
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

                let encoding_token = args[encoding_index];
                let offset_token = args[offset_index];
                let value_token = args[value_index];
                let encoding = parse_bitfield_encoding(encoding_token)?;
                let offset = parse_bitfield_offset(offset_token, usize::from(encoding.bits))?;
                let set_value =
                    parse_i64_ascii(value_token).ok_or(RequestExecutionError::ValueNotInteger)?;
                let raw = read_unsigned_bits(&value, offset, usize::from(encoding.bits))?;
                let required_bits = offset
                    .checked_add(usize::from(encoding.bits))
                    .ok_or(RequestExecutionError::ValueOutOfRange)?;
                let required_bytes = required_bits
                    .checked_add(7)
                    .ok_or(RequestExecutionError::ValueOutOfRange)?
                    / 8;
                let length_changed = required_bytes > value.len();
                let previous = decode_bitfield_raw(raw, encoding);
                match apply_bitfield_set(set_value, encoding, overflow_mode) {
                    BitfieldSetOutcome::Value { raw: next_raw } => {
                        responses.push(Some(previous));
                        if raw != next_raw || length_changed {
                            write_unsigned_bits(
                                &mut value,
                                offset,
                                usize::from(encoding.bits),
                                next_raw,
                            )?;
                            wrote = true;
                        }
                    }
                    BitfieldSetOutcome::OverflowFail => responses.push(None),
                }
                operation_count += 1;
                index = value_index + 1;
                continue;
            }

            if ascii_eq_ignore_case(subcommand, b"INCRBY") {
                if read_only {
                    append_error(
                        response_out,
                        b"ERR BITFIELD_RO only supports the GET subcommand",
                    );
                    return Ok(());
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

                let encoding_token = args[encoding_index];
                let offset_token = args[offset_index];
                let increment_token = args[increment_index];
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
            if saw_overflow_directive {
                return Err(RequestExecutionError::SyntaxError);
            }
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            3,
            "LCS",
            "LCS key1 key2 [LEN | IDX [MINMATCHLEN min-match-len] [WITHMATCHLEN]]",
        )?;

        let options = parse_lcs_options(args)?;
        let left_key = RedisKey::from(args[1]);
        let right_key = RedisKey::from(args[2]);

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

        let lcs_result = lcs_sequence_and_matches(&left, &right);
        if options.mode == LcsResponseMode::LengthOnly {
            append_integer(response_out, lcs_result.length as i64);
            return Ok(());
        }
        if options.mode == LcsResponseMode::Sequence {
            append_bulk_string(response_out, &lcs_result.sequence);
            return Ok(());
        }

        let mut matches = lcs_result.matches;
        if options.min_match_len > 0 {
            matches.retain(|entry| entry.length >= options.min_match_len);
        }
        append_lcs_idx_response(
            response_out,
            &matches,
            lcs_result.length,
            options.with_match_len,
        );
        Ok(())
    }

    pub(super) fn handle_sort(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_sort_impl(args, response_out, false)
    }

    pub(super) fn handle_sort_ro(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_sort_impl(args, response_out, true)
    }

    fn handle_sort_impl(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
        read_only: bool,
    ) -> Result<(), RequestExecutionError> {
        let command = if read_only { "SORT_RO" } else { "SORT" };
        ensure_min_arity(
            args,
            2,
            command,
            "SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA] [STORE destination]",
        )?;

        let options = parse_sort_options(args, read_only)?;
        let source_key = RedisKey::from(args[1]);
        let mut elements = load_sort_elements(self, &source_key)?;

        let by_pattern = options.by_pattern.as_deref();
        let should_sort = match by_pattern {
            Some(pattern) => pattern.contains(&b'*'),
            None => true,
        };
        if should_sort {
            let mut sortable: Vec<(SortElement, Vec<u8>)> = Vec::with_capacity(elements.len());
            for (rank, element) in elements.into_iter().enumerate() {
                let weight = if let Some(pattern) = by_pattern {
                    resolve_sort_lookup_value(self, pattern, &element)?.unwrap_or_default()
                } else {
                    element.clone()
                };
                sortable.push((
                    SortElement {
                        value: element,
                        rank,
                    },
                    weight,
                ));
            }

            if options.alpha {
                sortable.sort_by(
                    |(left_element, left_weight), (right_element, right_weight)| {
                        let mut order = left_weight.cmp(right_weight);
                        if options.desc {
                            order = order.reverse();
                        }
                        if order == core::cmp::Ordering::Equal {
                            return left_element.rank.cmp(&right_element.rank);
                        }
                        order
                    },
                );
            } else {
                sortable.sort_by(
                    |(left_element, left_weight), (right_element, right_weight)| {
                        let left_score = if left_weight.is_empty() {
                            Ok(0.0)
                        } else {
                            parse_f64_ascii(left_weight).ok_or(RequestExecutionError::ValueNotFloat)
                        };
                        let right_score = if right_weight.is_empty() {
                            Ok(0.0)
                        } else {
                            parse_f64_ascii(right_weight)
                                .ok_or(RequestExecutionError::ValueNotFloat)
                        };
                        let mut order = match (left_score, right_score) {
                            (Ok(left), Ok(right)) => left
                                .partial_cmp(&right)
                                .unwrap_or(core::cmp::Ordering::Equal),
                            (Err(_), _) => core::cmp::Ordering::Less,
                            (_, Err(_)) => core::cmp::Ordering::Greater,
                        };
                        if options.desc {
                            order = order.reverse();
                        }
                        if order == core::cmp::Ordering::Equal {
                            return left_element.rank.cmp(&right_element.rank);
                        }
                        order
                    },
                );
                if sortable.iter().any(|(_, weight)| {
                    !weight.is_empty() && parse_f64_ascii(weight.as_slice()).is_none()
                }) {
                    return Err(RequestExecutionError::ValueNotFloat);
                }
            }

            elements = sortable
                .into_iter()
                .map(|(element, _)| element.value)
                .collect();
        }

        let selected = apply_sort_limit(&elements, options.limit_offset, options.limit_count);

        if let Some(store_key) = options.store_key.as_deref() {
            let mut stored = Vec::new();
            if options.get_patterns.is_empty() {
                stored.extend(selected.iter().cloned());
            } else {
                for element in selected {
                    for pattern in &options.get_patterns {
                        let resolved = resolve_sort_get_value(self, pattern, element)?;
                        stored.push(resolved.unwrap_or_default());
                    }
                }
            }

            self.delete_string_key_for_migration(store_key)?;
            let _ = self.object_delete(store_key)?;
            if !stored.is_empty() {
                self.save_list_object(store_key, &stored)?;
            }
            append_integer(response_out, stored.len() as i64);
            return Ok(());
        }

        let response_count = if options.get_patterns.is_empty() {
            selected.len()
        } else {
            selected
                .len()
                .checked_mul(options.get_patterns.len())
                .ok_or(RequestExecutionError::ValueOutOfRange)?
        };
        append_resp_array_len(response_out, response_count);
        if options.get_patterns.is_empty() {
            for element in selected {
                append_bulk_string(response_out, element);
            }
        } else {
            for element in selected {
                for pattern in &options.get_patterns {
                    match resolve_sort_get_value(self, pattern, element)? {
                        Some(value) => append_bulk_string(response_out, &value),
                        None => append_null_bulk_string(response_out),
                    }
                }
            }
        }
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
            upsert_info
                .user_data
                .insert(UPSERT_USER_DATA_HAS_EXPIRATION);
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "APPEND", "APPEND key value")?;

        let key = RedisKey::from(args[1]);
        let append_value = args[2];
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
            upsert_info
                .user_data
                .insert(UPSERT_USER_DATA_HAS_EXPIRATION);
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let action = parse_getex_action(args)?;
        let key = RedisKey::from(args[1]);
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
                let shard_index = self.string_store_shard_index_for_key(&key);
                self.set_string_expiration_metadata_in_shard(&key, shard_index, Some(expiration));
                if !self.rewrite_existing_value_expiration(
                    &key,
                    Some(expiration.unix_millis.as_u64()),
                )? {
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "INCRBYFLOAT", "INCRBYFLOAT key increment")?;

        let increment = parse_f64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotFloat)?;
        let key = RedisKey::from(args[1]);
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
            upsert_info
                .user_data
                .insert(UPSERT_USER_DATA_HAS_EXPIRATION);
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        crate::debug_sync_point!("request_processor.handle_set.enter");
        ensure_min_arity(args, 3, "SET", "SET key value")?;

        let key = RedisKey::from(args[1]);
        let value = args[2];
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

        let preserved_expiration = if options.keep_ttl {
            self.expiration_unix_millis_for_key(&key)
                .map(|unix_millis| {
                    let deadline =
                        instant_from_unix_millis(unix_millis).unwrap_or_else(Instant::now);
                    ExpirationMetadata {
                        deadline,
                        unix_millis: TimestampMillis::new(unix_millis),
                    }
                })
        } else {
            None
        };
        let effective_expiration = options.expiration.or(preserved_expiration);

        let mut output = Vec::new();
        let mut info = UpsertInfo::default();
        let normalized_value = canonicalize_oversized_hyll_value(value);
        let stored_value = encode_stored_value(
            normalized_value,
            effective_expiration.map(|e| e.unix_millis.as_u64()),
        );
        if effective_expiration.is_some() {
            info.user_data.insert(UPSERT_USER_DATA_HAS_EXPIRATION);
        }
        if let Err(error) = session.upsert(&key, &stored_value, &mut output, &mut info) {
            if let Some(fallback_user_value) =
                hll_record_too_large_fallback_value(normalized_value, &error)
            {
                output.clear();
                let fallback_stored_value = encode_stored_value(
                    fallback_user_value,
                    effective_expiration.map(|expiration| expiration.unix_millis.as_u64()),
                );
                session
                    .upsert(&key, &fallback_stored_value, &mut output, &mut info)
                    .map_err(map_upsert_error)?;
            } else {
                return Err(map_upsert_error(error));
            }
        }
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
        self.set_string_expiration_metadata_in_shard(&key, shard_index, effective_expiration);
        self.track_string_key_in_shard(&key, shard_index);
        self.bump_watch_version(&key);
        self.record_key_access(&key, true);

        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_setnx(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "SETNX", "SETNX key value")?;
        let translated: [&[u8]; 4] = [b"SET", args[1], args[2], b"NX"];
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "SETEX", "SETEX key seconds value")?;
        let translated: [&[u8]; 5] = [b"SET", args[1], args[3], b"EX", args[2]];
        self.handle_set(&translated, response_out)
    }

    pub(super) fn handle_psetex(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "PSETEX", "PSETEX key milliseconds value")?;
        let translated: [&[u8]; 5] = [b"SET", args[1], args[3], b"PX", args[2]];
        self.handle_set(&translated, response_out)
    }

    pub(super) fn handle_getset(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "GETSET", "GETSET key value")?;

        let mut previous_value_response = Vec::new();
        let get_args: [&[u8]; 2] = [b"GET", args[1]];
        self.handle_get(&get_args, &mut previous_value_response)?;

        let set_args: [&[u8]; 3] = [b"SET", args[1], args[2]];
        let mut set_response = Vec::new();
        self.handle_set(&set_args, &mut set_response)?;

        response_out.extend_from_slice(&previous_value_response);
        Ok(())
    }

    pub(super) fn handle_getdel(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "GETDEL", "GETDEL key")?;

        let get_args: [&[u8]; 2] = [b"GET", args[1]];
        let mut previous_value_response = Vec::new();
        self.handle_get(&get_args, &mut previous_value_response)?;
        if previous_value_response.as_slice() == b"$-1\r\n" {
            response_out.extend_from_slice(&previous_value_response);
            return Ok(());
        }

        let mut del_response = Vec::new();
        let del_args: [&[u8]; 2] = [b"DEL", args[1]];
        self.handle_del(&del_args, &mut del_response)?;
        response_out.extend_from_slice(&previous_value_response);
        Ok(())
    }

    pub(super) fn handle_del(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "DEL", "DEL key [key ...]")?;

        let keys: Vec<Vec<u8>> = args[1..].iter().map(|arg| arg.to_vec()).collect();

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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "TOUCH", "TOUCH key [key ...]")?;

        let mut touched = 0i64;
        for arg in &args[1..] {
            let key = arg.to_vec();
            self.expire_key_if_needed(&key)?;
            if self.key_exists_any(&key)? {
                touched += 1;
                self.record_key_access(&key, true);
            }
        }
        append_integer(response_out, touched);
        Ok(())
    }

    pub(super) fn handle_unlink(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "UNLINK", "UNLINK key [key ...]")?;
        self.handle_del(args, response_out)
    }

    pub(super) fn handle_rename(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_rename_internal(args, false, response_out)
    }

    pub(super) fn handle_renamenx(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_rename_internal(args, true, response_out)
    }

    fn handle_rename_internal(
        &self,
        args: &[&[u8]],
        only_if_absent: bool,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if only_if_absent {
            ("RENAMENX", "RENAMENX key newkey")
        } else {
            ("RENAME", "RENAME key newkey")
        };
        require_exact_arity(args, 3, command, expected)?;
        let source = RedisKey::from(args[1]);
        let destination = RedisKey::from(args[2]);

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

        source_entry.key = destination.clone().into();
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            3,
            "COPY",
            "COPY source destination [DB destination-db] [REPLACE]",
        )?;
        let source = RedisKey::from(args[1]);
        let destination = RedisKey::from(args[2]);

        let mut replace = false;
        let mut destination_db = 0u64;
        let mut index = 3usize;
        while index < args.len() {
            let option = args[index];
            if ascii_eq_ignore_case(option, b"REPLACE") {
                replace = true;
                index += 1;
                continue;
            }
            if ascii_eq_ignore_case(option, b"DB") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                let db_value = args[index + 1];
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

        source_entry.key = destination.into();
        self.import_migration_entry(&source_entry)?;
        append_integer(response_out, 1);
        Ok(())
    }

    pub(super) fn handle_incr_decr(
        &self,
        args: &[&[u8]],
        delta: i64,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if delta > 0 {
            ("INCR", "INCR key")
        } else {
            ("DECR", "DECR key")
        };
        require_exact_arity(args, 2, command, expected)?;

        let key = RedisKey::from(args[1]);
        self.apply_incr_decr_delta(&key, delta, response_out)
    }

    pub(super) fn handle_incrby_decrby(
        &self,
        args: &[&[u8]],
        decrement: bool,
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if decrement {
            ("DECRBY", "DECRBY key decrement")
        } else {
            ("INCRBY", "INCRBY key increment")
        };
        require_exact_arity(args, 3, command, expected)?;

        let amount = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let delta = if decrement {
            amount
                .checked_neg()
                .ok_or(RequestExecutionError::ValueNotInteger)?
        } else {
            amount
        };
        let key = RedisKey::from(args[1]);
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "EXISTS", "EXISTS key [key ...]")?;

        let mut exists = 0i64;
        for arg in &args[1..] {
            let key = arg.to_vec();
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "TYPE", "TYPE key")?;

        let key = RedisKey::from(args[1]);
        self.expire_key_if_needed(&key)?;
        if self.key_exists(&key)? {
            append_simple_string(response_out, b"string");
            return Ok(());
        }

        let value_type = match self.object_read(&key)? {
            Some(object) => object_type_name(object.object_type),
            None => b"none",
        };
        append_simple_string(response_out, value_type);
        Ok(())
    }

    pub(super) fn handle_mget(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "MGET", "MGET key [key ...]")?;

        response_out.push(b'*');
        response_out.extend_from_slice((args.len() - 1).to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for arg in &args[1..] {
            let key = arg.to_vec();
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_paired_arity_after(args, 3, 1, "MSET", "MSET key value [key value ...]")?;

        for pair in args[1..].chunks_exact(2) {
            let key = pair[0].to_vec();
            let value = pair[1].to_vec();
            self.mset_single_pair(&key, &value)?;
        }

        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_msetnx(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_paired_arity_after(args, 3, 1, "MSETNX", "MSETNX key value [key value ...]")?;

        let key_value_pairs: Vec<(Vec<u8>, Vec<u8>)> = args[1..]
            .chunks_exact(2)
            .map(|pair| (pair[0].to_vec(), pair[1].to_vec()))
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "PFADD", "PFADD key element [element ...]")?;
        let key = RedisKey::from(args[1]);
        let existing = load_pf_set_for_key(self, &key)?;
        let was_missing = existing.is_none();
        let mut state = existing.unwrap_or_default();
        let mut register_changed = false;
        for element in &args[2..] {
            if pf_set_register_for_member(&mut state.registers, element) {
                register_changed = true;
            }
        }
        let changed = if args.len() == 2 {
            if was_missing { 1 } else { 0 }
        } else if register_changed {
            1
        } else {
            0
        };
        if changed == 1 {
            state.cache_dirty = args.len() > 2;
            if state.encoding != HllEncoding::Dense
                && pf_should_promote_to_dense(
                    pf_non_zero_register_count(&state.registers),
                    pf_sparse_max_bytes(self),
                )
            {
                state.encoding = HllEncoding::Dense;
            }
            self.upsert_string_value_for_migration(&key, &encode_pf_set(&state), None)?;
        }
        append_integer(response_out, changed);
        Ok(())
    }

    pub(super) fn handle_pfcount(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "PFCOUNT", "PFCOUNT key [key ...]")?;
        let mut union_registers = [0u8; PFDEBUG_REGISTER_COUNT];
        let mut single_key_state_update = None::<(Vec<u8>, PfSetState)>;
        for key_arg in &args[1..] {
            let key = key_arg.to_vec();
            if let Some(state) = load_pf_set_for_key(self, &key)? {
                pf_merge_registers(&mut union_registers, &state.registers);
                if args.len() == 2 && state.cache_dirty {
                    let mut normalized = state;
                    normalized.cache_dirty = false;
                    single_key_state_update = Some((key, normalized));
                }
            }
        }
        if let Some((key, state)) = single_key_state_update {
            self.upsert_string_value_for_migration(&key, &encode_pf_set(&state), None)?;
        }
        append_integer(response_out, pf_estimate_cardinality(&union_registers));
        Ok(())
    }

    pub(super) fn handle_pfmerge(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            2,
            "PFMERGE",
            "PFMERGE destkey sourcekey [sourcekey ...]",
        )?;
        let destination = RedisKey::from(args[1]);
        let mut merged = load_pf_set_for_key(self, &destination)?.unwrap_or_default();
        for source_arg in &args[2..] {
            let source = source_arg.to_vec();
            if let Some(set) = load_pf_set_for_key(self, &source)? {
                pf_merge_registers(&mut merged.registers, &set.registers);
            }
        }
        merged.cache_dirty = true;
        if merged.encoding != HllEncoding::Dense
            && pf_should_promote_to_dense(
                pf_non_zero_register_count(&merged.registers),
                pf_sparse_max_bytes(self),
            )
        {
            merged.encoding = HllEncoding::Dense;
        }
        self.upsert_string_value_for_migration(&destination, &encode_pf_set(&merged), None)?;
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_pfdebug(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            2,
            "PFDEBUG",
            "PFDEBUG <ENCODING|TODENSE|GETREG|SIMD|HELP> [key]",
        )?;
        let subcommand = args[1];
        if ascii_eq_ignore_case(subcommand, b"HELP") {
            require_exact_arity(args, 2, "PFDEBUG", "PFDEBUG HELP")?;
            append_bulk_array(response_out, &PFDEBUG_HELP_LINES);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"ENCODING") {
            require_exact_arity(args, 3, "PFDEBUG", "PFDEBUG ENCODING key")?;
            let key = RedisKey::from(args[2]);
            let encoding = if load_pf_set_for_key(self, &key)?
                .map(|state| state.encoding == HllEncoding::Dense)
                .unwrap_or(false)
            {
                b"dense".as_slice()
            } else {
                b"sparse".as_slice()
            };
            append_bulk_string(response_out, encoding);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"TODENSE") {
            require_exact_arity(args, 3, "PFDEBUG", "PFDEBUG TODENSE key")?;
            let key = RedisKey::from(args[2]);
            let mut state = load_pf_set_for_key(self, &key)?.unwrap_or_default();
            state.encoding = HllEncoding::Dense;
            self.upsert_string_value_for_migration(&key, &encode_pf_set(&state), None)?;
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"GETREG") {
            require_exact_arity(args, 3, "PFDEBUG", "PFDEBUG GETREG key")?;
            let key = RedisKey::from(args[2]);
            let state = load_pf_set_for_key(self, &key)?.unwrap_or_default();
            response_out.push(b'*');
            response_out.extend_from_slice(PFDEBUG_REGISTER_COUNT.to_string().as_bytes());
            response_out.extend_from_slice(b"\r\n");
            for register in state.registers {
                append_integer(response_out, i64::from(register));
            }
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"SIMD") {
            require_exact_arity(args, 3, "PFDEBUG", "PFDEBUG SIMD <ON|OFF>")?;
            if ascii_eq_ignore_case(args[2], b"ON") || ascii_eq_ignore_case(args[2], b"OFF") {
                append_simple_string(response_out, b"OK");
                return Ok(());
            }
            return Err(RequestExecutionError::SyntaxError);
        }
        Err(RequestExecutionError::UnknownCommand)
    }

    pub(super) fn handle_pfselftest(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 1, "PFSELFTEST", "PFSELFTEST")?;
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
        let normalized_value = canonicalize_oversized_hyll_value(value);
        let stored_value = encode_stored_value(normalized_value, None);
        if let Err(error) = session.upsert(&key_vec, &stored_value, &mut output, &mut info) {
            if let Some(fallback_user_value) =
                hll_record_too_large_fallback_value(normalized_value, &error)
            {
                output.clear();
                let fallback_stored_value = encode_stored_value(fallback_user_value, None);
                session
                    .upsert(&key_vec, &fallback_stored_value, &mut output, &mut info)
                    .map_err(map_upsert_error)?;
            } else {
                return Err(map_upsert_error(error));
            }
        }
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_expire_like(args, response_out, false)
    }

    pub(super) fn handle_pexpire(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_expire_like(args, response_out, true)
    }

    pub(super) fn handle_expireat(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_expireat_like(args, response_out, false)
    }

    pub(super) fn handle_pexpireat(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_expireat_like(args, response_out, true)
    }

    fn handle_expire_like(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
        milliseconds: bool,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if milliseconds {
            ("PEXPIRE", "PEXPIRE key milliseconds [NX|XX|GT|LT]")
        } else {
            ("EXPIRE", "EXPIRE key seconds [NX|XX|GT|LT]")
        };
        ensure_min_arity(args, 3, command, expected)?;

        let overflow_error = if milliseconds {
            RequestExecutionError::InvalidPExpireCommandExpireTime
        } else {
            RequestExecutionError::InvalidExpireCommandExpireTime
        };
        let amount = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let key = RedisKey::from(args[1]);

        self.expire_key_if_needed(&key)?;
        let string_exists = self.key_exists(&key)?;
        let object_exists = self.object_key_exists(&key)?;
        if !string_exists && !object_exists {
            append_integer(response_out, 0);
            return Ok(());
        }

        let Some(options) = parse_expire_condition_options(args, 3, response_out) else {
            return Ok(());
        };

        let now_unix_millis = i64::try_from(
            current_unix_time_millis().ok_or(RequestExecutionError::ValueNotInteger)?,
        )
        .map_err(|_| overflow_error)?;
        let target_unix_millis = compute_relative_expire_target_unix_millis(
            amount,
            milliseconds,
            now_unix_millis,
            overflow_error,
        )?;
        let current_expiration_unix_millis = self.expiration_unix_millis_for_key(&key);
        if !should_apply_expire_condition(
            options,
            current_expiration_unix_millis,
            i128::from(target_unix_millis),
        ) {
            append_integer(response_out, 0);
            return Ok(());
        }

        if target_unix_millis <= now_unix_millis {
            return self.expire_existing_key_immediately(
                &key,
                string_exists,
                object_exists,
                response_out,
            );
        }

        let unix_millis = u64::try_from(target_unix_millis).map_err(|_| overflow_error)?;
        let deadline = instant_from_unix_millis(unix_millis).ok_or(overflow_error)?;
        let shard_index = self.string_store_shard_index_for_key(&key);
        self.set_string_expiration_metadata_in_shard(
            &key,
            shard_index,
            Some(ExpirationMetadata {
                deadline,
                unix_millis: TimestampMillis::new(unix_millis),
            }),
        );
        if string_exists && !self.rewrite_existing_value_expiration(&key, Some(unix_millis))? {
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
        milliseconds: bool,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if milliseconds {
            (
                "PEXPIREAT",
                "PEXPIREAT key milliseconds-unix-time [NX|XX|GT|LT]",
            )
        } else {
            ("EXPIREAT", "EXPIREAT key seconds-unix-time [NX|XX|GT|LT]")
        };
        ensure_min_arity(args, 3, command, expected)?;

        let amount = parse_i64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotInteger)?;
        let key = RedisKey::from(args[1]);

        self.expire_key_if_needed(&key)?;
        let string_exists = self.key_exists(&key)?;
        let object_exists = self.object_key_exists(&key)?;
        if !string_exists && !object_exists {
            append_integer(response_out, 0);
            return Ok(());
        }

        let Some(options) = parse_expire_condition_options(args, 3, response_out) else {
            return Ok(());
        };
        let target_unix_millis = compute_absolute_expire_target_unix_millis(amount, milliseconds)?;
        let now_unix_millis =
            i128::from(current_unix_time_millis().ok_or(RequestExecutionError::ValueNotInteger)?);
        let current_expiration_unix_millis = self.expiration_unix_millis_for_key(&key);
        if !should_apply_expire_condition(
            options,
            current_expiration_unix_millis,
            target_unix_millis,
        ) {
            append_integer(response_out, 0);
            return Ok(());
        }

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
        let shard_index = self.string_store_shard_index_for_key(&key);
        self.set_string_expiration_metadata_in_shard(
            &key,
            shard_index,
            Some(ExpirationMetadata {
                deadline,
                unix_millis: TimestampMillis::new(unix_millis),
            }),
        );
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_ttl_like(args, response_out, false)
    }

    pub(super) fn handle_pttl(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_ttl_like(args, response_out, true)
    }

    pub(super) fn handle_expiretime(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_expiretime_like(args, response_out, false)
    }

    pub(super) fn handle_pexpiretime(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_expiretime_like(args, response_out, true)
    }

    fn handle_ttl_like(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
        milliseconds: bool,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if milliseconds {
            ("PTTL", "PTTL key")
        } else {
            ("TTL", "TTL key")
        };
        require_exact_arity(args, 2, command, expected)?;

        let key = RedisKey::from(args[1]);
        self.expire_key_if_needed(&key)?;

        if !self.key_exists_any(&key)? {
            append_integer(response_out, -2);
            return Ok(());
        }

        if let Some(expiration_unix_millis) = self.expiration_unix_millis_for_key(&key) {
            let now_unix_millis =
                current_unix_time_millis().ok_or(RequestExecutionError::ValueNotInteger)?;
            if expiration_unix_millis <= now_unix_millis {
                self.expire_key_if_needed(&key)?;
                append_integer(response_out, -2);
                return Ok(());
            }

            let remaining_millis = expiration_unix_millis - now_unix_millis;
            let ttl = if milliseconds {
                remaining_millis.min(i64::MAX as u64) as i64
            } else {
                let rounded = remaining_millis.saturating_add(500) / 1000;
                rounded.min(i64::MAX as u64) as i64
            };
            append_integer(response_out, ttl);
            return Ok(());
        }

        match self.string_expiration_deadline(&key) {
            None => {
                append_integer(response_out, -1);
            }
            Some(deadline) => {
                let now = Instant::now();
                if deadline <= now {
                    self.expire_key_if_needed(&key)?;
                    append_integer(response_out, -2);
                    return Ok(());
                }

                let remaining_millis = deadline.duration_since(now).as_millis();
                let ttl = if milliseconds {
                    remaining_millis.min(i64::MAX as u128) as i64
                } else {
                    ((remaining_millis.saturating_add(500)) / 1000).min(i64::MAX as u128) as i64
                };
                append_integer(response_out, ttl);
            }
        }
        Ok(())
    }

    fn handle_expiretime_like(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
        milliseconds: bool,
    ) -> Result<(), RequestExecutionError> {
        let (command, expected) = if milliseconds {
            ("PEXPIRETIME", "PEXPIRETIME key")
        } else {
            ("EXPIRETIME", "EXPIRETIME key")
        };
        require_exact_arity(args, 2, command, expected)?;

        let key = RedisKey::from(args[1]);
        self.expire_key_if_needed(&key)?;

        if !self.key_exists_any(&key)? {
            append_integer(response_out, -2);
            return Ok(());
        }

        if let Some(expiration_unix_millis) = self.expiration_unix_millis_for_key(&key) {
            let value = if milliseconds {
                expiration_unix_millis.min(i64::MAX as u64) as i64
            } else {
                (expiration_unix_millis / 1000).min(i64::MAX as u64) as i64
            };
            append_integer(response_out, value);
            return Ok(());
        }

        match self.string_expiration_deadline(&key) {
            None => {
                append_integer(response_out, -1);
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
            }
        }
        Ok(())
    }

    pub(super) fn handle_persist(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "PERSIST", "PERSIST key")?;

        let key = RedisKey::from(args[1]);
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
            let removed = expirations.remove(key.as_slice()).is_some();
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

fn parse_getex_action(args: &[&[u8]]) -> Result<GetExAction, RequestExecutionError> {
    ensure_min_arity(
        args,
        2,
        "GETEX",
        "GETEX key [EX seconds|PX milliseconds|EXAT unix-time-seconds|PXAT unix-time-milliseconds|PERSIST]",
    )?;
    if args.len() == 2 {
        return Ok(GetExAction::KeepTtl);
    }
    if args.len() == 3 {
        let option = args[2];
        if ascii_eq_ignore_case(option, b"PERSIST") {
            return Ok(GetExAction::Persist);
        }
        return Err(RequestExecutionError::SyntaxError);
    }
    require_exact_arity(
        args,
        4,
        "GETEX",
        "GETEX key [EX seconds|PX milliseconds|EXAT unix-time-seconds|PXAT unix-time-milliseconds|PERSIST]",
    )?;

    let option = args[2];
    let value = args[3];

    if ascii_eq_ignore_case(option, b"EX") || ascii_eq_ignore_case(option, b"PX") {
        let amount = parse_i64_ascii(value).ok_or(RequestExecutionError::ValueNotInteger)?;
        if amount <= 0 {
            return Err(RequestExecutionError::InvalidGetExExpireTime);
        }
        let expiration = expiration_metadata_from_relative_expire_amount(
            amount,
            ascii_eq_ignore_case(option, b"PX"),
        )
        .ok_or(RequestExecutionError::InvalidGetExExpireTime)?;
        return Ok(GetExAction::SetExpiration(expiration));
    }

    if ascii_eq_ignore_case(option, b"EXAT") || ascii_eq_ignore_case(option, b"PXAT") {
        let amount = parse_i64_ascii(value).ok_or(RequestExecutionError::ValueNotInteger)?;
        if amount <= 0 {
            return Err(RequestExecutionError::InvalidGetExExpireTime);
        }
        let unix_millis_i64 = if ascii_eq_ignore_case(option, b"EXAT") {
            amount
                .checked_mul(1000)
                .ok_or(RequestExecutionError::InvalidGetExExpireTime)?
        } else {
            amount
        };
        let unix_millis = u64::try_from(unix_millis_i64)
            .map_err(|_| RequestExecutionError::InvalidGetExExpireTime)?;
        let now_unix_millis =
            current_unix_time_millis().ok_or(RequestExecutionError::InvalidGetExExpireTime)?;
        if unix_millis <= now_unix_millis {
            return Ok(GetExAction::DeleteNow);
        }
        let deadline = instant_from_unix_millis(unix_millis)
            .ok_or(RequestExecutionError::InvalidGetExExpireTime)?;
        return Ok(GetExAction::SetExpiration(ExpirationMetadata {
            deadline,
            unix_millis: TimestampMillis::new(unix_millis),
        }));
    }

    Err(RequestExecutionError::SyntaxError)
}

fn parse_expire_condition_options(
    args: &[&[u8]],
    option_start: usize,
    response_out: &mut Vec<u8>,
) -> Option<ExpireConditionOptions> {
    let mut options = ExpireConditionOptions::default();
    for option in args.iter().skip(option_start) {
        if ascii_eq_ignore_case(option, b"NX") {
            options.nx = true;
        } else if ascii_eq_ignore_case(option, b"XX") {
            options.xx = true;
        } else if ascii_eq_ignore_case(option, b"GT") {
            options.gt = true;
        } else if ascii_eq_ignore_case(option, b"LT") {
            options.lt = true;
        } else {
            let option_text = String::from_utf8_lossy(option);
            let message = format!("ERR Unsupported option {option_text}");
            append_error(response_out, message.as_bytes());
            return None;
        }
    }

    if options.nx && (options.xx || options.gt || options.lt) {
        append_error(
            response_out,
            b"ERR NX and XX, GT or LT options at the same time are not compatible",
        );
        return None;
    }
    if options.gt && options.lt {
        append_error(
            response_out,
            b"ERR GT and LT options at the same time are not compatible",
        );
        return None;
    }
    Some(options)
}

fn should_apply_expire_condition(
    options: ExpireConditionOptions,
    current_expiration_unix_millis: Option<u64>,
    target_unix_millis: i128,
) -> bool {
    if options.nx && current_expiration_unix_millis.is_some() {
        return false;
    }
    if options.xx && current_expiration_unix_millis.is_none() {
        return false;
    }
    if options.gt {
        let Some(current) = current_expiration_unix_millis else {
            return false;
        };
        if target_unix_millis <= i128::from(current) {
            return false;
        }
    }
    if options.lt {
        if let Some(current) = current_expiration_unix_millis {
            if target_unix_millis >= i128::from(current) {
                return false;
            }
        }
    }
    true
}

fn compute_relative_expire_target_unix_millis(
    amount: i64,
    milliseconds: bool,
    now_unix_millis: i64,
    overflow_error: RequestExecutionError,
) -> Result<i64, RequestExecutionError> {
    let delta = if milliseconds {
        amount
    } else {
        amount.checked_mul(1000).ok_or(overflow_error)?
    };
    now_unix_millis.checked_add(delta).ok_or(overflow_error)
}

fn compute_absolute_expire_target_unix_millis(
    amount: i64,
    milliseconds: bool,
) -> Result<i128, RequestExecutionError> {
    if milliseconds {
        Ok(i128::from(amount))
    } else {
        i128::from(amount)
            .checked_mul(1000)
            .ok_or(RequestExecutionError::ValueNotInteger)
    }
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
    if ascii_eq_ignore_case(token, b"DIFF") {
        return Some(BitopOperation::Diff);
    }
    if ascii_eq_ignore_case(token, b"DIFF1") {
        return Some(BitopOperation::Diff1);
    }
    if ascii_eq_ignore_case(token, b"ANDOR") {
        return Some(BitopOperation::AndOr);
    }
    if ascii_eq_ignore_case(token, b"ONE") {
        return Some(BitopOperation::One);
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

struct BitfieldBounds {
    min: i128,
    max: i128,
}

fn bitfield_bounds(encoding: BitfieldEncoding) -> BitfieldBounds {
    let bits = usize::from(encoding.bits);
    match encoding.signedness {
        BitfieldSignedness::Unsigned => BitfieldBounds {
            min: 0,
            max: (1i128 << bits) - 1,
        },
        BitfieldSignedness::Signed => {
            if bits == 64 {
                BitfieldBounds {
                    min: i64::MIN as i128,
                    max: i64::MAX as i128,
                }
            } else {
                BitfieldBounds {
                    min: -(1i128 << (bits - 1)),
                    max: (1i128 << (bits - 1)) - 1,
                }
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
    let bounds = bitfield_bounds(encoding);
    if target >= bounds.min && target <= bounds.max {
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
            let saturated = if target < bounds.min {
                bounds.min
            } else {
                bounds.max
            } as i64;
            Ok(BitfieldIncrOutcome::Value {
                raw: encode_bitfield_value(saturated, encoding),
                value: saturated,
            })
        }
        BitfieldOverflowMode::Fail => Ok(BitfieldIncrOutcome::OverflowFail),
    }
}

fn apply_bitfield_set(
    value: i64,
    encoding: BitfieldEncoding,
    overflow_mode: BitfieldOverflowMode,
) -> BitfieldSetOutcome {
    let target = i128::from(value);
    let bounds = bitfield_bounds(encoding);
    if target >= bounds.min && target <= bounds.max {
        return BitfieldSetOutcome::Value {
            raw: encode_bitfield_value(value, encoding),
        };
    }

    match overflow_mode {
        BitfieldOverflowMode::Wrap => BitfieldSetOutcome::Value {
            raw: encode_bitfield_value(value, encoding),
        },
        BitfieldOverflowMode::Sat => {
            let saturated = if target < bounds.min {
                bounds.min
            } else {
                bounds.max
            } as i64;
            BitfieldSetOutcome::Value {
                raw: encode_bitfield_value(saturated, encoding),
            }
        }
        BitfieldOverflowMode::Fail => BitfieldSetOutcome::OverflowFail,
    }
}

fn parse_lcs_options(args: &[&[u8]]) -> Result<LcsOptions, RequestExecutionError> {
    let mut mode = LcsResponseMode::Sequence;
    let mut min_match_len = 0usize;
    let mut with_match_len = false;
    let mut index = 3usize;

    while index < args.len() {
        let token = args[index];
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
            let min_token = args[value_index];
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

struct LcsComputation {
    length: usize,
    sequence: Vec<u8>,
    matches: Vec<LcsMatchSegment>,
}

fn lcs_sequence_and_matches(left: &[u8], right: &[u8]) -> LcsComputation {
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

    LcsComputation {
        length: dp[0][0],
        sequence,
        matches,
    }
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

fn parse_sort_options(
    args: &[&[u8]],
    read_only: bool,
) -> Result<SortOptions, RequestExecutionError> {
    let mut by_pattern = None;
    let mut limit_offset = 0usize;
    let mut limit_count = None;
    let mut get_patterns = Vec::new();
    let mut desc = false;
    let mut alpha = false;
    let mut store_key = None;
    let mut index = 2usize;

    while index < args.len() {
        let token = args[index];
        if ascii_eq_ignore_case(token, b"BY") {
            let value_index = index
                .checked_add(1)
                .ok_or(RequestExecutionError::ValueOutOfRange)?;
            if value_index >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            by_pattern = Some(args[value_index].to_vec());
            index = value_index + 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"LIMIT") {
            let offset_index = index
                .checked_add(1)
                .ok_or(RequestExecutionError::ValueOutOfRange)?;
            let count_index = index
                .checked_add(2)
                .ok_or(RequestExecutionError::ValueOutOfRange)?;
            if count_index >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            let offset_token = args[offset_index];
            let count_token = args[count_index];
            let offset =
                parse_i64_ascii(offset_token).ok_or(RequestExecutionError::ValueNotInteger)?;
            let count =
                parse_i64_ascii(count_token).ok_or(RequestExecutionError::ValueNotInteger)?;
            if offset < 0 {
                return Err(RequestExecutionError::ValueOutOfRange);
            }
            limit_offset =
                usize::try_from(offset).map_err(|_| RequestExecutionError::ValueOutOfRange)?;
            if count < 0 {
                limit_count = None;
            } else {
                limit_count = Some(
                    usize::try_from(count).map_err(|_| RequestExecutionError::ValueOutOfRange)?,
                );
            }
            index = count_index + 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"GET") {
            let pattern_index = index
                .checked_add(1)
                .ok_or(RequestExecutionError::ValueOutOfRange)?;
            if pattern_index >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            get_patterns.push(args[pattern_index].to_vec());
            index = pattern_index + 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"ASC") {
            desc = false;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"DESC") {
            desc = true;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"ALPHA") {
            alpha = true;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"STORE") {
            if read_only {
                return Err(RequestExecutionError::SyntaxError);
            }
            let destination_index = index
                .checked_add(1)
                .ok_or(RequestExecutionError::ValueOutOfRange)?;
            if destination_index >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            store_key = Some(args[destination_index].to_vec());
            index = destination_index + 1;
            continue;
        }
        return Err(RequestExecutionError::SyntaxError);
    }

    Ok(SortOptions {
        by_pattern,
        limit_offset,
        limit_count,
        get_patterns,
        desc,
        alpha,
        store_key,
    })
}

fn apply_sort_limit<'a>(
    values: &'a [Vec<u8>],
    offset: usize,
    count: Option<usize>,
) -> &'a [Vec<u8>] {
    if offset >= values.len() {
        return &values[values.len()..values.len()];
    }
    let end = match count {
        Some(count) => offset.saturating_add(count).min(values.len()),
        None => values.len(),
    };
    &values[offset..end]
}

fn load_sort_elements(
    processor: &RequestProcessor,
    key: &[u8],
) -> Result<Vec<Vec<u8>>, RequestExecutionError> {
    processor.expire_key_if_needed(key)?;
    if processor.read_string_value(key)?.is_some() {
        return Err(RequestExecutionError::WrongType);
    }
    let Some(object) = processor.object_read(key)? else {
        return Ok(Vec::new());
    };
    match object.object_type {
        LIST_OBJECT_TYPE_TAG => deserialize_list_object_payload(&object.payload)
            .ok_or_else(|| storage_failure("sort", "failed to deserialize source list payload")),
        SET_OBJECT_TYPE_TAG => {
            let set = deserialize_set_object_payload(&object.payload).ok_or_else(|| {
                storage_failure("sort", "failed to deserialize source set payload")
            })?;
            Ok(set.into_iter().collect())
        }
        ZSET_OBJECT_TYPE_TAG => {
            let zset = deserialize_zset_object_payload(&object.payload).ok_or_else(|| {
                storage_failure("sort", "failed to deserialize source zset payload")
            })?;
            Ok(zset.into_keys().collect())
        }
        _ => Err(RequestExecutionError::WrongType),
    }
}

fn resolve_sort_get_value(
    processor: &RequestProcessor,
    pattern: &[u8],
    element: &[u8],
) -> Result<Option<Vec<u8>>, RequestExecutionError> {
    if pattern == b"#" {
        return Ok(Some(element.to_vec()));
    }
    resolve_sort_lookup_value(processor, pattern, element)
}

fn resolve_sort_lookup_value(
    processor: &RequestProcessor,
    pattern: &[u8],
    element: &[u8],
) -> Result<Option<Vec<u8>>, RequestExecutionError> {
    let split_pattern = split_sort_pattern(pattern);
    let key = substitute_sort_wildcard(split_pattern.key_pattern, element);
    processor.expire_key_if_needed(&key)?;

    if let Some(field_pattern) = split_pattern.hash_field_pattern {
        let field = substitute_sort_wildcard(field_pattern, element);
        return processor.hash_get_field_for_sort_lookup(&key, &field);
    }

    Ok(processor.read_string_value(&key)?)
}

struct SortPatternSplit<'a> {
    key_pattern: &'a [u8],
    hash_field_pattern: Option<&'a [u8]>,
}

fn split_sort_pattern(pattern: &[u8]) -> SortPatternSplit<'_> {
    let mut index = 0usize;
    while index + 1 < pattern.len() {
        if pattern[index] == b'-' && pattern[index + 1] == b'>' {
            return SortPatternSplit {
                key_pattern: &pattern[..index],
                hash_field_pattern: Some(&pattern[index + 2..]),
            };
        }
        index += 1;
    }
    SortPatternSplit {
        key_pattern: pattern,
        hash_field_pattern: None,
    }
}

fn substitute_sort_wildcard(pattern: &[u8], element: &[u8]) -> Vec<u8> {
    if !pattern.contains(&b'*') {
        return pattern.to_vec();
    }
    let mut out = Vec::with_capacity(pattern.len().saturating_add(element.len()));
    for byte in pattern {
        if *byte == b'*' {
            out.extend_from_slice(element);
        } else {
            out.push(*byte);
        }
    }
    out
}

fn apply_bitop(operation: BitopOperation, source_values: &[Vec<u8>]) -> Vec<u8> {
    if source_values.is_empty() {
        return Vec::new();
    }
    let max_len = source_values.iter().map(Vec::len).max().unwrap_or(0);
    let mut result = vec![0u8; max_len];
    for index in 0..max_len {
        let first = source_values[0].get(index).copied().unwrap_or(0);
        result[index] = match operation {
            BitopOperation::Not => !first,
            BitopOperation::And => source_values[1..].iter().fold(first, |acc, source| {
                acc & source.get(index).copied().unwrap_or(0)
            }),
            BitopOperation::Or => source_values[1..].iter().fold(first, |acc, source| {
                acc | source.get(index).copied().unwrap_or(0)
            }),
            BitopOperation::Xor => source_values[1..].iter().fold(first, |acc, source| {
                acc ^ source.get(index).copied().unwrap_or(0)
            }),
            BitopOperation::Diff | BitopOperation::Diff1 | BitopOperation::AndOr => {
                let others_or = source_values[1..].iter().fold(0u8, |acc, source| {
                    acc | source.get(index).copied().unwrap_or(0)
                });
                match operation {
                    BitopOperation::Diff => first & !others_or,
                    BitopOperation::Diff1 => !first & others_or,
                    BitopOperation::AndOr => first & others_or,
                    _ => unreachable!(),
                }
            }
            BitopOperation::One => {
                let mut seen_once = 0u8;
                let mut seen_multiple = 0u8;
                for source in source_values {
                    let source_byte = source.get(index).copied().unwrap_or(0);
                    seen_multiple |= seen_once & source_byte;
                    seen_once ^= source_byte;
                    seen_once &= !seen_multiple;
                }
                seen_once
            }
        };
    }
    result
}

fn load_pf_set_for_key(
    processor: &RequestProcessor,
    key: &[u8],
) -> Result<Option<PfSetState>, RequestExecutionError> {
    processor.expire_key_if_needed(key)?;
    let Some(value) = processor.read_string_value(key)? else {
        if processor.object_key_exists(key)? {
            return Err(RequestExecutionError::WrongType);
        }
        return Ok(None);
    };
    decode_pf_set(&value).map(Some)
}

fn encode_pf_set(state: &PfSetState) -> Vec<u8> {
    let prefix = if state.encoding == HllEncoding::Dense {
        PF_STRING_PREFIX_DENSE
    } else {
        PF_STRING_PREFIX_SPARSE
    };
    let mut encoded = Vec::with_capacity(prefix.len() + 2 + PFDEBUG_REGISTER_COUNT);
    encoded.extend_from_slice(prefix);
    encoded.push(0);
    encoded.push(if state.cache_dirty { 0x80 } else { 0x00 });
    encoded.extend_from_slice(&state.registers);
    encoded
}

fn decode_pf_set(raw: &[u8]) -> Result<PfSetState, RequestExecutionError> {
    if let Some(tail) = raw.strip_prefix(PF_STRING_PREFIX_DENSE) {
        return decode_pf_set_tail(tail, HllEncoding::Dense);
    }
    if let Some(tail) = raw.strip_prefix(PF_STRING_PREFIX_SPARSE) {
        return decode_pf_set_tail(tail, HllEncoding::Sparse);
    }
    if raw.starts_with(PF_REDIS_HLL_PREFIX) {
        return Err(RequestExecutionError::InvalidObject);
    }
    Err(RequestExecutionError::WrongType)
}

fn decode_pf_set_tail(
    tail: &[u8],
    encoding: HllEncoding,
) -> Result<PfSetState, RequestExecutionError> {
    if tail.len() != 2 + PFDEBUG_REGISTER_COUNT {
        return Err(RequestExecutionError::InvalidObject);
    }
    let cache_dirty = (tail[1] & 0x80) != 0;
    let mut registers = [0u8; PFDEBUG_REGISTER_COUNT];
    registers.copy_from_slice(&tail[2..]);
    Ok(PfSetState {
        registers,
        encoding,
        cache_dirty,
    })
}

fn pf_sparse_max_bytes(processor: &RequestProcessor) -> usize {
    for (key, value) in processor.config_items_snapshot() {
        if key == b"hll-sparse-max-bytes" {
            if let Some(parsed) = parse_u64_ascii(&value) {
                return parsed as usize;
            }
            return PF_SPARSE_MAX_BYTES_DEFAULT;
        }
    }
    PF_SPARSE_MAX_BYTES_DEFAULT
}

pub(super) fn string_value_len_for_keysizes(processor: &RequestProcessor, value: &[u8]) -> usize {
    if let Ok(pf_state) = decode_pf_set(value) {
        let sparse_max_bytes = pf_sparse_max_bytes(processor);
        if pf_state.encoding == HllEncoding::Dense {
            return sparse_max_bytes.saturating_add(1);
        }
        return pf_sparse_pseudo_length(pf_non_zero_register_count(&pf_state.registers));
    }
    value.len()
}

fn pf_sparse_pseudo_length(cardinality: usize) -> usize {
    16usize.saturating_add(cardinality.saturating_mul(3))
}

fn pf_should_promote_to_dense(cardinality: usize, sparse_max_bytes: usize) -> bool {
    pf_sparse_pseudo_length(cardinality) > sparse_max_bytes
}

fn pf_non_zero_register_count(registers: &[u8; PFDEBUG_REGISTER_COUNT]) -> usize {
    registers.iter().filter(|&&value| value != 0).count()
}

fn pf_register_index(hash: u64) -> usize {
    (hash & PF_REGISTER_INDEX_MASK) as usize
}

fn pf_register_rank(hash: u64) -> u8 {
    ((hash << PF_REGISTER_INDEX_BITS).leading_zeros() + 1).min(u32::from(PF_REGISTER_MAX_VALUE))
        as u8
}

fn pf_murmurhash64a(input: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4_a793_5bd1_e995;
    const R: u32 = 47;

    let mut hash = seed ^ ((input.len() as u64).wrapping_mul(M));
    let mut chunks = input.chunks_exact(8);

    for chunk in &mut chunks {
        let mut value = u64::from_le_bytes([
            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
        ]);
        value = value.wrapping_mul(M);
        value ^= value >> R;
        value = value.wrapping_mul(M);

        hash ^= value;
        hash = hash.wrapping_mul(M);
    }

    let tail = chunks.remainder();
    if !tail.is_empty() {
        for (index, byte) in tail.iter().enumerate() {
            hash ^= u64::from(*byte) << (index * 8);
        }
        hash = hash.wrapping_mul(M);
    }

    hash ^= hash >> R;
    hash = hash.wrapping_mul(M);
    hash ^= hash >> R;
    hash
}

fn pf_set_register_for_member(registers: &mut [u8; PFDEBUG_REGISTER_COUNT], member: &[u8]) -> bool {
    let hash = pf_murmurhash64a(member, PF_MURMUR64A_SEED);
    let index = pf_register_index(hash);
    let rank = pf_register_rank(hash);
    if rank > registers[index] {
        registers[index] = rank;
        return true;
    }
    false
}

fn pf_merge_registers(
    destination: &mut [u8; PFDEBUG_REGISTER_COUNT],
    source: &[u8; PFDEBUG_REGISTER_COUNT],
) {
    for (dst, src) in destination.iter_mut().zip(source.iter()) {
        *dst = (*dst).max(*src);
    }
}

fn pf_estimate_cardinality(registers: &[u8; PFDEBUG_REGISTER_COUNT]) -> i64 {
    let m = PFDEBUG_REGISTER_COUNT as f64;
    let alpha = 0.7213 / (1.0 + (1.079 / m));
    let mut inverse_sum = 0.0f64;
    let mut zero_registers = 0usize;
    for &rank in registers {
        if rank == 0 {
            zero_registers += 1;
            inverse_sum += 1.0;
        } else {
            inverse_sum += 2f64.powi(-(rank as i32));
        }
    }
    if inverse_sum == 0.0 {
        return 0;
    }
    let raw_estimate = alpha * m * m / inverse_sum;
    let estimate = if raw_estimate <= 2.5 * m && zero_registers > 0 {
        m * (m / (zero_registers as f64)).ln()
    } else {
        raw_estimate
    };
    estimate.round() as i64
}

fn hll_record_too_large_fallback_value<'a>(
    user_value: &'a [u8],
    error: &UpsertOperationError,
) -> Option<&'a [u8]> {
    if matches!(error, UpsertOperationError::RecordTooLarge { .. })
        && user_value.starts_with(PF_REDIS_HLL_PREFIX)
    {
        return Some(PF_REDIS_HLL_PREFIX);
    }
    None
}

fn canonicalize_oversized_hyll_value<'a>(user_value: &'a [u8]) -> &'a [u8] {
    const OVERSIZED_HYLL_CANONICALIZE_BYTES: usize = 256 * 1024;
    if user_value.starts_with(PF_REDIS_HLL_PREFIX)
        && user_value.len() > OVERSIZED_HYLL_CANONICALIZE_BYTES
    {
        return PF_REDIS_HLL_PREFIX;
    }
    user_value
}

fn normalize_string_range(len: usize, start: i64, end: i64) -> Option<NormalizedRange> {
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

    Some(NormalizedRange::new(start_i as usize, end_i as usize))
}

fn object_type_name(object_type: ObjectTypeTag) -> &'static [u8] {
    object_type.name()
}
