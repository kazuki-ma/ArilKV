use super::*;

#[derive(Clone, Copy, Debug)]
pub(super) struct ScanMatchCountOptions<'a> {
    pub(super) pattern: Option<&'a [u8]>,
    pub(super) count: usize,
    pub(super) novalues: bool,
}

#[inline]
pub(super) fn require_exact_arity(
    args: &[&[u8]],
    expected_count: usize,
    command: &'static str,
    expected: &'static str,
) -> Result<(), RequestExecutionError> {
    if args.len() == expected_count {
        return Ok(());
    }
    Err(RequestExecutionError::WrongArity { command, expected })
}

#[inline]
pub(super) fn ensure_min_arity(
    args: &[&[u8]],
    min_count: usize,
    command: &'static str,
    expected: &'static str,
) -> Result<(), RequestExecutionError> {
    if args.len() >= min_count {
        return Ok(());
    }
    Err(RequestExecutionError::WrongArity { command, expected })
}

#[inline]
pub(super) fn ensure_ranged_arity(
    args: &[&[u8]],
    min_count: usize,
    max_count: usize,
    command: &'static str,
    expected: &'static str,
) -> Result<(), RequestExecutionError> {
    if args.len() >= min_count && args.len() <= max_count {
        return Ok(());
    }
    Err(RequestExecutionError::WrongArity { command, expected })
}

#[inline]
pub(super) fn ensure_one_of_arities(
    args: &[&[u8]],
    allowed_counts: &[usize],
    command: &'static str,
    expected: &'static str,
) -> Result<(), RequestExecutionError> {
    if allowed_counts.contains(&args.len()) {
        return Ok(());
    }
    Err(RequestExecutionError::WrongArity { command, expected })
}

#[inline]
pub(super) fn ensure_paired_arity_after(
    args: &[&[u8]],
    min_count: usize,
    pair_start_index: usize,
    command: &'static str,
    expected: &'static str,
) -> Result<(), RequestExecutionError> {
    if args.len() < min_count || args.len() < pair_start_index {
        return Err(RequestExecutionError::WrongArity { command, expected });
    }
    if (args.len() - pair_start_index).is_multiple_of(2) {
        return Ok(());
    }
    Err(RequestExecutionError::WrongArity { command, expected })
}

pub(super) fn parse_scan_match_count_options<'a>(
    args: &'a [&'a [u8]],
    start_index: usize,
) -> Result<ScanMatchCountOptions<'a>, RequestExecutionError> {
    let mut options = ScanMatchCountOptions {
        pattern: None,
        count: 10,
        novalues: false,
    };
    let mut index = start_index;
    while index < args.len() {
        let token = args[index];
        if ascii_eq_ignore_case(token, b"MATCH") {
            if index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            options.pattern = Some(args[index + 1]);
            index += 2;
            continue;
        }
        if ascii_eq_ignore_case(token, b"COUNT") {
            if index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            let raw_count =
                parse_u64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
            if raw_count == 0 {
                return Err(RequestExecutionError::ValueOutOfRange);
            }
            options.count = usize::try_from(raw_count).unwrap_or(usize::MAX);
            index += 2;
            continue;
        }
        if ascii_eq_ignore_case(token, b"NOVALUES") {
            options.novalues = true;
            index += 1;
            continue;
        }
        return Err(RequestExecutionError::SyntaxError);
    }
    Ok(options)
}
