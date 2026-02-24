//! Record layout helpers for SpanByte key/value records in the hybrid log.
//!
//! Layout:
//! `[RecordInfo:8][Key:SpanByte][padding to 8-byte align][Value:SpanByte][padding to 8-byte align]`
//!
//! See [Doc 06 Section 2.5].

use crate::RecordInfo;
use garnet_common::{SPAN_BYTE_LENGTH_PREFIX_SIZE, SpanByteError, SpanByteRef, SpanByteRefMut};

pub const RECORD_ALIGNMENT: usize = 8;
pub const RECORD_INFO_SIZE: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordLayout {
    pub key_offset: usize,
    pub key_size: usize,
    pub aligned_key_size: usize,
    pub value_offset: usize,
    pub value_size: usize,
    pub actual_size: usize,
    pub allocated_size: usize,
}

impl RecordLayout {
    pub fn for_payload_lengths(
        key_payload_length: usize,
        value_payload_length: usize,
    ) -> Result<Self, RecordFormatError> {
        let key_size = SPAN_BYTE_LENGTH_PREFIX_SIZE
            .checked_add(key_payload_length)
            .ok_or(RecordFormatError::SizeOverflow)?;
        let aligned_key_size = round_up(key_size, RECORD_ALIGNMENT)?;

        let value_size = SPAN_BYTE_LENGTH_PREFIX_SIZE
            .checked_add(value_payload_length)
            .ok_or(RecordFormatError::SizeOverflow)?;

        let key_offset = RECORD_INFO_SIZE;
        let value_offset = RECORD_INFO_SIZE
            .checked_add(aligned_key_size)
            .ok_or(RecordFormatError::SizeOverflow)?;

        let actual_size = value_offset
            .checked_add(value_size)
            .ok_or(RecordFormatError::SizeOverflow)?;
        let allocated_size = round_up(actual_size, RECORD_ALIGNMENT)?;

        Ok(Self {
            key_offset,
            key_size,
            aligned_key_size,
            value_offset,
            value_size,
            actual_size,
            allocated_size,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordParsedLayout {
    pub key_offset: usize,
    pub key_size: usize,
    pub aligned_key_size: usize,
    pub value_offset: usize,
    pub value_size: usize,
    pub actual_size: usize,
    pub allocated_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordFormatError {
    DestinationTooSmall { required: usize, actual: usize },
    SourceTooSmall { required: usize, actual: usize },
    SizeOverflow,
    SpanByte(SpanByteError),
}

impl core::fmt::Display for RecordFormatError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::DestinationTooSmall { required, actual } => write!(
                f,
                "destination buffer too small: need {}, got {}",
                required, actual
            ),
            Self::SourceTooSmall { required, actual } => write!(
                f,
                "source buffer too small: need {}, got {}",
                required, actual
            ),
            Self::SizeOverflow => write!(f, "record size calculation overflow"),
            Self::SpanByte(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for RecordFormatError {}

impl From<SpanByteError> for RecordFormatError {
    fn from(value: SpanByteError) -> Self {
        Self::SpanByte(value)
    }
}

pub fn round_up(value: usize, alignment: usize) -> Result<usize, RecordFormatError> {
    if alignment == 0 {
        return Err(RecordFormatError::SizeOverflow);
    }

    let remainder = value % alignment;
    if remainder == 0 {
        return Ok(value);
    }

    value
        .checked_add(alignment - remainder)
        .ok_or(RecordFormatError::SizeOverflow)
}

pub fn write_record(
    destination: &mut [u8],
    record_info: RecordInfo,
    key_payload: &[u8],
    value_payload: &[u8],
) -> Result<RecordLayout, RecordFormatError> {
    let layout = RecordLayout::for_payload_lengths(key_payload.len(), value_payload.len())?;

    if destination.len() < layout.allocated_size {
        return Err(RecordFormatError::DestinationTooSmall {
            required: layout.allocated_size,
            actual: destination.len(),
        });
    }

    destination[..RECORD_INFO_SIZE].copy_from_slice(&record_info.raw_word().to_le_bytes());

    let key_region_end = layout.key_offset + layout.key_size;
    SpanByteRefMut::write_payload(
        &mut destination[layout.key_offset..key_region_end],
        key_payload,
    )?;

    let key_padding_end = layout.key_offset + layout.aligned_key_size;
    destination[key_region_end..key_padding_end].fill(0);

    let value_region_end = layout.value_offset + layout.value_size;
    SpanByteRefMut::write_payload(
        &mut destination[layout.value_offset..value_region_end],
        value_payload,
    )?;

    destination[layout.actual_size..layout.allocated_size].fill(0);

    Ok(layout)
}

pub fn parse_record_info(source: &[u8]) -> Result<RecordInfo, RecordFormatError> {
    if source.len() < RECORD_INFO_SIZE {
        return Err(RecordFormatError::SourceTooSmall {
            required: RECORD_INFO_SIZE,
            actual: source.len(),
        });
    }

    let mut bytes = [0u8; RECORD_INFO_SIZE];
    bytes.copy_from_slice(&source[..RECORD_INFO_SIZE]);
    Ok(RecordInfo::from_raw_word(u64::from_le_bytes(bytes)))
}

pub fn parse_key_span<'a>(source: &'a [u8]) -> Result<SpanByteRef<'a>, RecordFormatError> {
    if source.len() < RECORD_INFO_SIZE {
        return Err(RecordFormatError::SourceTooSmall {
            required: RECORD_INFO_SIZE,
            actual: source.len(),
        });
    }

    Ok(SpanByteRef::parse(&source[RECORD_INFO_SIZE..])?)
}

pub fn parse_record_layout(source: &[u8]) -> Result<RecordParsedLayout, RecordFormatError> {
    let key = parse_key_span(source)?;
    let key_size = key.as_bytes().len();
    let aligned_key_size = round_up(key_size, RECORD_ALIGNMENT)?;

    let value_offset = RECORD_INFO_SIZE
        .checked_add(aligned_key_size)
        .ok_or(RecordFormatError::SizeOverflow)?;
    if source.len() < value_offset {
        return Err(RecordFormatError::SourceTooSmall {
            required: value_offset,
            actual: source.len(),
        });
    }

    let value = SpanByteRef::parse(&source[value_offset..])?;
    let value_size = value.as_bytes().len();

    let actual_size = value_offset
        .checked_add(value_size)
        .ok_or(RecordFormatError::SizeOverflow)?;
    let allocated_size = round_up(actual_size, RECORD_ALIGNMENT)?;

    if source.len() < actual_size {
        return Err(RecordFormatError::SourceTooSmall {
            required: actual_size,
            actual: source.len(),
        });
    }

    Ok(RecordParsedLayout {
        key_offset: RECORD_INFO_SIZE,
        key_size,
        aligned_key_size,
        value_offset,
        value_size,
        actual_size,
        allocated_size,
    })
}

pub fn parse_value_span<'a>(source: &'a [u8]) -> Result<SpanByteRef<'a>, RecordFormatError> {
    let layout = parse_record_layout(source)?;
    Ok(SpanByteRef::parse(&source[layout.value_offset..])?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_aligned_record_layout() {
        let layout = RecordLayout::for_payload_lengths(5, 3).unwrap();
        assert_eq!(layout.key_offset, 8);
        assert_eq!(layout.key_size, 9);
        assert_eq!(layout.aligned_key_size, 16);
        assert_eq!(layout.value_offset, 24);
        assert_eq!(layout.value_size, 7);
        assert_eq!(layout.actual_size, 31);
        assert_eq!(layout.allocated_size, 32);
    }

    #[test]
    fn writes_and_parses_record_roundtrip() {
        let mut buffer = [0u8; 128];

        let mut info = RecordInfo::default();
        info.set_valid(true);
        info.set_tombstone();

        let layout = write_record(&mut buffer, info, b"hello", b"world").unwrap();
        let source = &buffer[..layout.allocated_size];

        let parsed_info = parse_record_info(source).unwrap();
        assert_eq!(parsed_info.raw_word(), info.raw_word());

        let key = parse_key_span(source).unwrap();
        assert_eq!(key.as_slice(), b"hello");

        let value = parse_value_span(source).unwrap();
        assert_eq!(value.as_slice(), b"world");

        let parsed_layout = parse_record_layout(source).unwrap();
        assert_eq!(parsed_layout.actual_size, layout.actual_size);
        assert_eq!(parsed_layout.allocated_size, layout.allocated_size);
    }

    #[test]
    fn rejects_small_destination() {
        let mut dst = [0u8; 16];
        let err =
            write_record(&mut dst, RecordInfo::default(), b"long-key", b"long-value").unwrap_err();
        assert!(matches!(err, RecordFormatError::DestinationTooSmall { .. }));
    }
}
