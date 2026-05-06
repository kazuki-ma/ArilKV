//! SpanByte: variable-length byte sequence with a 4-byte packed length prefix.
//!
//! See [Doc 06 Section 2.4] for the serialized layout and header-bit semantics.

use core::mem::size_of;
use core::ops::Range;

/// Size of the SpanByte length prefix in bytes.
pub const SPAN_BYTE_LENGTH_PREFIX_SIZE: usize = size_of::<u32>();

/// Bit 31 marks an unserialized (pointer-backed) representation in the C# implementation.
pub const UNSERIALIZED_BIT_MASK: u32 = 1 << 31;
/// Bit 30 marks presence of 8-byte extra metadata at the start of payload.
pub const EXTRA_METADATA_BIT_MASK: u32 = 1 << 30;
/// Bit 29 marks presence of 1-byte namespace metadata at the start of payload.
pub const NAMESPACE_BIT_MASK: u32 = 1 << 29;
/// All flag bits in the 4-byte packed header.
pub const HEADER_MASK: u32 = UNSERIALIZED_BIT_MASK | EXTRA_METADATA_BIT_MASK | NAMESPACE_BIT_MASK;
/// Maximum payload length representable in the 29-bit length field.
#[allow(clippy::identity_op)]
pub const MAX_PAYLOAD_LENGTH: usize = (u32::MAX & !HEADER_MASK) as usize;

/// Errors produced when parsing or materializing serialized [`SpanByte`] values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanByteError {
    BufferTooShort {
        actual: usize,
    },
    BufferTooShortForPayload {
        required: usize,
        actual: usize,
    },
    PayloadTooLarge {
        payload_length: usize,
        max_payload_length: usize,
    },
    ConflictingMetadataFlags,
    MetadataExceedsPayload {
        metadata_size: usize,
        payload_length: usize,
    },
}

impl core::fmt::Display for SpanByteError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::BufferTooShort { actual } => {
                write!(
                    f,
                    "spanbyte buffer is too short: need at least {} bytes, got {}",
                    SPAN_BYTE_LENGTH_PREFIX_SIZE, actual
                )
            }
            Self::BufferTooShortForPayload { required, actual } => {
                write!(
                    f,
                    "spanbyte buffer is too short for payload: need {} bytes, got {}",
                    required, actual
                )
            }
            Self::PayloadTooLarge {
                payload_length,
                max_payload_length,
            } => {
                write!(
                    f,
                    "spanbyte payload length {} exceeds max {}",
                    payload_length, max_payload_length
                )
            }
            Self::ConflictingMetadataFlags => {
                write!(f, "spanbyte has both extra-metadata and namespace bits set")
            }
            Self::MetadataExceedsPayload {
                metadata_size,
                payload_length,
            } => write!(
                f,
                "spanbyte metadata size {} exceeds payload length {}",
                metadata_size, payload_length
            ),
        }
    }
}

impl std::error::Error for SpanByteError {}

/// 4-byte packed SpanByte header.
///
/// Layout: lower 29 bits store payload length; upper 3 bits store flags.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SpanByte {
    raw_length: u32,
}

impl SpanByte {
    /// Creates a new serialized SpanByte header with the provided payload length.
    pub fn new(payload_length: usize) -> Result<Self, SpanByteError> {
        if payload_length > MAX_PAYLOAD_LENGTH {
            return Err(SpanByteError::PayloadTooLarge {
                payload_length,
                max_payload_length: MAX_PAYLOAD_LENGTH,
            });
        }

        Ok(Self {
            raw_length: payload_length as u32,
        })
    }

    /// Creates a SpanByte header directly from the packed raw 32-bit field.
    pub const fn from_raw_length(raw_length: u32) -> Self {
        Self { raw_length }
    }

    /// Returns the packed 32-bit length field (flags + payload length).
    pub const fn raw_length(self) -> u32 {
        self.raw_length
    }

    /// Returns true when serialized representation is indicated by the header.
    pub const fn is_serialized(self) -> bool {
        (self.raw_length & UNSERIALIZED_BIT_MASK) == 0
    }

    /// Returns true if the extra metadata flag is set.
    pub const fn has_extra_metadata(self) -> bool {
        (self.raw_length & EXTRA_METADATA_BIT_MASK) != 0
    }

    /// Returns true if the namespace metadata flag is set.
    pub const fn has_namespace(self) -> bool {
        (self.raw_length & NAMESPACE_BIT_MASK) != 0
    }

    /// Returns the payload length, including metadata bytes when metadata flags are set.
    pub const fn payload_length(self) -> usize {
        (self.raw_length & !HEADER_MASK) as usize
    }

    /// Returns the serialized byte size (4-byte prefix + payload).
    pub const fn total_size(self) -> usize {
        SPAN_BYTE_LENGTH_PREFIX_SIZE + self.payload_length()
    }

    /// Returns metadata byte size (0, 1, 8, or 9 depending on set bits).
    pub const fn metadata_size_unchecked(self) -> usize {
        ((self.raw_length & EXTRA_METADATA_BIT_MASK) >> 27) as usize
            + ((self.raw_length & NAMESPACE_BIT_MASK) >> 29) as usize
    }

    /// Returns metadata byte size while enforcing C# current constraint (no dual extension bits).
    pub fn metadata_size(self) -> Result<usize, SpanByteError> {
        if self.has_extra_metadata() && self.has_namespace() {
            return Err(SpanByteError::ConflictingMetadataFlags);
        }

        Ok(self.metadata_size_unchecked())
    }

    /// Returns payload length excluding metadata bytes.
    pub fn payload_length_without_metadata(self) -> Result<usize, SpanByteError> {
        let metadata_size = self.metadata_size()?;
        let payload_length = self.payload_length();

        payload_length
            .checked_sub(metadata_size)
            .ok_or(SpanByteError::MetadataExceedsPayload {
                metadata_size,
                payload_length,
            })
    }

    /// Returns the payload range within a serialized SpanByte byte sequence.
    pub fn payload_range(self, include_metadata: bool) -> Result<Range<usize>, SpanByteError> {
        let payload_length = self.payload_length();
        let metadata_size = if include_metadata {
            0
        } else {
            self.metadata_size()?
        };

        if metadata_size > payload_length {
            return Err(SpanByteError::MetadataExceedsPayload {
                metadata_size,
                payload_length,
            });
        }

        let start = SPAN_BYTE_LENGTH_PREFIX_SIZE + metadata_size;
        let end = SPAN_BYTE_LENGTH_PREFIX_SIZE + payload_length;
        Ok(start..end)
    }

    /// Returns a copy of this header with the extra metadata flag set.
    pub fn with_extra_metadata(self) -> Result<Self, SpanByteError> {
        if self.has_namespace() {
            return Err(SpanByteError::ConflictingMetadataFlags);
        }

        if self.payload_length() < 8 {
            return Err(SpanByteError::MetadataExceedsPayload {
                metadata_size: 8,
                payload_length: self.payload_length(),
            });
        }

        Ok(Self {
            raw_length: self.raw_length | EXTRA_METADATA_BIT_MASK,
        })
    }

    /// Returns a copy of this header with the namespace metadata flag set.
    pub fn with_namespace(self) -> Result<Self, SpanByteError> {
        if self.has_extra_metadata() {
            return Err(SpanByteError::ConflictingMetadataFlags);
        }

        if self.payload_length() < 1 {
            return Err(SpanByteError::MetadataExceedsPayload {
                metadata_size: 1,
                payload_length: self.payload_length(),
            });
        }

        Ok(Self {
            raw_length: self.raw_length | NAMESPACE_BIT_MASK,
        })
    }

    /// Returns a copy of this header without extra metadata flag.
    pub const fn clear_extra_metadata(self) -> Self {
        Self {
            raw_length: self.raw_length & !EXTRA_METADATA_BIT_MASK,
        }
    }

    /// Returns a copy of this header without namespace metadata flag.
    pub const fn clear_namespace(self) -> Self {
        Self {
            raw_length: self.raw_length & !NAMESPACE_BIT_MASK,
        }
    }

    /// Writes this packed header to the start of `destination`.
    pub fn write_prefix(self, destination: &mut [u8]) -> Result<(), SpanByteError> {
        if destination.len() < SPAN_BYTE_LENGTH_PREFIX_SIZE {
            return Err(SpanByteError::BufferTooShort {
                actual: destination.len(),
            });
        }

        destination[..SPAN_BYTE_LENGTH_PREFIX_SIZE].copy_from_slice(&self.raw_length.to_le_bytes());
        Ok(())
    }

    /// Reads the packed header from the start of `bytes`.
    pub fn read_prefix(bytes: &[u8]) -> Result<Self, SpanByteError> {
        if bytes.len() < SPAN_BYTE_LENGTH_PREFIX_SIZE {
            return Err(SpanByteError::BufferTooShort {
                actual: bytes.len(),
            });
        }

        let raw_length = u32::from_le_bytes(
            bytes[..SPAN_BYTE_LENGTH_PREFIX_SIZE]
                .try_into()
                .expect("length prefix is exactly 4 bytes after explicit bound check"),
        );
        Ok(Self::from_raw_length(raw_length))
    }

    fn validate(self) -> Result<(), SpanByteError> {
        if self.payload_length() > MAX_PAYLOAD_LENGTH {
            return Err(SpanByteError::PayloadTooLarge {
                payload_length: self.payload_length(),
                max_payload_length: MAX_PAYLOAD_LENGTH,
            });
        }

        let _ = self.payload_length_without_metadata()?;
        Ok(())
    }
}

/// Borrowed immutable view over serialized SpanByte bytes: `[length-prefix][payload...]`.
#[derive(Debug, Clone, Copy)]
pub struct SpanByteRef<'a> {
    bytes: &'a [u8],
    header: SpanByte,
}

impl<'a> SpanByteRef<'a> {
    /// Parses a SpanByte from the beginning of `bytes`.
    ///
    /// The returned view is sliced to the exact serialized length (`total_size`).
    pub fn parse(bytes: &'a [u8]) -> Result<Self, SpanByteError> {
        let header = SpanByte::read_prefix(bytes)?;
        header.validate()?;

        let required = header.total_size();
        if bytes.len() < required {
            return Err(SpanByteError::BufferTooShortForPayload {
                required,
                actual: bytes.len(),
            });
        }

        Ok(Self {
            bytes: &bytes[..required],
            header,
        })
    }

    /// Returns the parsed SpanByte header.
    pub const fn header(&self) -> SpanByte {
        self.header
    }

    /// Returns serialized bytes including 4-byte prefix.
    pub const fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }

    /// Returns payload bytes excluding optional metadata.
    pub fn as_slice(&self) -> &'a [u8] {
        let range = self
            .header
            .payload_range(false)
            .expect("parsed SpanByteRef validates payload and metadata invariants");
        &self.bytes[range]
    }

    /// Returns payload bytes including optional metadata.
    pub fn as_slice_with_metadata(&self) -> &'a [u8] {
        let range = self
            .header
            .payload_range(true)
            .expect("parsed SpanByteRef validates payload and metadata invariants");
        &self.bytes[range]
    }
}

/// Borrowed mutable view over serialized SpanByte bytes: `[length-prefix][payload...]`.
#[derive(Debug)]
pub struct SpanByteRefMut<'a> {
    bytes: &'a mut [u8],
    header: SpanByte,
}

impl<'a> SpanByteRefMut<'a> {
    /// Parses a mutable SpanByte from the beginning of `bytes`.
    ///
    /// The returned view is sliced to the exact serialized length (`total_size`).
    pub fn parse(bytes: &'a mut [u8]) -> Result<Self, SpanByteError> {
        let header = SpanByte::read_prefix(bytes)?;
        header.validate()?;

        let required = header.total_size();
        if bytes.len() < required {
            return Err(SpanByteError::BufferTooShortForPayload {
                required,
                actual: bytes.len(),
            });
        }

        let (span_byte_bytes, _) = bytes.split_at_mut(required);
        Ok(Self {
            bytes: span_byte_bytes,
            header,
        })
    }

    /// Initializes a serialized SpanByte in `bytes` with a given payload length.
    pub fn initialize(bytes: &'a mut [u8], payload_length: usize) -> Result<Self, SpanByteError> {
        let header = SpanByte::new(payload_length)?;
        header.validate()?;

        let required = header.total_size();
        if bytes.len() < required {
            return Err(SpanByteError::BufferTooShortForPayload {
                required,
                actual: bytes.len(),
            });
        }

        header.write_prefix(bytes)?;

        let (span_byte_bytes, _) = bytes.split_at_mut(required);
        Ok(Self {
            bytes: span_byte_bytes,
            header,
        })
    }

    /// Initializes and copies `payload` into the serialized SpanByte.
    pub fn write_payload(bytes: &'a mut [u8], payload: &[u8]) -> Result<Self, SpanByteError> {
        let mut span_byte = Self::initialize(bytes, payload.len())?;
        span_byte.as_mut_slice().copy_from_slice(payload);
        Ok(span_byte)
    }

    /// Returns the parsed SpanByte header.
    pub const fn header(&self) -> SpanByte {
        self.header
    }

    /// Returns serialized bytes including 4-byte prefix.
    pub fn as_bytes(&self) -> &[u8] {
        self.bytes
    }

    /// Returns payload bytes excluding optional metadata.
    pub fn as_slice(&self) -> &[u8] {
        let range = self
            .header
            .payload_range(false)
            .expect("parsed SpanByteRefMut validates payload and metadata invariants");
        &self.bytes[range]
    }

    /// Returns mutable payload bytes excluding optional metadata.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        let range = self
            .header
            .payload_range(false)
            .expect("parsed SpanByteRefMut validates payload and metadata invariants");
        &mut self.bytes[range]
    }

    /// Returns payload bytes including optional metadata.
    pub fn as_slice_with_metadata(&self) -> &[u8] {
        let range = self
            .header
            .payload_range(true)
            .expect("parsed SpanByteRefMut validates payload and metadata invariants");
        &self.bytes[range]
    }

    /// Returns mutable payload bytes including optional metadata.
    pub fn as_mut_slice_with_metadata(&mut self) -> &mut [u8] {
        let range = self
            .header
            .payload_range(true)
            .expect("parsed SpanByteRefMut validates payload and metadata invariants");
        &mut self.bytes[range]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn span_byte_roundtrip_payload() {
        let mut bytes = vec![0u8; SPAN_BYTE_LENGTH_PREFIX_SIZE + 5];
        let payload = [1u8, 2, 3, 4, 5];

        let mut span_byte = SpanByteRefMut::write_payload(&mut bytes, &payload).unwrap();
        assert_eq!(span_byte.header().payload_length(), payload.len());
        assert_eq!(span_byte.as_slice(), payload);

        span_byte.as_mut_slice()[0] = 9;

        let parsed = SpanByteRef::parse(&bytes).unwrap();
        assert_eq!(parsed.as_slice(), [9u8, 2, 3, 4, 5]);
    }

    proptest! {
        #[test]
        fn span_byte_roundtrip_proptest(payload in proptest::collection::vec(any::<u8>(), 0usize..4096)) {
            let mut bytes = vec![0u8; SPAN_BYTE_LENGTH_PREFIX_SIZE + payload.len()];
            let mut span_byte = SpanByteRefMut::write_payload(&mut bytes, &payload).unwrap();

            prop_assert_eq!(span_byte.as_slice(), payload.as_slice());
            span_byte
                .as_mut_slice()
                .iter_mut()
                .for_each(|byte| *byte = byte.wrapping_add(1));

            let parsed = SpanByteRef::parse(&bytes).unwrap();
            let expected = payload
                .iter()
                .map(|byte| byte.wrapping_add(1))
                .collect::<Vec<_>>();
            prop_assert_eq!(parsed.as_slice(), expected.as_slice());
        }
    }

    #[test]
    fn span_byte_respects_metadata_offsets() {
        let header = SpanByte::new(8).unwrap().with_extra_metadata().unwrap();
        assert_eq!(header.metadata_size().unwrap(), 8);
        assert_eq!(header.payload_length_without_metadata().unwrap(), 0);

        let mut bytes = vec![0u8; header.total_size()];
        header.write_prefix(&mut bytes).unwrap();
        bytes[SPAN_BYTE_LENGTH_PREFIX_SIZE..].copy_from_slice(&[7; 8]);

        let parsed = SpanByteRef::parse(&bytes).unwrap();
        assert_eq!(parsed.as_slice().len(), 0);
        assert_eq!(parsed.as_slice_with_metadata(), [7; 8]);
    }

    #[test]
    fn parse_rejects_short_buffers() {
        let err = SpanByteRef::parse(&[]).unwrap_err();
        assert_eq!(err, SpanByteError::BufferTooShort { actual: 0 });
    }

    #[test]
    fn parse_rejects_incomplete_payload() {
        let mut bytes = [0u8; SPAN_BYTE_LENGTH_PREFIX_SIZE + 2];
        SpanByte::new(8).unwrap().write_prefix(&mut bytes).unwrap();

        let err = SpanByteRef::parse(&bytes).unwrap_err();
        assert_eq!(
            err,
            SpanByteError::BufferTooShortForPayload {
                required: SPAN_BYTE_LENGTH_PREFIX_SIZE + 8,
                actual: SPAN_BYTE_LENGTH_PREFIX_SIZE + 2,
            }
        );
    }

    #[test]
    fn disallows_conflicting_metadata_flags() {
        let raw = (8u32 | EXTRA_METADATA_BIT_MASK) | NAMESPACE_BIT_MASK;
        let err = SpanByteRef::parse(&raw.to_le_bytes()).unwrap_err();
        assert_eq!(err, SpanByteError::ConflictingMetadataFlags);
    }

    #[test]
    fn span_byte_header_is_4_bytes() {
        assert_eq!(size_of::<SpanByte>(), SPAN_BYTE_LENGTH_PREFIX_SIZE);
    }
}
