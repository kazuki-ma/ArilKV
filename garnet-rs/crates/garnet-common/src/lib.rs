//! Shared common types for garnet-rs.

pub mod resp;
pub mod span_byte;

pub use resp::{parse_resp_command, RespCommandMeta, RespParseError};
pub use span_byte::{
    SpanByte, SpanByteError, SpanByteRef, SpanByteRefMut, EXTRA_METADATA_BIT_MASK, HEADER_MASK,
    MAX_PAYLOAD_LENGTH, NAMESPACE_BIT_MASK, SPAN_BYTE_LENGTH_PREFIX_SIZE, UNSERIALIZED_BIT_MASK,
};
