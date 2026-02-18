//! Shared common types for garnet-rs.

pub mod arg_slice;
pub mod resp;
pub mod span_byte;

pub use arg_slice::{ArgSlice, ArgSliceError};
pub use resp::{
    parse_resp_command, parse_resp_command_arg_slices, RespCommandMeta, RespParseError,
};
pub use span_byte::{
    SpanByte, SpanByteError, SpanByteRef, SpanByteRefMut, EXTRA_METADATA_BIT_MASK, HEADER_MASK,
    MAX_PAYLOAD_LENGTH, NAMESPACE_BIT_MASK, SPAN_BYTE_LENGTH_PREFIX_SIZE, UNSERIALIZED_BIT_MASK,
};
