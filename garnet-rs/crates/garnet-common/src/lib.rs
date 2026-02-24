//! Shared common types for garnet-rs.

pub mod arg_slice;
pub mod resp;
pub mod span_byte;

pub use arg_slice::{ArgSlice, ArgSliceError};
pub use resp::{
    RespCommandMeta, RespParseError, parse_resp_command, parse_resp_command_arg_slices,
    parse_resp_command_arg_slices_dynamic,
};
pub use span_byte::{
    EXTRA_METADATA_BIT_MASK, HEADER_MASK, MAX_PAYLOAD_LENGTH, NAMESPACE_BIT_MASK,
    SPAN_BYTE_LENGTH_PREFIX_SIZE, SpanByte, SpanByteError, SpanByteRef, SpanByteRefMut,
    UNSERIALIZED_BIT_MASK,
};
