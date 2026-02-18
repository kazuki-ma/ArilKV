//! Tsavorite storage engine for garnet-rs.

pub mod record_info;

pub use record_info::{
    RecordInfo, PREVIOUS_ADDRESS_BITS, PREVIOUS_ADDRESS_MASK, RECORD_INFO_LENGTH,
};
