//! Session callback trait definitions for Tsavorite operations.
//!
//! See [Doc 10 Section 2] for callback roles and operation-phase semantics.

use crate::LogicalAddress;
use crate::RecordInfo;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(transparent)]
pub struct SessionUserData(u8);

impl SessionUserData {
    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn from_bits(bits: u8) -> Self {
        Self(bits)
    }

    pub const fn bits(self) -> u8 {
        self.0
    }

    pub const fn contains(self, flags: Self) -> bool {
        (self.0 & flags.0) == flags.0
    }

    pub fn insert(&mut self, flags: Self) {
        self.0 |= flags.0;
    }
}

impl From<u8> for SessionUserData {
    fn from(value: u8) -> Self {
        Self::from_bits(value)
    }
}

impl From<SessionUserData> for u8 {
    fn from(value: SessionUserData) -> Self {
        value.bits()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadInfo {
    pub logical_address: LogicalAddress,
    pub user_data: SessionUserData,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UpsertInfo {
    pub logical_address: LogicalAddress,
    pub user_data: SessionUserData,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RmwInfo {
    pub logical_address: LogicalAddress,
    pub user_data: SessionUserData,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeleteInfo {
    pub logical_address: LogicalAddress,
    pub user_data: SessionUserData,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteReason {
    Insert,
    CopyUpdate,
}

/// Generic callback interface used by Tsavorite operation paths.
///
/// This maps the core read/upsert/RMW callbacks used by Phase 4 operation flows.
pub trait ISessionFunctions {
    type Key;
    type Value;
    type Input;
    type Output;
    type Context;
    type Reader;
    type Writer;
    type Comparer;

    fn single_reader(
        &self,
        key: &Self::Key,
        input: &Self::Input,
        value: &Self::Value,
        output: &mut Self::Output,
        read_info: &ReadInfo,
    ) -> bool;

    fn concurrent_reader(
        &self,
        key: &Self::Key,
        input: &Self::Input,
        value: &Self::Value,
        output: &mut Self::Output,
        read_info: &ReadInfo,
        record_info: &RecordInfo,
    ) -> bool;

    fn single_writer(
        &self,
        key: &Self::Key,
        input: &Self::Input,
        src: &Self::Value,
        dst: &mut Self::Value,
        output: &mut Self::Output,
        upsert_info: &mut UpsertInfo,
        reason: WriteReason,
        record_info: &mut RecordInfo,
    ) -> bool;

    fn concurrent_writer(
        &self,
        key: &Self::Key,
        input: &Self::Input,
        src: &Self::Value,
        dst: &mut Self::Value,
        output: &mut Self::Output,
        upsert_info: &mut UpsertInfo,
        record_info: &mut RecordInfo,
    ) -> bool;

    fn in_place_updater(
        &self,
        key: &Self::Key,
        input: &Self::Input,
        value: &mut Self::Value,
        output: &mut Self::Output,
        rmw_info: &mut RmwInfo,
        record_info: &mut RecordInfo,
    ) -> bool;

    fn copy_updater(
        &self,
        key: &Self::Key,
        input: &Self::Input,
        old_value: &Self::Value,
        new_value: &mut Self::Value,
        output: &mut Self::Output,
        rmw_info: &mut RmwInfo,
        record_info: &mut RecordInfo,
    ) -> bool;

    fn single_deleter(
        &self,
        key: &Self::Key,
        value: &mut Self::Value,
        delete_info: &mut DeleteInfo,
        record_info: &mut RecordInfo,
    ) -> bool;

    fn concurrent_deleter(
        &self,
        key: &Self::Key,
        value: &mut Self::Value,
        delete_info: &mut DeleteInfo,
        record_info: &mut RecordInfo,
    ) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct IntSessionFunctions;

    impl ISessionFunctions for IntSessionFunctions {
        type Key = u64;
        type Value = i64;
        type Input = i64;
        type Output = i64;
        type Context = ();
        type Reader = ();
        type Writer = ();
        type Comparer = ();

        fn single_reader(
            &self,
            _key: &Self::Key,
            input: &Self::Input,
            value: &Self::Value,
            output: &mut Self::Output,
            _read_info: &ReadInfo,
        ) -> bool {
            *output = *value + *input;
            true
        }

        fn concurrent_reader(
            &self,
            _key: &Self::Key,
            input: &Self::Input,
            value: &Self::Value,
            output: &mut Self::Output,
            _read_info: &ReadInfo,
            _record_info: &RecordInfo,
        ) -> bool {
            *output = *value + *input + 1;
            true
        }

        fn single_writer(
            &self,
            _key: &Self::Key,
            input: &Self::Input,
            src: &Self::Value,
            dst: &mut Self::Value,
            output: &mut Self::Output,
            upsert_info: &mut UpsertInfo,
            _reason: WriteReason,
            record_info: &mut RecordInfo,
        ) -> bool {
            *dst = *src + *input;
            *output = *dst;
            upsert_info.user_data = SessionUserData::from_bits(1);
            record_info.set_dirty();
            true
        }

        fn concurrent_writer(
            &self,
            _key: &Self::Key,
            input: &Self::Input,
            src: &Self::Value,
            dst: &mut Self::Value,
            output: &mut Self::Output,
            upsert_info: &mut UpsertInfo,
            record_info: &mut RecordInfo,
        ) -> bool {
            *dst = *src + *input + 10;
            *output = *dst;
            upsert_info.user_data = SessionUserData::from_bits(2);
            record_info.set_modified(true);
            true
        }

        fn in_place_updater(
            &self,
            _key: &Self::Key,
            input: &Self::Input,
            value: &mut Self::Value,
            output: &mut Self::Output,
            rmw_info: &mut RmwInfo,
            record_info: &mut RecordInfo,
        ) -> bool {
            *value += *input;
            *output = *value;
            rmw_info.user_data = SessionUserData::from_bits(3);
            record_info.set_dirty();
            true
        }

        fn copy_updater(
            &self,
            _key: &Self::Key,
            input: &Self::Input,
            old_value: &Self::Value,
            new_value: &mut Self::Value,
            output: &mut Self::Output,
            rmw_info: &mut RmwInfo,
            record_info: &mut RecordInfo,
        ) -> bool {
            *new_value = *old_value + *input + 100;
            *output = *new_value;
            rmw_info.user_data = SessionUserData::from_bits(4);
            record_info.set_modified(true);
            true
        }

        fn single_deleter(
            &self,
            _key: &Self::Key,
            value: &mut Self::Value,
            delete_info: &mut DeleteInfo,
            record_info: &mut RecordInfo,
        ) -> bool {
            *value = 0;
            delete_info.user_data = SessionUserData::from_bits(5);
            record_info.set_tombstone();
            true
        }

        fn concurrent_deleter(
            &self,
            _key: &Self::Key,
            value: &mut Self::Value,
            delete_info: &mut DeleteInfo,
            record_info: &mut RecordInfo,
        ) -> bool {
            *value = 0;
            delete_info.user_data = SessionUserData::from_bits(6);
            record_info.set_tombstone();
            true
        }
    }

    #[test]
    fn session_functions_callbacks_can_be_implemented_and_invoked() {
        let functions = IntSessionFunctions;
        let key = 7u64;
        let input = 3i64;
        let src = 10i64;
        let mut dst = 0i64;
        let mut output = 0i64;
        let read_info = ReadInfo::default();
        let mut upsert_info = UpsertInfo::default();
        let mut rmw_info = RmwInfo::default();
        let mut delete_info = DeleteInfo::default();
        let mut record_info = RecordInfo::default();

        assert!(functions.single_reader(&key, &input, &src, &mut output, &read_info));
        assert_eq!(output, 13);

        assert!(functions.concurrent_reader(
            &key,
            &input,
            &src,
            &mut output,
            &read_info,
            &record_info
        ));
        assert_eq!(output, 14);

        assert!(functions.single_writer(
            &key,
            &input,
            &src,
            &mut dst,
            &mut output,
            &mut upsert_info,
            WriteReason::Insert,
            &mut record_info
        ));
        assert_eq!(dst, 13);
        assert_eq!(upsert_info.user_data.bits(), 1);
        assert!(record_info.dirty());

        record_info.clear_dirty();
        assert!(functions.concurrent_writer(
            &key,
            &input,
            &src,
            &mut dst,
            &mut output,
            &mut upsert_info,
            &mut record_info
        ));
        assert_eq!(dst, 23);
        assert_eq!(upsert_info.user_data.bits(), 2);
        assert!(record_info.modified());

        assert!(functions.in_place_updater(
            &key,
            &input,
            &mut dst,
            &mut output,
            &mut rmw_info,
            &mut record_info
        ));
        assert_eq!(dst, 26);
        assert_eq!(rmw_info.user_data.bits(), 3);

        assert!(functions.copy_updater(
            &key,
            &input,
            &src,
            &mut dst,
            &mut output,
            &mut rmw_info,
            &mut record_info
        ));
        assert_eq!(dst, 113);
        assert_eq!(rmw_info.user_data.bits(), 4);

        assert!(functions.single_deleter(&key, &mut dst, &mut delete_info, &mut record_info));
        assert_eq!(dst, 0);
        assert_eq!(delete_info.user_data.bits(), 5);
        assert!(record_info.tombstone());

        record_info.clear_tombstone();
        assert!(functions.concurrent_deleter(&key, &mut dst, &mut delete_info, &mut record_info));
        assert_eq!(delete_info.user_data.bits(), 6);
        assert!(record_info.tombstone());
    }
}
