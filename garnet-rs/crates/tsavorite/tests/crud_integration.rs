use std::sync::{Arc, Mutex};
use std::thread;
use tsavorite::{
    DeleteInfo, DeleteOperationStatus, HybridLogDeleteAdapter, HybridLogReadAdapter,
    HybridLogRmwAdapter, HybridLogUpsertAdapter, ISessionFunctions, ReadInfo, ReadOperationStatus,
    RecordInfo, RmwInfo, TsavoriteKV, TsavoriteKvConfig, UpsertInfo, WriteReason,
};

struct CrudSessionFunctions;

impl ISessionFunctions for CrudSessionFunctions {
    type Key = Vec<u8>;
    type Value = Vec<u8>;
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    type Context = ();
    type Reader = ();
    type Writer = ();
    type Comparer = ();

    fn single_reader(
        &self,
        _key: &Self::Key,
        _input: &Self::Input,
        value: &Self::Value,
        output: &mut Self::Output,
        _read_info: &ReadInfo,
    ) -> bool {
        *output = value.clone();
        true
    }

    fn concurrent_reader(
        &self,
        _key: &Self::Key,
        _input: &Self::Input,
        value: &Self::Value,
        output: &mut Self::Output,
        _read_info: &ReadInfo,
        _record_info: &RecordInfo,
    ) -> bool {
        *output = value.clone();
        true
    }

    fn single_writer(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        _src: &Self::Value,
        dst: &mut Self::Value,
        output: &mut Self::Output,
        _upsert_info: &mut UpsertInfo,
        _reason: WriteReason,
        _record_info: &mut RecordInfo,
    ) -> bool {
        *dst = input.clone();
        *output = dst.clone();
        true
    }

    fn concurrent_writer(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        _src: &Self::Value,
        dst: &mut Self::Value,
        output: &mut Self::Output,
        _upsert_info: &mut UpsertInfo,
        _record_info: &mut RecordInfo,
    ) -> bool {
        *dst = input.clone();
        *output = dst.clone();
        true
    }

    fn in_place_updater(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        value: &mut Self::Value,
        output: &mut Self::Output,
        _rmw_info: &mut RmwInfo,
        _record_info: &mut RecordInfo,
    ) -> bool {
        *value = input.clone();
        *output = value.clone();
        true
    }

    fn copy_updater(
        &self,
        _key: &Self::Key,
        input: &Self::Input,
        _old_value: &Self::Value,
        new_value: &mut Self::Value,
        output: &mut Self::Output,
        _rmw_info: &mut RmwInfo,
        _record_info: &mut RecordInfo,
    ) -> bool {
        *new_value = input.clone();
        *output = new_value.clone();
        true
    }

    fn single_deleter(
        &self,
        _key: &Self::Key,
        value: &mut Self::Value,
        _delete_info: &mut DeleteInfo,
        record_info: &mut RecordInfo,
    ) -> bool {
        value.clear();
        record_info.set_tombstone();
        true
    }

    fn concurrent_deleter(
        &self,
        _key: &Self::Key,
        value: &mut Self::Value,
        _delete_info: &mut DeleteInfo,
        record_info: &mut RecordInfo,
    ) -> bool {
        value.clear();
        record_info.set_tombstone();
        true
    }
}

impl HybridLogReadAdapter for CrudSessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
        requested_key.as_slice() == record_key
    }

    fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
        record_value.to_vec()
    }
}

impl HybridLogUpsertAdapter for CrudSessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
        requested_key.as_slice() == record_key
    }

    fn key_to_record_bytes(&self, key: &Self::Key) -> Vec<u8> {
        key.clone()
    }

    fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
        record_value.to_vec()
    }

    fn value_to_record_bytes(&self, value: &Self::Value) -> Vec<u8> {
        value.clone()
    }
}

impl HybridLogRmwAdapter for CrudSessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
        requested_key.as_slice() == record_key
    }

    fn key_to_record_bytes(&self, key: &Self::Key) -> Vec<u8> {
        key.clone()
    }

    fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
        record_value.to_vec()
    }

    fn value_to_record_bytes(&self, value: &Self::Value) -> Vec<u8> {
        value.clone()
    }
}

impl HybridLogDeleteAdapter for CrudSessionFunctions {
    fn record_key_equals(&self, requested_key: &Self::Key, record_key: &[u8]) -> bool {
        requested_key.as_slice() == record_key
    }

    fn key_to_record_bytes(&self, key: &Self::Key) -> Vec<u8> {
        key.clone()
    }

    fn value_from_record(&self, record_value: &[u8]) -> Self::Value {
        record_value.to_vec()
    }

    fn value_to_record_bytes(&self, value: &Self::Value) -> Vec<u8> {
        value.clone()
    }
}

fn test_config() -> TsavoriteKvConfig {
    TsavoriteKvConfig {
        hash_index_size_bits: 6,
        page_size_bits: 10,
        max_in_memory_pages: 64,
        initial_logical_address: tsavorite::RECORD_ALIGNMENT as u64,
    }
}

fn key_for(i: usize) -> Vec<u8> {
    format!("key-{i:04}").into_bytes()
}

fn value_for(i: usize) -> Vec<u8> {
    format!("value-{i:04}").into_bytes()
}

#[test]
fn end_to_end_insert_read_delete_verify() {
    let functions = CrudSessionFunctions;
    let mut store = TsavoriteKV::<Vec<u8>, Vec<u8>>::new(test_config()).unwrap();
    let count = 128usize;

    {
        let mut session = store.session(&functions);
        for i in 0..count {
            let key = key_for(i);
            let value = value_for(i);
            let mut output = Vec::new();
            let mut upsert_info = UpsertInfo::default();
            session
                .upsert(&key, &value, &mut output, &mut upsert_info)
                .unwrap();
        }
    }

    {
        let mut session = store.session(&functions);
        for i in 0..count {
            let key = key_for(i);
            let mut output = Vec::new();
            let status = session
                .read(&key, &Vec::new(), &mut output, &ReadInfo::default())
                .unwrap();
            assert_eq!(status, ReadOperationStatus::FoundInMemory);
            assert_eq!(output, value_for(i));
        }
    }

    {
        let mut session = store.session(&functions);
        for i in 0..count {
            if i % 2 == 0 {
                let key = key_for(i);
                let mut delete_info = DeleteInfo::default();
                let status = session.delete(&key, &mut delete_info).unwrap();
                assert!(matches!(
                    status,
                    DeleteOperationStatus::TombstonedInPlace
                        | DeleteOperationStatus::AppendedTombstone
                ));
            }
        }
    }

    {
        let mut session = store.session(&functions);
        for i in 0..count {
            let key = key_for(i);
            let mut output = Vec::new();
            let status = session
                .read(&key, &Vec::new(), &mut output, &ReadInfo::default())
                .unwrap();
            if i % 2 == 0 {
                assert_eq!(status, ReadOperationStatus::NotFound);
            } else {
                assert_eq!(status, ReadOperationStatus::FoundInMemory);
                assert_eq!(output, value_for(i));
            }
        }
    }
}

#[test]
fn end_to_end_concurrent_insert_read_delete_verify() {
    let store = Arc::new(Mutex::new(
        TsavoriteKV::<Vec<u8>, Vec<u8>>::new(test_config()).unwrap(),
    ));
    let functions = Arc::new(CrudSessionFunctions);
    let count = 128usize;
    let workers = 4usize;
    let chunk = count / workers;

    let mut handles = Vec::new();
    for worker in 0..workers {
        let shared_store = Arc::clone(&store);
        let shared_functions = Arc::clone(&functions);
        handles.push(thread::spawn(move || {
            let start = worker * chunk;
            let end = start + chunk;
            for i in start..end {
                let key = key_for(i);
                let value = value_for(i);
                let mut output = Vec::new();
                let mut upsert_info = UpsertInfo::default();
                let mut guard = shared_store.lock().expect("store mutex poisoned");
                let mut session = guard.session(shared_functions.as_ref());
                session
                    .upsert(&key, &value, &mut output, &mut upsert_info)
                    .unwrap();
            }
        }));
    }
    for handle in handles {
        handle.join().expect("insert worker panicked");
    }

    let mut read_handles = Vec::new();
    for worker in 0..workers {
        let shared_store = Arc::clone(&store);
        let shared_functions = Arc::clone(&functions);
        read_handles.push(thread::spawn(move || {
            let start = worker * chunk;
            let end = start + chunk;
            for i in start..end {
                let key = key_for(i);
                let mut output = Vec::new();
                let mut guard = shared_store.lock().expect("store mutex poisoned");
                let mut session = guard.session(shared_functions.as_ref());
                let status = session
                    .read(&key, &Vec::new(), &mut output, &ReadInfo::default())
                    .unwrap();
                assert_eq!(status, ReadOperationStatus::FoundInMemory);
                assert_eq!(output, value_for(i));
            }
        }));
    }
    for handle in read_handles {
        handle.join().expect("read worker panicked");
    }

    let mut delete_handles = Vec::new();
    for worker in 0..workers {
        let shared_store = Arc::clone(&store);
        let shared_functions = Arc::clone(&functions);
        delete_handles.push(thread::spawn(move || {
            let start = worker * chunk;
            let end = start + chunk;
            for i in start..end {
                if i % 2 == 0 {
                    let key = key_for(i);
                    let mut delete_info = DeleteInfo::default();
                    let mut guard = shared_store.lock().expect("store mutex poisoned");
                    let mut session = guard.session(shared_functions.as_ref());
                    let status = session.delete(&key, &mut delete_info).unwrap();
                    assert!(matches!(
                        status,
                        DeleteOperationStatus::TombstonedInPlace
                            | DeleteOperationStatus::AppendedTombstone
                    ));
                }
            }
        }));
    }
    for handle in delete_handles {
        handle.join().expect("delete worker panicked");
    }

    for i in 0..count {
        let key = key_for(i);
        let mut output = Vec::new();
        let mut guard = store.lock().expect("store mutex poisoned");
        let mut session = guard.session(functions.as_ref());
        let status = session
            .read(&key, &Vec::new(), &mut output, &ReadInfo::default())
            .unwrap();
        if i % 2 == 0 {
            assert_eq!(status, ReadOperationStatus::NotFound);
        } else {
            assert_eq!(status, ReadOperationStatus::FoundInMemory);
            assert_eq!(output, value_for(i));
        }
    }
}
