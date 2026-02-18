use core::sync::atomic::Ordering;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tsavorite::{HashBucketEntry, LightEpoch, RecordInfo};

fn bench_record_info(c: &mut Criterion) {
    c.bench_function("record_info_set_dirty_and_modified", |b| {
        b.iter(|| {
            let mut info = RecordInfo::default();
            info.set_valid(true);
            info.set_dirty_and_modified();
            black_box(info.raw_word())
        })
    });
}

fn bench_hash_bucket_entry_cas(c: &mut Criterion) {
    c.bench_function("hash_bucket_entry_compare_exchange_word", |b| {
        let entry = HashBucketEntry::default();

        b.iter(|| {
            let current = entry.load(Ordering::Acquire);
            let next = HashBucketEntry::with_pending(
                current,
                !HashBucketEntry::pending_from_word(current),
            );
            let _ = entry.compare_exchange_word(current, next, Ordering::AcqRel, Ordering::Acquire);
            black_box(next)
        })
    });
}

fn bench_epoch_pin_unpin(c: &mut Criterion) {
    c.bench_function("light_epoch_pin_drop", |b| {
        let epoch = LightEpoch::with_table_size(128);

        b.iter(|| {
            let guard = epoch.pin();
            black_box(&guard);
        })
    });
}

criterion_group!(
    phase1_hotpath,
    bench_record_info,
    bench_hash_bucket_entry_cas,
    bench_epoch_pin_unpin
);
criterion_main!(phase1_hotpath);
