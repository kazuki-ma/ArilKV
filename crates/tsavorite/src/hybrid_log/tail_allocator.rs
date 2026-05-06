//! Lock-free style tail allocator for hybrid-log appends.
//!
//! See [Doc 02 Section 2.5] for fetch-add allocation and page-turn retry behavior.

use crate::hybrid_log::LogAddressPointers;
use crate::hybrid_log::LogicalAddress;
use crate::hybrid_log::PageManager;
use crate::hybrid_log::PageManagerError;
use std::collections::HashSet;
use std::sync::Mutex;
use std::sync::TryLockError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TailAllocationStatus {
    Success(LogicalAddress),
    RetryNow,
    RetryLater,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TailAllocatorError {
    InvalidRecordSize,
    RecordTooLarge {
        record_size: usize,
        page_size: usize,
    },
    PageManager(PageManagerError),
}

impl core::fmt::Display for TailAllocatorError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidRecordSize => write!(f, "record_size must be > 0"),
            Self::RecordTooLarge {
                record_size,
                page_size,
            } => write!(
                f,
                "record_size {} exceeds page_size {}",
                record_size, page_size
            ),
            Self::PageManager(inner) => inner.fmt(f),
        }
    }
}

impl std::error::Error for TailAllocatorError {}

/// Tail allocator with fetch-add reservations and page-turn handling.
#[derive(Debug)]
pub struct TailAllocator {
    pointers: LogAddressPointers,
    page_manager: Mutex<PageManager>,
    page_size_bits: u8,
    page_size: usize,
    page_size_mask: u64,
    sealed_pages: Mutex<HashSet<u64>>,
    page_turn_lock: Mutex<()>,
}

impl TailAllocator {
    pub fn new(
        page_size_bits: u8,
        max_in_memory_pages: usize,
        begin_address: LogicalAddress,
    ) -> Result<Self, TailAllocatorError> {
        let mut page_manager = PageManager::new(page_size_bits, max_in_memory_pages)
            .map_err(TailAllocatorError::PageManager)?;

        let page_size = page_manager.page_size();
        let page_size_mask = (page_size as u64) - 1;

        let begin_page = begin_address.raw() >> page_size_bits;
        if !page_manager.is_page_allocated(begin_page) {
            page_manager
                .allocate_page(begin_page)
                .map_err(TailAllocatorError::PageManager)?;
        }

        Ok(Self {
            pointers: LogAddressPointers::new(begin_address),
            page_manager: Mutex::new(page_manager),
            page_size_bits,
            page_size,
            page_size_mask,
            sealed_pages: Mutex::new(HashSet::new()),
            page_turn_lock: Mutex::new(()),
        })
    }

    #[inline]
    pub fn pointers(&self) -> &LogAddressPointers {
        &self.pointers
    }

    #[inline]
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    #[inline]
    pub fn is_page_sealed(&self, page_index: u64) -> bool {
        self.sealed_pages
            .lock()
            .expect("sealed_pages lock poisoned")
            .contains(&page_index)
    }

    #[inline]
    pub fn tail_address(&self) -> LogicalAddress {
        self.pointers.tail_address()
    }

    pub fn try_allocate(
        &self,
        record_size: usize,
    ) -> Result<TailAllocationStatus, TailAllocatorError> {
        if record_size == 0 {
            return Err(TailAllocatorError::InvalidRecordSize);
        }
        if record_size > self.page_size {
            return Err(TailAllocatorError::RecordTooLarge {
                record_size,
                page_size: self.page_size,
            });
        }

        let old_tail = self.pointers.advance_tail_by(record_size as u64);
        let old_page = old_tail.raw() >> self.page_size_bits;
        let old_offset = old_tail.raw() & self.page_size_mask;
        let end_offset = old_offset + record_size as u64;

        if end_offset <= self.page_size as u64 {
            return self
                .ensure_page_allocated(old_page)
                .map(|status| status.unwrap_or(TailAllocationStatus::Success(old_tail)));
        }

        // This reservation crossed page boundary; only one thread should handle page-turn setup.
        if old_offset >= self.page_size as u64 {
            return Ok(TailAllocationStatus::RetryNow);
        }

        match self.page_turn_lock.try_lock() {
            Ok(_guard) => self.handle_page_turn(old_page, record_size),
            Err(TryLockError::WouldBlock) => Ok(TailAllocationStatus::RetryNow),
            Err(TryLockError::Poisoned(_)) => Ok(TailAllocationStatus::RetryNow),
        }
    }

    fn ensure_page_allocated(
        &self,
        page_index: u64,
    ) -> Result<Option<TailAllocationStatus>, TailAllocatorError> {
        let mut page_manager = self
            .page_manager
            .lock()
            .expect("page_manager lock poisoned");
        if page_manager.is_page_allocated(page_index) {
            return Ok(None);
        }

        match page_manager.allocate_page(page_index) {
            Ok(()) => Ok(None),
            Err(PageManagerError::BufferFull { .. }) => Ok(Some(TailAllocationStatus::RetryLater)),
            Err(err) => Err(TailAllocatorError::PageManager(err)),
        }
    }

    fn handle_page_turn(
        &self,
        old_page: u64,
        record_size: usize,
    ) -> Result<TailAllocationStatus, TailAllocatorError> {
        let next_page = old_page + 1;
        let next_page_start = next_page << self.page_size_bits;
        let desired_tail = LogicalAddress(next_page_start + record_size as u64);

        // If another thread already advanced tail beyond this point, retry from fresh state.
        if !self.pointers.shift_tail_address(desired_tail)
            && self.pointers.tail_address() > desired_tail
        {
            return Ok(TailAllocationStatus::RetryNow);
        }

        {
            let mut sealed_pages = self
                .sealed_pages
                .lock()
                .expect("sealed_pages lock poisoned");
            sealed_pages.insert(old_page);
        }

        let mut page_manager = self
            .page_manager
            .lock()
            .expect("page_manager lock poisoned");
        for page in [next_page, next_page + 1] {
            if page_manager.is_page_allocated(page) {
                continue;
            }

            if let Err(err) = page_manager.allocate_page(page) {
                return match err {
                    PageManagerError::BufferFull { .. } => Ok(TailAllocationStatus::RetryLater),
                    other => Err(TailAllocatorError::PageManager(other)),
                };
            }
        }

        Ok(TailAllocationStatus::Success(LogicalAddress(
            next_page_start,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn allocates_within_single_page() {
        let allocator = TailAllocator::new(8, 4, LogicalAddress(0)).unwrap();

        let first = allocator.try_allocate(16).unwrap();
        let second = allocator.try_allocate(32).unwrap();

        assert_eq!(first, TailAllocationStatus::Success(LogicalAddress(0)));
        assert_eq!(second, TailAllocationStatus::Success(LogicalAddress(16)));
    }

    #[test]
    fn page_turn_seals_old_page_and_allocates_next_page() {
        let allocator = TailAllocator::new(8, 4, LogicalAddress(0)).unwrap();

        assert_eq!(
            allocator.try_allocate(250).unwrap(),
            TailAllocationStatus::Success(LogicalAddress(0))
        );

        let status = allocator.try_allocate(16).unwrap();
        assert_eq!(status, TailAllocationStatus::Success(LogicalAddress(256)));
        assert!(allocator.is_page_sealed(0));
    }

    #[test]
    fn returns_retry_later_when_buffer_full_on_page_turn() {
        let allocator = TailAllocator::new(8, 1, LogicalAddress(0)).unwrap();

        assert_eq!(
            allocator.try_allocate(250).unwrap(),
            TailAllocationStatus::Success(LogicalAddress(0))
        );

        assert_eq!(
            allocator.try_allocate(16).unwrap(),
            TailAllocationStatus::RetryLater
        );
    }

    #[test]
    fn rejects_invalid_sizes() {
        let allocator = TailAllocator::new(8, 2, LogicalAddress(0)).unwrap();

        let err = allocator.try_allocate(0).unwrap_err();
        assert_eq!(err, TailAllocatorError::InvalidRecordSize);

        let err = allocator.try_allocate(1024).unwrap_err();
        assert!(matches!(err, TailAllocatorError::RecordTooLarge { .. }));
    }

    #[test]
    fn allocates_many_records_without_overlap() {
        let allocator = TailAllocator::new(8, 96, LogicalAddress(0)).unwrap();
        let record_size = 48usize;
        let mut addresses = Vec::new();

        while addresses.len() < 100 {
            match allocator.try_allocate(record_size).unwrap() {
                TailAllocationStatus::Success(address) => addresses.push(address.raw()),
                TailAllocationStatus::RetryNow => {}
                TailAllocationStatus::RetryLater => panic!("unexpected RetryLater"),
            }
        }

        let mut unique = addresses.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), addresses.len());

        for address in unique {
            let offset = address & ((allocator.page_size() as u64) - 1);
            assert!(offset + record_size as u64 <= allocator.page_size() as u64);
        }

        assert!(allocator.is_page_sealed(0));
    }

    #[test]
    fn concurrent_allocation_stress_produces_unique_addresses() {
        let allocator = Arc::new(TailAllocator::new(8, 256, LogicalAddress(0)).unwrap());
        let thread_count = 4usize;
        let per_thread_successes = 200usize;
        let record_size = 16usize;

        let mut handles = Vec::new();
        for _ in 0..thread_count {
            let allocator = Arc::clone(&allocator);
            handles.push(thread::spawn(move || {
                let mut addresses = Vec::with_capacity(per_thread_successes);
                let mut attempts = 0usize;
                while addresses.len() < per_thread_successes {
                    attempts += 1;
                    assert!(attempts < per_thread_successes * 200);

                    match allocator.try_allocate(record_size).unwrap() {
                        TailAllocationStatus::Success(address) => addresses.push(address.raw()),
                        TailAllocationStatus::RetryNow => thread::yield_now(),
                        TailAllocationStatus::RetryLater => panic!("unexpected RetryLater"),
                    }
                }
                addresses
            }));
        }

        let mut all_addresses = Vec::with_capacity(thread_count * per_thread_successes);
        for handle in handles {
            all_addresses.extend(handle.join().expect("allocator thread panicked"));
        }

        assert_eq!(all_addresses.len(), thread_count * per_thread_successes);

        let mut unique = all_addresses.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), all_addresses.len());

        for address in unique {
            let offset = address & ((allocator.page_size() as u64) - 1);
            assert!(offset + record_size as u64 <= allocator.page_size() as u64);
        }
    }
}
