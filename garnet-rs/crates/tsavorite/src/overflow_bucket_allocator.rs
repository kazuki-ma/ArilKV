//! Fixed-page allocator for overflow hash buckets.
//!
//! See [Doc 03 Section 2.7] for overflow bucket allocation semantics.

use crate::hash_bucket::HashBucket;
use core::sync::atomic::AtomicU64;
use core::sync::atomic::Ordering;
use crossbeam_queue::SegQueue;
use std::sync::Arc;
use std::sync::RwLock;

pub const OVERFLOW_PAGE_SIZE_BITS: u8 = 16;
pub const OVERFLOW_PAGE_SIZE: usize = 1 << OVERFLOW_PAGE_SIZE_BITS;

#[derive(Clone)]
pub struct OverflowBucketHandle {
    logical_address: u64,
    page: Arc<[HashBucket]>,
    slot: usize,
}

impl OverflowBucketHandle {
    #[inline]
    pub const fn logical_address(&self) -> u64 {
        self.logical_address
    }

    #[inline]
    pub fn bucket(&self) -> &HashBucket {
        &self.page[self.slot]
    }

    #[inline]
    pub fn as_ptr(&self) -> *const HashBucket {
        self.bucket() as *const HashBucket
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowBucketAllocatorError {
    InvalidLogicalAddress { logical_address: u64 },
    LogicalAddressOutOfRange { logical_address: u64 },
}

impl core::fmt::Display for OverflowBucketAllocatorError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidLogicalAddress { logical_address } => {
                write!(f, "invalid overflow logical address {}", logical_address)
            }
            Self::LogicalAddressOutOfRange { logical_address } => {
                write!(
                    f,
                    "overflow logical address {} is out of range",
                    logical_address
                )
            }
        }
    }
}

impl std::error::Error for OverflowBucketAllocatorError {}

/// Fixed-page overflow allocator.
///
/// Logical address 0 is reserved as "no overflow bucket".
pub struct OverflowBucketAllocator {
    pages: RwLock<Vec<Arc<[HashBucket]>>>,
    free_list: SegQueue<u64>,
    next_logical_address: AtomicU64,
}

impl OverflowBucketAllocator {
    pub fn new() -> Self {
        Self {
            pages: RwLock::new(Vec::new()),
            free_list: SegQueue::new(),
            next_logical_address: AtomicU64::new(1),
        }
    }

    pub fn allocate(&self) -> Result<u64, OverflowBucketAllocatorError> {
        if let Some(logical_address) = self.free_list.pop() {
            self.reset_bucket(logical_address)?;
            return Ok(logical_address);
        }

        let logical_address = self.next_logical_address.fetch_add(1, Ordering::AcqRel);
        self.ensure_page_for_address(logical_address)?;
        Ok(logical_address)
    }

    pub fn free(&self, logical_address: u64) -> Result<(), OverflowBucketAllocatorError> {
        self.reset_bucket(logical_address)?;
        self.free_list.push(logical_address);
        Ok(())
    }

    pub fn get(
        &self,
        logical_address: u64,
    ) -> Result<OverflowBucketHandle, OverflowBucketAllocatorError> {
        let (page_index, slot) = logical_address_to_page_slot(logical_address)?;
        let page = {
            let pages = self.pages.read().expect("overflow page lock poisoned");
            pages
                .get(page_index)
                .cloned()
                .ok_or(OverflowBucketAllocatorError::InvalidLogicalAddress { logical_address })?
        };

        Ok(OverflowBucketHandle {
            logical_address,
            page,
            slot,
        })
    }

    #[inline]
    pub fn page_count(&self) -> usize {
        self.pages
            .read()
            .expect("overflow page lock poisoned")
            .len()
    }

    #[inline]
    pub fn free_list_len(&self) -> usize {
        self.free_list.len()
    }

    fn reset_bucket(&self, logical_address: u64) -> Result<(), OverflowBucketAllocatorError> {
        let handle = self.get(logical_address)?;
        handle.bucket().clear(Ordering::Release);
        Ok(())
    }

    fn ensure_page_for_address(
        &self,
        logical_address: u64,
    ) -> Result<(), OverflowBucketAllocatorError> {
        let (page_index, _) = logical_address_to_page_slot(logical_address)?;
        {
            let pages = self.pages.read().expect("overflow page lock poisoned");
            if page_index < pages.len() {
                return Ok(());
            }
        }

        let mut pages = self.pages.write().expect("overflow page lock poisoned");
        while page_index >= pages.len() {
            pages.push(new_overflow_page());
        }

        Ok(())
    }
}

impl Default for OverflowBucketAllocator {
    fn default() -> Self {
        Self::new()
    }
}

fn logical_address_to_page_slot(
    logical_address: u64,
) -> Result<(usize, usize), OverflowBucketAllocatorError> {
    if logical_address == 0 {
        return Err(OverflowBucketAllocatorError::InvalidLogicalAddress { logical_address });
    }

    let logical_usize = usize::try_from(logical_address)
        .map_err(|_| OverflowBucketAllocatorError::LogicalAddressOutOfRange { logical_address })?;
    let page_index = logical_usize >> OVERFLOW_PAGE_SIZE_BITS;
    let slot = logical_usize & (OVERFLOW_PAGE_SIZE - 1);
    Ok((page_index, slot))
}

fn new_overflow_page() -> Arc<[HashBucket]> {
    let mut buckets = Vec::with_capacity(OVERFLOW_PAGE_SIZE);
    buckets.resize_with(OVERFLOW_PAGE_SIZE, HashBucket::default);
    Arc::from(buckets.into_boxed_slice())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HashBucketEntry;
    use core::sync::atomic::Ordering;

    #[test]
    fn allocate_returns_non_zero_addresses() {
        let allocator = OverflowBucketAllocator::new();
        let first = allocator.allocate().unwrap();
        let second = allocator.allocate().unwrap();

        assert!(first > 0);
        assert!(second > first);
        assert!(allocator.page_count() >= 1);
    }

    #[test]
    fn free_reuses_same_address() {
        let allocator = OverflowBucketAllocator::new();
        let addr = allocator.allocate().unwrap();
        allocator.free(addr).unwrap();
        let reused = allocator.allocate().unwrap();

        assert_eq!(addr, reused);
    }

    #[test]
    fn free_resets_bucket_contents() {
        let allocator = OverflowBucketAllocator::new();
        let addr = allocator.allocate().unwrap();

        let handle = allocator.get(addr).unwrap();
        let entry_word =
            HashBucketEntry::pack(12, crate::LogicalAddress(345), false, false).unwrap();
        handle
            .bucket()
            .entry(0)
            .unwrap()
            .store(entry_word, Ordering::Release);

        allocator.free(addr).unwrap();
        let reused = allocator.allocate().unwrap();
        assert_eq!(addr, reused);

        let reused_handle = allocator.get(reused).unwrap();
        assert!(
            reused_handle
                .bucket()
                .entry(0)
                .unwrap()
                .is_empty(Ordering::Acquire)
        );
        assert_eq!(
            reused_handle.bucket().overflow_address(Ordering::Acquire),
            0
        );
    }

    #[test]
    fn rejects_zero_address() {
        let allocator = OverflowBucketAllocator::new();
        assert!(matches!(
            allocator.get(0),
            Err(OverflowBucketAllocatorError::InvalidLogicalAddress { logical_address: 0 })
        ));
    }
}
