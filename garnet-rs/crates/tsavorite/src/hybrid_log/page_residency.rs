//! Page eviction and disk-read fallback helpers for the hybrid log.
//!
//! See [Doc 02 Sections 2.6-2.7] for flush/eviction and on-disk read path semantics.

use crate::hybrid_log::{
    LogAddressPointers, LogicalAddress, PageDevice, PageIoError, PageManager, PageManagerError,
    flush_page_to_device, load_page_from_device,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadPathStatus {
    InMemory,
    LoadedFromDisk,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageResidencyError {
    PageManager(PageManagerError),
    PageIo(PageIoError),
    NonMonotonicHeadShift {
        current_head_address: u64,
        new_head_address: u64,
    },
    NoEvictablePage {
        requested_page_index: u64,
    },
}

impl core::fmt::Display for PageResidencyError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::PageManager(inner) => inner.fmt(f),
            Self::PageIo(inner) => inner.fmt(f),
            Self::NonMonotonicHeadShift {
                current_head_address,
                new_head_address,
            } => write!(
                f,
                "cannot move head backwards from {} to {}",
                current_head_address, new_head_address
            ),
            Self::NoEvictablePage {
                requested_page_index,
            } => write!(
                f,
                "no evictable page available while loading page {}",
                requested_page_index
            ),
        }
    }
}

impl std::error::Error for PageResidencyError {}

impl From<PageManagerError> for PageResidencyError {
    fn from(value: PageManagerError) -> Self {
        Self::PageManager(value)
    }
}

impl From<PageIoError> for PageResidencyError {
    fn from(value: PageIoError) -> Self {
        Self::PageIo(value)
    }
}

pub fn shift_head_address_and_evict<D: PageDevice>(
    page_manager: &mut PageManager,
    pointers: &LogAddressPointers,
    device: &D,
    new_head_address: u64,
) -> Result<Vec<u64>, PageResidencyError> {
    let current_head_address = pointers.head_address();
    if new_head_address < current_head_address {
        return Err(PageResidencyError::NonMonotonicHeadShift {
            current_head_address,
            new_head_address,
        });
    }

    pointers.shift_safe_head_address(new_head_address);

    let address_space = page_manager.address_space();
    let new_head_page = new_head_address >> address_space.page_size_bits();
    let allocated = page_manager.allocated_page_indices();
    let mut evicted = Vec::new();

    for page_index in allocated {
        if page_index >= new_head_page {
            continue;
        }

        evict_page_with_flush(page_manager, device, page_index)?;
        evicted.push(page_index);
    }

    pointers.shift_head_address(new_head_address);
    pointers.shift_safe_head_address(new_head_address);
    Ok(evicted)
}

pub fn read_with_callback<D, F>(
    page_manager: &mut PageManager,
    pointers: &LogAddressPointers,
    device: &D,
    address: LogicalAddress,
    length: usize,
    on_complete: F,
) -> Result<ReadPathStatus, PageResidencyError>
where
    D: PageDevice,
    F: FnOnce(&[u8]),
{
    let status = if address.raw() < pointers.head_address() {
        ensure_page_resident_for_read(page_manager, device, address)?;
        ReadPathStatus::LoadedFromDisk
    } else {
        ReadPathStatus::InMemory
    };

    let data = page_manager.read_at(address, length)?;
    on_complete(data);
    Ok(status)
}

fn ensure_page_resident_for_read<D: PageDevice>(
    page_manager: &mut PageManager,
    device: &D,
    address: LogicalAddress,
) -> Result<(), PageResidencyError> {
    let page_index = page_manager.address_space().page_index(address);
    if page_manager.is_page_allocated(page_index) {
        return Ok(());
    }

    loop {
        match page_manager.allocate_page(page_index) {
            Ok(()) => break,
            Err(PageManagerError::BufferFull { .. }) => {
                evict_one_page_for_load(page_manager, device, page_index)?;
            }
            Err(err) => return Err(PageResidencyError::PageManager(err)),
        }
    }

    load_page_from_device(page_manager, device, page_index)?;
    Ok(())
}

fn evict_one_page_for_load<D: PageDevice>(
    page_manager: &mut PageManager,
    device: &D,
    requested_page_index: u64,
) -> Result<(), PageResidencyError> {
    let candidate = page_manager
        .allocated_page_indices()
        .into_iter()
        .find(|index| *index != requested_page_index)
        .ok_or(PageResidencyError::NoEvictablePage {
            requested_page_index,
        })?;
    evict_page_with_flush(page_manager, device, candidate)
}

fn evict_page_with_flush<D: PageDevice>(
    page_manager: &mut PageManager,
    device: &D,
    page_index: u64,
) -> Result<(), PageResidencyError> {
    let page = page_manager
        .get_page(page_index)
        .ok_or(PageManagerError::PageNotAllocated { page_index })?;
    let should_flush = page.is_dirty() || !page.is_flushed();

    if should_flush {
        flush_page_to_device(page_manager, device, page_index)?;
    }

    page_manager.evict_page(page_index)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hybrid_log::InMemoryPageDevice;

    #[test]
    fn shift_head_evicts_pages_below_new_head_and_flushes_them() {
        let mut page_manager = PageManager::new(8, 4).unwrap();
        page_manager.allocate_page(0).unwrap();
        page_manager.allocate_page(1).unwrap();
        page_manager.allocate_page(2).unwrap();

        let addr0 = page_manager.address_space().encode(0, 0).unwrap();
        let addr1 = page_manager.address_space().encode(1, 8).unwrap();
        page_manager.write_at(addr0, &[1, 2, 3, 4]).unwrap();
        page_manager.write_at(addr1, &[5, 6, 7, 8]).unwrap();

        let pointers = LogAddressPointers::new(0);
        let device = InMemoryPageDevice::new(page_manager.page_size());

        let evicted =
            shift_head_address_and_evict(&mut page_manager, &pointers, &device, 2u64 << 8).unwrap();

        assert_eq!(evicted, vec![0, 1]);
        assert_eq!(page_manager.allocated_page_indices(), vec![2]);
        assert_eq!(pointers.head_address(), 2u64 << 8);
        assert_eq!(pointers.safe_head_address(), 2u64 << 8);
        assert_eq!(device.page_count(), 2);
    }

    #[test]
    fn shift_head_rejects_backwards_move() {
        let mut page_manager = PageManager::new(8, 2).unwrap();
        let pointers = LogAddressPointers::new(1u64 << 8);
        let device = InMemoryPageDevice::new(page_manager.page_size());

        let err =
            shift_head_address_and_evict(&mut page_manager, &pointers, &device, 64).unwrap_err();
        assert!(matches!(
            err,
            PageResidencyError::NonMonotonicHeadShift { .. }
        ));
    }

    #[test]
    fn read_with_callback_loads_page_from_disk_when_below_head() {
        let mut page_manager = PageManager::new(8, 2).unwrap();
        page_manager.allocate_page(0).unwrap();

        let address = page_manager.address_space().encode(0, 4).unwrap();
        page_manager.write_at(address, &[10, 11, 12, 13]).unwrap();

        let device = InMemoryPageDevice::new(page_manager.page_size());
        flush_page_to_device(&mut page_manager, &device, 0).unwrap();
        page_manager.evict_page(0).unwrap();

        let pointers = LogAddressPointers::new(0);
        pointers.shift_head_address(1u64 << 8);

        let mut captured = Vec::new();
        let status =
            read_with_callback(&mut page_manager, &pointers, &device, address, 4, |bytes| {
                captured.extend_from_slice(bytes)
            })
            .unwrap();

        assert_eq!(status, ReadPathStatus::LoadedFromDisk);
        assert_eq!(captured, vec![10, 11, 12, 13]);
        assert!(page_manager.is_page_allocated(0));
    }

    #[test]
    fn read_with_callback_evicts_oldest_page_when_buffer_is_full() {
        let mut page_manager = PageManager::new(8, 1).unwrap();
        page_manager.allocate_page(0).unwrap();

        let address = page_manager.address_space().encode(0, 2).unwrap();
        page_manager.write_at(address, &[21, 22, 23]).unwrap();

        let device = InMemoryPageDevice::new(page_manager.page_size());
        flush_page_to_device(&mut page_manager, &device, 0).unwrap();
        page_manager.evict_page(0).unwrap();

        page_manager.allocate_page(1).unwrap();
        let page1_addr = page_manager.address_space().encode(1, 0).unwrap();
        page_manager.write_at(page1_addr, &[1]).unwrap();

        let pointers = LogAddressPointers::new(0);
        pointers.shift_head_address(1u64 << 8);

        let mut captured = Vec::new();
        let status =
            read_with_callback(&mut page_manager, &pointers, &device, address, 3, |bytes| {
                captured.extend_from_slice(bytes)
            })
            .unwrap();

        assert_eq!(status, ReadPathStatus::LoadedFromDisk);
        assert_eq!(captured, vec![21, 22, 23]);
        assert!(page_manager.is_page_allocated(0));
        assert!(!page_manager.is_page_allocated(1));
    }

    #[test]
    fn read_with_callback_uses_in_memory_path_at_or_above_head() {
        let mut page_manager = PageManager::new(8, 2).unwrap();
        page_manager.allocate_page(1).unwrap();

        let address = page_manager.address_space().encode(1, 6).unwrap();
        page_manager.write_at(address, &[40, 41]).unwrap();

        let pointers = LogAddressPointers::new(0);
        pointers.shift_head_address(1u64 << 8);
        let device = InMemoryPageDevice::new(page_manager.page_size());

        let mut captured = Vec::new();
        let status =
            read_with_callback(&mut page_manager, &pointers, &device, address, 2, |bytes| {
                captured.extend_from_slice(bytes)
            })
            .unwrap();

        assert_eq!(status, ReadPathStatus::InMemory);
        assert_eq!(captured, vec![40, 41]);
    }
}
