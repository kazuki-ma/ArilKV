//! Page I/O abstractions for hybrid-log pages.
//!
//! See [Doc 02 Section 2.6] for page flush/read pipeline context.

use crate::hybrid_log::PageManager;
use crate::hybrid_log::PageManagerError;
use std::collections::HashMap;
use std::sync::Mutex;

pub trait PageDevice: Send + Sync {
    fn write_async(&self, page_index: u64, source: &[u8]) -> Result<(), PageIoError>;
    fn read_async(&self, page_index: u64, destination: &mut [u8]) -> Result<(), PageIoError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageIoError {
    PageManager(PageManagerError),
    PageNotFoundOnDevice { page_index: u64 },
    InvalidPageSize { expected: usize, actual: usize },
}

impl core::fmt::Display for PageIoError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::PageManager(inner) => inner.fmt(f),
            Self::PageNotFoundOnDevice { page_index } => {
                write!(f, "page {} not found on device", page_index)
            }
            Self::InvalidPageSize { expected, actual } => {
                write!(
                    f,
                    "invalid page size: expected {}, got {}",
                    expected, actual
                )
            }
        }
    }
}

impl std::error::Error for PageIoError {}

impl From<PageManagerError> for PageIoError {
    fn from(value: PageManagerError) -> Self {
        Self::PageManager(value)
    }
}

/// In-memory test/stub device storing pages in a hash map.
#[derive(Debug)]
pub struct InMemoryPageDevice {
    page_size: usize,
    pages: Mutex<HashMap<u64, Vec<u8>>>,
}

impl InMemoryPageDevice {
    pub fn new(page_size: usize) -> Self {
        Self {
            page_size,
            pages: Mutex::new(HashMap::new()),
        }
    }

    pub fn page_count(&self) -> usize {
        self.pages.lock().expect("device lock poisoned").len()
    }
}

impl PageDevice for InMemoryPageDevice {
    fn write_async(&self, page_index: u64, source: &[u8]) -> Result<(), PageIoError> {
        if source.len() != self.page_size {
            return Err(PageIoError::InvalidPageSize {
                expected: self.page_size,
                actual: source.len(),
            });
        }

        self.pages
            .lock()
            .expect("device lock poisoned")
            .insert(page_index, source.to_vec());
        Ok(())
    }

    fn read_async(&self, page_index: u64, destination: &mut [u8]) -> Result<(), PageIoError> {
        if destination.len() != self.page_size {
            return Err(PageIoError::InvalidPageSize {
                expected: self.page_size,
                actual: destination.len(),
            });
        }

        let pages = self.pages.lock().expect("device lock poisoned");
        let source = pages
            .get(&page_index)
            .ok_or(PageIoError::PageNotFoundOnDevice { page_index })?;
        destination.copy_from_slice(source);
        Ok(())
    }
}

pub fn flush_page_to_device<D: PageDevice>(
    page_manager: &mut PageManager,
    device: &D,
    page_index: u64,
) -> Result<(), PageIoError> {
    let page = page_manager
        .get_page(page_index)
        .ok_or(PageManagerError::PageNotAllocated { page_index })?;

    device.write_async(page_index, page.bytes())?;
    page_manager.flush_page(page_index)?;
    Ok(())
}

pub fn load_page_from_device<D: PageDevice>(
    page_manager: &mut PageManager,
    device: &D,
    page_index: u64,
) -> Result<(), PageIoError> {
    if !page_manager.is_page_allocated(page_index) {
        page_manager.allocate_page(page_index)?;
    }

    let page = page_manager
        .get_page_mut(page_index)
        .ok_or(PageManagerError::PageNotAllocated { page_index })?;

    device.read_async(page_index, page.bytes_mut_for_load())?;
    page.mark_clean_loaded();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hybrid_log::LogicalAddress;

    #[test]
    fn flush_and_load_roundtrip() {
        let mut source_manager = PageManager::new(8, 2).unwrap();
        source_manager.allocate_page(0).unwrap();

        let addr = source_manager.address_space().encode(0, 4).unwrap();
        source_manager.write_at(addr, &[10, 11, 12, 13]).unwrap();

        let device = InMemoryPageDevice::new(source_manager.page_size());
        flush_page_to_device(&mut source_manager, &device, 0).unwrap();

        let mut target_manager = PageManager::new(8, 2).unwrap();
        load_page_from_device(&mut target_manager, &device, 0).unwrap();

        let loaded = target_manager
            .read_at(LogicalAddress(addr.raw()), 4)
            .unwrap();
        assert_eq!(loaded, &[10, 11, 12, 13]);
    }

    #[test]
    fn load_missing_page_returns_error() {
        let mut manager = PageManager::new(8, 1).unwrap();
        let device = InMemoryPageDevice::new(manager.page_size());

        let err = load_page_from_device(&mut manager, &device, 1).unwrap_err();
        assert!(matches!(err, PageIoError::PageNotFoundOnDevice { .. }));
    }

    #[test]
    fn flush_requires_allocated_page() {
        let mut manager = PageManager::new(8, 1).unwrap();
        let device = InMemoryPageDevice::new(manager.page_size());

        let err = flush_page_to_device(&mut manager, &device, 99).unwrap_err();
        assert!(matches!(err, PageIoError::PageManager(_)));
    }
}
