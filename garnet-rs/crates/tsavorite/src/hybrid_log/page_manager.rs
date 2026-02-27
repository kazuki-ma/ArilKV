//! Hybrid-log page management primitives.
//!
//! See [Doc 02] for logical address and page lifecycle model.

use std::collections::HashMap;
use std::collections::VecDeque;

/// Logical hybrid-log address encoded as `[page][offset]`.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct LogicalAddress(pub u64);

impl LogicalAddress {
    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

impl core::fmt::Display for LogicalAddress {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.0.fmt(f)
    }
}

/// Page/offset encoding configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageAddressSpace {
    page_size_bits: u8,
    page_size: usize,
    page_offset_mask: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodedAddress {
    pub page_index: u64,
    pub page_offset: u64,
}

impl PageAddressSpace {
    pub fn new(page_size_bits: u8) -> Result<Self, PageManagerError> {
        if !(1..=30).contains(&page_size_bits) {
            return Err(PageManagerError::InvalidPageSizeBits { page_size_bits });
        }

        let page_size = 1usize << page_size_bits;
        let page_offset_mask = (page_size as u64) - 1;

        Ok(Self {
            page_size_bits,
            page_size,
            page_offset_mask,
        })
    }

    #[inline]
    pub const fn page_size_bits(self) -> u8 {
        self.page_size_bits
    }

    #[inline]
    pub const fn page_size(self) -> usize {
        self.page_size
    }

    #[inline]
    pub const fn page_offset_mask(self) -> u64 {
        self.page_offset_mask
    }

    #[inline]
    pub fn encode(
        self,
        page_index: u64,
        page_offset: u64,
    ) -> Result<LogicalAddress, PageManagerError> {
        if page_offset >= self.page_size as u64 {
            return Err(PageManagerError::OffsetOutOfBounds {
                page_offset,
                page_size: self.page_size,
            });
        }

        Ok(LogicalAddress(
            (page_index << self.page_size_bits) | page_offset,
        ))
    }

    #[inline]
    pub fn decode(self, logical_address: LogicalAddress) -> DecodedAddress {
        let raw = logical_address.raw();
        DecodedAddress {
            page_index: raw >> self.page_size_bits,
            page_offset: raw & self.page_offset_mask,
        }
    }

    #[inline]
    pub fn page_index(self, logical_address: LogicalAddress) -> u64 {
        logical_address.raw() >> self.page_size_bits
    }

    #[inline]
    pub fn page_offset(self, logical_address: LogicalAddress) -> u64 {
        logical_address.raw() & self.page_offset_mask
    }
}

/// In-memory page in hybrid-log circular buffer.
#[derive(Debug)]
pub struct Page {
    page_index: u64,
    bytes: Box<[u8]>,
    dirty: bool,
    flushed: bool,
}

impl Page {
    fn new(page_index: u64, page_size: usize) -> Self {
        Self {
            page_index,
            bytes: vec![0; page_size].into_boxed_slice(),
            dirty: false,
            flushed: false,
        }
    }

    #[inline]
    pub const fn page_index(&self) -> u64 {
        self.page_index
    }

    #[inline]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[inline]
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        self.dirty = true;
        self.flushed = false;
        &mut self.bytes
    }

    #[inline]
    pub(crate) fn bytes_mut_for_load(&mut self) -> &mut [u8] {
        &mut self.bytes
    }

    #[inline]
    pub const fn is_dirty(&self) -> bool {
        self.dirty
    }

    #[inline]
    pub const fn is_flushed(&self) -> bool {
        self.flushed
    }

    #[inline]
    fn mark_flushed(&mut self) {
        self.flushed = true;
        self.dirty = false;
    }

    #[inline]
    pub(crate) fn mark_clean_loaded(&mut self) {
        self.flushed = true;
        self.dirty = false;
    }
}

/// Page lifecycle manager for the hybrid log.
#[derive(Debug)]
pub struct PageManager {
    address_space: PageAddressSpace,
    max_in_memory_pages: usize,
    pages: HashMap<u64, Page>,
    page_order: VecDeque<u64>,
}

impl PageManager {
    pub fn new(page_size_bits: u8, max_in_memory_pages: usize) -> Result<Self, PageManagerError> {
        if max_in_memory_pages == 0 {
            return Err(PageManagerError::InvalidBufferSize {
                max_in_memory_pages,
            });
        }

        Ok(Self {
            address_space: PageAddressSpace::new(page_size_bits)?,
            max_in_memory_pages,
            // Keep startup allocation bounded even when callers set very large max pages.
            pages: HashMap::with_capacity(max_in_memory_pages.min(4096)),
            page_order: VecDeque::with_capacity(max_in_memory_pages.min(4096)),
        })
    }

    #[inline]
    pub const fn address_space(&self) -> PageAddressSpace {
        self.address_space
    }

    #[inline]
    pub fn page_size(&self) -> usize {
        self.address_space.page_size()
    }

    #[inline]
    pub fn page_count(&self) -> usize {
        self.pages.len()
    }

    #[inline]
    pub fn is_page_allocated(&self, page_index: u64) -> bool {
        self.pages.contains_key(&page_index)
    }

    #[inline]
    pub fn oldest_page_index(&self) -> Option<u64> {
        self.page_order.front().copied()
    }

    #[inline]
    pub fn allocated_page_indices(&self) -> Vec<u64> {
        self.page_order.iter().copied().collect()
    }

    pub fn allocate_page(&mut self, page_index: u64) -> Result<(), PageManagerError> {
        if self.pages.contains_key(&page_index) {
            return Err(PageManagerError::PageAlreadyAllocated { page_index });
        }

        if self.pages.len() >= self.max_in_memory_pages {
            return Err(PageManagerError::BufferFull {
                max_in_memory_pages: self.max_in_memory_pages,
            });
        }

        let page = Page::new(page_index, self.page_size());
        self.pages.insert(page_index, page);
        self.page_order.push_back(page_index);
        Ok(())
    }

    pub fn get_page(&self, page_index: u64) -> Option<&Page> {
        self.pages.get(&page_index)
    }

    pub fn get_page_mut(&mut self, page_index: u64) -> Option<&mut Page> {
        self.pages.get_mut(&page_index)
    }

    pub fn flush_page(&mut self, page_index: u64) -> Result<(), PageManagerError> {
        let page = self
            .pages
            .get_mut(&page_index)
            .ok_or(PageManagerError::PageNotAllocated { page_index })?;
        page.mark_flushed();
        Ok(())
    }

    pub fn evict_page(&mut self, page_index: u64) -> Result<Page, PageManagerError> {
        let page = self
            .pages
            .get(&page_index)
            .ok_or(PageManagerError::PageNotAllocated { page_index })?;

        if !page.is_flushed() {
            return Err(PageManagerError::PageNotFlushed { page_index });
        }

        self.page_order.retain(|idx| *idx != page_index);
        self.pages
            .remove(&page_index)
            .ok_or(PageManagerError::PageNotAllocated { page_index })
    }

    pub fn write_at(
        &mut self,
        address: LogicalAddress,
        data: &[u8],
    ) -> Result<(), PageManagerError> {
        let page_index = self.address_space.page_index(address);
        let page_offset = self.address_space.page_offset(address) as usize;

        let page_size = self.page_size();
        if page_offset + data.len() > page_size {
            return Err(PageManagerError::AddressOutOfBounds {
                page_offset,
                length: data.len(),
                page_size,
            });
        }

        let page = self
            .pages
            .get_mut(&page_index)
            .ok_or(PageManagerError::PageNotAllocated { page_index })?;

        page.bytes_mut()[page_offset..page_offset + data.len()].copy_from_slice(data);
        Ok(())
    }

    pub fn read_at(
        &self,
        address: LogicalAddress,
        length: usize,
    ) -> Result<&[u8], PageManagerError> {
        let page_index = self.address_space.page_index(address);
        let page_offset = self.address_space.page_offset(address) as usize;
        let page_size = self.page_size();

        if page_offset + length > page_size {
            return Err(PageManagerError::AddressOutOfBounds {
                page_offset,
                length,
                page_size,
            });
        }

        let page = self
            .pages
            .get(&page_index)
            .ok_or(PageManagerError::PageNotAllocated { page_index })?;

        Ok(&page.bytes()[page_offset..page_offset + length])
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageManagerError {
    InvalidPageSizeBits {
        page_size_bits: u8,
    },
    InvalidBufferSize {
        max_in_memory_pages: usize,
    },
    OffsetOutOfBounds {
        page_offset: u64,
        page_size: usize,
    },
    AddressOutOfBounds {
        page_offset: usize,
        length: usize,
        page_size: usize,
    },
    BufferFull {
        max_in_memory_pages: usize,
    },
    PageAlreadyAllocated {
        page_index: u64,
    },
    PageNotAllocated {
        page_index: u64,
    },
    PageNotFlushed {
        page_index: u64,
    },
}

impl core::fmt::Display for PageManagerError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::InvalidPageSizeBits { page_size_bits } => {
                write!(
                    f,
                    "invalid page_size_bits {} (expected 1..=30)",
                    page_size_bits
                )
            }
            Self::InvalidBufferSize {
                max_in_memory_pages,
            } => {
                write!(
                    f,
                    "invalid max_in_memory_pages {} (expected > 0)",
                    max_in_memory_pages
                )
            }
            Self::OffsetOutOfBounds {
                page_offset,
                page_size,
            } => {
                write!(
                    f,
                    "page offset {} is out of bounds for page_size {}",
                    page_offset, page_size
                )
            }
            Self::AddressOutOfBounds {
                page_offset,
                length,
                page_size,
            } => {
                write!(
                    f,
                    "range [{}, {}) is out of bounds for page_size {}",
                    page_offset,
                    page_offset + length,
                    page_size
                )
            }
            Self::BufferFull {
                max_in_memory_pages,
            } => {
                write!(f, "page buffer full (max {})", max_in_memory_pages)
            }
            Self::PageAlreadyAllocated { page_index } => {
                write!(f, "page {} is already allocated", page_index)
            }
            Self::PageNotAllocated { page_index } => {
                write!(f, "page {} is not allocated", page_index)
            }
            Self::PageNotFlushed { page_index } => {
                write!(f, "page {} must be flushed before eviction", page_index)
            }
        }
    }
}

impl std::error::Error for PageManagerError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_address_encode_decode_roundtrip() {
        let space = PageAddressSpace::new(12).unwrap();
        let address = space.encode(42, 123).unwrap();

        assert_eq!(address.raw(), (42 << 12) | 123);
        assert_eq!(
            space.decode(address),
            DecodedAddress {
                page_index: 42,
                page_offset: 123,
            }
        );
    }

    #[test]
    fn page_lifecycle_allocate_flush_evict() {
        let mut manager = PageManager::new(8, 2).unwrap();
        manager.allocate_page(1).unwrap();

        let addr = manager.address_space().encode(1, 3).unwrap();
        manager.write_at(addr, &[7, 8, 9]).unwrap();
        assert_eq!(manager.read_at(addr, 3).unwrap(), &[7, 8, 9]);

        assert!(matches!(
            manager.evict_page(1),
            Err(PageManagerError::PageNotFlushed { page_index: 1 })
        ));

        manager.flush_page(1).unwrap();
        let page = manager.evict_page(1).unwrap();
        assert_eq!(page.page_index(), 1);
        assert_eq!(manager.page_count(), 0);
    }

    #[test]
    fn enforces_buffer_limit() {
        let mut manager = PageManager::new(8, 1).unwrap();
        manager.allocate_page(10).unwrap();

        let err = manager.allocate_page(11).unwrap_err();
        assert_eq!(
            err,
            PageManagerError::BufferFull {
                max_in_memory_pages: 1,
            }
        );
    }

    #[test]
    fn reject_out_of_page_access() {
        let mut manager = PageManager::new(4, 1).unwrap();
        manager.allocate_page(0).unwrap();

        let addr = manager.address_space().encode(0, 15).unwrap();
        let err = manager.write_at(addr, &[1, 2]).unwrap_err();
        assert!(matches!(err, PageManagerError::AddressOutOfBounds { .. }));
    }
}
