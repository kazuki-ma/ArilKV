//! Compact pointer+length argument reference used by RESP command paths.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArgSliceError {
    LengthOverflow { length: usize },
}

impl core::fmt::Display for ArgSliceError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::LengthOverflow { length } => write!(
                f,
                "argument length {} exceeds ArgSlice i32 length capacity",
                length
            ),
        }
    }
}

impl std::error::Error for ArgSliceError {}

/// 12-byte compact pointer+length argument reference.
///
/// `as_slice` is unsafe because caller must ensure the backing bytes outlive
/// the returned borrow.
#[repr(C, packed)]
#[derive(Clone, Copy, Default)]
pub struct ArgSlice {
    ptr: u64,
    length: i32,
}

impl ArgSlice {
    pub const EMPTY: Self = Self { ptr: 0, length: 0 };

    pub fn from_slice(slice: &[u8]) -> Result<Self, ArgSliceError> {
        if slice.len() > i32::MAX as usize {
            return Err(ArgSliceError::LengthOverflow {
                length: slice.len(),
            });
        }
        Ok(Self {
            ptr: slice.as_ptr() as usize as u64,
            length: slice.len() as i32,
        })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.length as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr as usize as *const u8
    }

    /// # Safety
    ///
    /// Caller must ensure that the pointer and length remain valid for `'a`.
    pub unsafe fn as_slice<'a>(&self) -> &'a [u8] {
        if self.length <= 0 {
            return &[];
        }
        let ptr = self.as_ptr();
        let len = self.len();
        // SAFETY: contract is upheld by caller.
        unsafe { core::slice::from_raw_parts(ptr, len) }
    }
}

impl core::fmt::Debug for ArgSlice {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ArgSlice")
            .field("ptr", &(self.as_ptr() as usize))
            .field("length", &self.len())
            .finish()
    }
}

impl PartialEq for ArgSlice {
    fn eq(&self, other: &Self) -> bool {
        self.as_ptr() == other.as_ptr() && self.len() == other.len()
    }
}

impl Eq for ArgSlice {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arg_slice_is_12_bytes() {
        assert_eq!(core::mem::size_of::<ArgSlice>(), 12);
    }

    #[test]
    fn from_slice_roundtrip_pointer_and_length() {
        let bytes = b"hello";
        let arg = ArgSlice::from_slice(bytes).unwrap();
        assert_eq!(arg.len(), 5);
        assert!(!arg.is_empty());

        // SAFETY: `bytes` lives for this scope and backs `arg`.
        let view = unsafe { arg.as_slice() };
        assert_eq!(view, bytes);
    }

    #[test]
    fn empty_slice_maps_to_empty_view() {
        let arg = ArgSlice::from_slice(&[]).unwrap();
        assert_eq!(arg.len(), 0);
        assert!(arg.is_empty());
        // SAFETY: empty slice is always valid.
        assert_eq!(unsafe { arg.as_slice() }, &[]);
    }
}
