use std::fmt;
use std::io;

/// Error type returned by all [`bllist`](crate) operations.
#[derive(Debug)]
pub enum Error {
    /// An I/O error from the underlying [`bstack`] file.
    Io(io::Error),

    /// The stored CRC32 checksum does not match the recomputed checksum.
    ///
    /// This indicates the block at `block` was written partially (e.g. a
    /// crash mid-write) or that the file has been silently corrupted.
    ChecksumMismatch {
        /// Logical byte offset of the corrupt block within the BStack payload.
        block: u64,
    },

    /// The file structure is invalid or was written by an incompatible version.
    ///
    /// The inner string contains a human-readable description of the problem.
    Corruption(String),

    /// The [`BlockRef`](crate::BlockRef) does not point to a valid block
    /// boundary (offset too small, or not aligned to `BLOCK_SIZE`).
    InvalidBlock,

    /// `BLOCK_SIZE` ≤ 12: there is no room for a payload after the per-block
    /// header (4-byte checksum + 8-byte next pointer = 12 bytes of overhead).
    ///
    /// This variant is returned at runtime when the compile-time assertion
    /// could not fire (e.g. via a checked path). In practice the const
    /// assertion in [`FixedBlockList::PAYLOAD_CAPACITY`] rejects bad sizes
    /// at compile time.
    ///
    /// [`FixedBlockList::PAYLOAD_CAPACITY`]: crate::FixedBlockList::PAYLOAD_CAPACITY
    BlockTooSmall,

    /// The supplied data slice (for [`write`]) or buffer (for [`read_into`] /
    /// [`pop_front_into`]) exceeds the block's payload capacity.
    ///
    /// [`write`]: crate::FixedBlockList::write
    /// [`read_into`]: crate::FixedBlockList::read_into
    /// [`pop_front_into`]: crate::FixedBlockList::pop_front_into
    DataTooLarge {
        /// Maximum bytes the payload field can hold (`BLOCK_SIZE - 12`).
        capacity: usize,
        /// Number of bytes the caller supplied.
        provided: usize,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::ChecksumMismatch { block } => {
                write!(f, "CRC32 checksum mismatch on block at offset {block}")
            }
            Error::Corruption(msg) => write!(f, "file corruption: {msg}"),
            Error::InvalidBlock => {
                write!(f, "invalid block reference (wrong offset or alignment)")
            }
            Error::BlockTooSmall => write!(
                f,
                "BLOCK_SIZE must be greater than 12 \
                 (4-byte checksum + 8-byte next pointer = 12 bytes of overhead)"
            ),
            Error::DataTooLarge { capacity, provided } => write!(
                f,
                "data length {provided} exceeds block payload capacity {capacity}"
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl PartialEq for Error {
    /// Two `Error` values are equal when their discriminants and payloads match.
    ///
    /// `Io` variants are compared by [`io::ErrorKind`] because [`io::Error`]
    /// does not implement [`PartialEq`] itself.  Two `Io` errors with the same
    /// kind but different OS error codes compare equal under this definition.
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Error::Io(a), Error::Io(b)) => a.kind() == b.kind(),
            (Error::ChecksumMismatch { block: a }, Error::ChecksumMismatch { block: b }) => a == b,
            (Error::Corruption(a), Error::Corruption(b)) => a == b,
            (Error::InvalidBlock, Error::InvalidBlock) => true,
            (Error::BlockTooSmall, Error::BlockTooSmall) => true,
            (
                Error::DataTooLarge {
                    capacity: ca,
                    provided: pa,
                },
                Error::DataTooLarge {
                    capacity: cb,
                    provided: pb,
                },
            ) => ca == cb && pa == pb,
            _ => false,
        }
    }
}
