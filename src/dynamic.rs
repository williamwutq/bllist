//! Variable-size block linked-list allocator backed by a single BStack file.
//!
//! [`DynamicBlockList`] stores blocks of arbitrary size using a bin-based
//! free-list allocator.  Each block's **total on-disk footprint** (header +
//! payload) is a power of two, which enables splitting large free blocks to
//! satisfy smaller requests and coalescing adjacent free blocks on open.
//!
//! ## File layout
//!
//! ```text
//! ┌──────────────────────────┬───────────────────────────────────────────────┐
//! │  BStack header (16 B)    │  bllist-dynamic header (272 B, logical off 0) │
//! │  "BSTK" magic + clen     │  "BLLD" + version + root + bin_heads[32]      │
//! ├──────────────────────────┴───────────────────────────────────────────────┤
//! │  Block (total size = 2^k bytes)                                          │
//! │  checksum(4) │ next(8) │ block_size(4) │ data_len(4) │ payload(bs-20 B) │
//! ├──────────────────────────────────────────────────────────────────────────┤
//! │  Block …                                                                 │
//! └──────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! **bllist-dynamic header** (272 bytes at logical offset 0):
//! - `[0..4]`    — magic `"BLLD"`
//! - `[4..8]`    — version `u32 LE = 2`
//! - `[8..16]`   — root `u64 LE` (0 = empty)
//! - `[16..272]` — 32 × `u64 LE` bin free-list heads (bin *k* holds free
//!   blocks whose **total on-disk size** equals 2^k; 0 = empty bin)
//!
//! **Block on-disk layout** (`block_size` bytes total):
//! - `[0..4]`           — CRC32 of bytes `[4..block_size]`
//! - `[4..12]`          — next `u64 LE` (logical offset of next block; 0 = null)
//! - `[12..16]`         — `block_size` `u32 LE` (total bytes on disk; always a power
//!   of two, minimum [`MIN_BIN`] = 32)
//! - `[16..20]`         — `data_len` `u32 LE` (bytes written; ≤ `block_size − 20`)
//! - `[20..block_size]` — payload
//!
//! ## Bin allocator
//!
//! Bin *k* holds free blocks whose total on-disk size equals 2^*k* bytes.
//! The minimum usable bin is [`MIN_BIN`] = 5 (32 bytes total, 12 bytes payload).
//!
//! **Allocation** (`alloc(size)`):
//! 1. Compute *bs* = [`block_size_for(size)`](Self::block_size_for).
//! 2. *k* = log₂(*bs*).
//! 3. If bin *k* is non-empty: pop its head and return it.
//! 4. Search bins *k+1* … *k+[`MAX_SPLIT`]* for the first non-empty bin *m*.
//!    If found: pop from *m*, then split down to *k* by halving repeatedly —
//!    each split writes the upper half as a free block in bin *m−1*, *m−2*, …
//!    until the lower half reaches bin *k*.
//! 5. If no free block found within `MAX_SPLIT` levels: extend the file.
//!
//! **Deallocation** (`free(block)`):
//! 1. Read `block_size` from the block header.
//! 2. Zero the payload, `data_len = 0`, link into bin log₂(`block_size`).
//!
//! ## Coalescing on open
//!
//! [`DynamicBlockList::open`] scans the file sequentially and collects all
//! non-active blocks (both proper free blocks and orphans).  Adjacent free
//! blocks whose combined size is a power of two are merged into a single
//! block and linked into the appropriate bin.  All bin free-lists are
//! rebuilt from scratch so orphaned blocks are always reclaimed.
//!
//! ## Crash safety
//!
//! Every mutation flushes durably before returning.  The worst case of a
//! mid-operation crash is an orphaned block, reclaimed on the next open.
//! During coalescing a zeroed header is written first (clearing all bin
//! heads), so a crash mid-coalesce leaves all non-active blocks as orphans
//! that are recovered cleanly on the subsequent open.
//!
//! ## Cross-type safety
//!
//! `DynamicBlockList::open` rejects files with magic `"BLLS"` (fixed-list
//! files) and vice versa.  The two list types **cannot** share a file.

use std::collections::HashSet;
use std::fmt;
use std::path::Path;
use std::sync::Mutex;

use bstack::BStack;
use crc32fast::Hasher as CrcHasher;

use crate::Error;

// ── on-disk constants ────────────────────────────────────────────────────────

/// Magic bytes that identify a bllist-dynamic file.
///
/// Deliberately different from `b"BLLS"` (fixed-list magic) so that
/// [`DynamicBlockList::open`] rejects fixed-list files and `FixedBlockList::open`
/// rejects dynamic-list files.
pub const MAGIC: [u8; 4] = *b"BLLD";

/// On-disk format version stored in the dynamic-list header.
pub const VERSION: u32 = 2;

/// Size of the bllist-dynamic file header at logical offset 0 (bytes).
///
/// `4` (magic) + `4` (version) + `8` (root) + `32 × 8` (bin heads) = 272.
pub const HEADER_SIZE: u64 = 272;

/// Number of power-of-two free-list bins.
///
/// Bin *k* holds blocks whose total on-disk size equals 2^*k*.
pub const NUM_BINS: usize = 32;

/// Byte size of the per-block header: checksum(4) + next(8) + block_size(4) + data_len(4).
pub const BLOCK_HEADER_SIZE: usize = 20;

/// Index of the smallest usable bin.
///
/// 2^5 = 32 bytes: the minimum block size (20-byte header + 12-byte payload).
/// Bins 0–4 are never populated.
pub const MIN_BIN: usize = 5;

/// Maximum number of bin levels to search above the target bin before giving
/// up on splitting and extending the file instead.
///
/// A value of 3 means an allocation for bin *k* will consider splitting blocks
/// from bins *k+1*, *k+2*, and *k+3* before allocating fresh file space.
pub const MAX_SPLIT: usize = 3;

/// Maximum total on-disk size of a single block (2^31 bytes, bin 31).
const MAX_BLOCK_SIZE: usize = 1 << 31;

// ── helpers ───────────────────────────────────────────────────────────────────

/// Return the bin index for a power-of-two block size (i.e. log₂(block_size)).
#[inline]
fn bin_index(block_size: usize) -> usize {
    block_size.trailing_zeros() as usize
}

// ── DynHeader (in-memory mirror of the 272-byte on-disk header) ───────────────

struct DynHeader {
    root: u64,
    bin_heads: [u64; NUM_BINS],
}

impl DynHeader {
    fn from_bytes(buf: &[u8; 272]) -> Result<Self, Error> {
        if &buf[0..4] == b"BLLS" {
            return Err(Error::Corruption(
                "file was created by FixedBlockList; use FixedBlockList::open".into(),
            ));
        }
        if buf[0..4] != MAGIC {
            return Err(Error::Corruption(format!(
                "invalid magic: expected {:?} (\"BLLD\"), found {:?}",
                MAGIC,
                &buf[0..4]
            )));
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if version != VERSION {
            return Err(Error::Corruption(format!(
                "unsupported dynamic-list version {version}, expected {VERSION}"
            )));
        }
        let root = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let mut bin_heads = [0u64; NUM_BINS];
        for (k, bh) in bin_heads.iter_mut().enumerate() {
            let s = 16 + k * 8;
            *bh = u64::from_le_bytes(buf[s..s + 8].try_into().unwrap());
        }
        Ok(Self { root, bin_heads })
    }

    fn to_bytes(&self) -> [u8; 272] {
        let mut buf = [0u8; 272];
        buf[0..4].copy_from_slice(&MAGIC);
        buf[4..8].copy_from_slice(&VERSION.to_le_bytes());
        buf[8..16].copy_from_slice(&self.root.to_le_bytes());
        for (k, bh) in self.bin_heads.iter().enumerate() {
            let s = 16 + k * 8;
            buf[s..s + 8].copy_from_slice(&bh.to_le_bytes());
        }
        buf
    }
}

// ── DynBlockRef ───────────────────────────────────────────────────────────────

/// A handle to a block in a [`DynamicBlockList`], encoded as the block's
/// logical byte offset within the underlying BStack file.
///
/// `DynBlockRef` is `Copy` and cheap to store; treat it like a typed index.
/// Offset `0` is never a valid block (logical offset 0 is the file header)
/// and is used internally as a null / end-of-list sentinel.
///
/// `DynBlockRef` values from one [`DynamicBlockList`] must **not** be used
/// with a different list backed by a different file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DynBlockRef(pub u64);

impl fmt::Display for DynBlockRef {
    /// Formats the block reference as `@offset` (decimal).
    ///
    /// Use `{:x}` / `{:#x}` for hexadecimal output via [`LowerHex`](fmt::LowerHex).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "@{}", self.0)
    }
}

impl fmt::LowerHex for DynBlockRef {
    /// Formats the block offset in lower-case hexadecimal.
    ///
    /// Respects the `#` flag: `{:#x}` produces `@0x110`, `{:x}` produces `@110`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("@")?;
        fmt::LowerHex::fmt(&self.0, f)
    }
}

impl fmt::UpperHex for DynBlockRef {
    /// Formats the block offset in upper-case hexadecimal.
    ///
    /// Respects the `#` flag: `{:#X}` produces `@0x110`, `{:X}` produces `@110`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("@")?;
        fmt::UpperHex::fmt(&self.0, f)
    }
}

impl From<u64> for DynBlockRef {
    /// Create a `DynBlockRef` from a raw logical byte offset.
    ///
    /// No validation is performed; the offset is not checked against the file.
    /// Use [`DynamicBlockList::alloc`] to obtain a valid reference.
    fn from(offset: u64) -> Self {
        DynBlockRef(offset)
    }
}

impl From<DynBlockRef> for u64 {
    /// Extract the raw logical byte offset from a `DynBlockRef`.
    fn from(r: DynBlockRef) -> u64 {
        r.0
    }
}

// ── DynamicBlockList ──────────────────────────────────────────────────────────

/// A durable, crash-safe singly-linked list of variable-size blocks backed by
/// a single BStack file.
///
/// Each block stores an arbitrary payload.  The total on-disk footprint of a
/// block (20-byte header + payload) is always a power of two, which enables
/// splitting large free blocks and coalescing adjacent free blocks on open.
///
/// The file format uses magic bytes `"BLLD"`, which means a `DynamicBlockList`
/// file cannot be opened as a [`FixedBlockList`](crate::FixedBlockList) and
/// vice versa — the wrong `open` call returns [`Error::Corruption`](crate::Error)
/// immediately.
///
/// # Bin allocator
///
/// `DynamicBlockList` maintains **32 segregated free lists**.  Bin *k* holds
/// all free blocks whose **total on-disk size** equals exactly 2^*k* bytes:
///
/// | Bin | Total size | Payload capacity |
/// |-----|-----------|-----------------|
/// | 5   | 32 B      | 12 B            |
/// | 6   | 64 B      | 44 B            |
/// | 7   | 128 B     | 108 B           |
/// | 10  | 1 KiB     | 1004 B          |
/// | 20  | 1 MiB     | ~1 MiB          |
/// | 31  | 2 GiB     | ~2 GiB          |
///
/// Bins 0–4 are never populated (they would hold blocks smaller than the
/// 20-byte header).
///
/// **Allocation** (`alloc(size)`):
/// 1. Compute *bs* = [`block_size_for(size)`](Self::block_size_for).
/// 2. If bin log₂(*bs*) is non-empty: pop and return.
/// 3. Otherwise search up to [`MAX_SPLIT`] bins higher for a block to split.
/// 4. If nothing within `MAX_SPLIT` levels: extend the file.
///
/// **Deallocation** (`free(block)`):
/// Zero the payload and link into the bin matching the block's total size.
///
/// # Crash safety
///
/// Every mutation flushes durably before returning.  The worst case of a
/// mid-operation crash is one orphaned block — reclaimed on the next
/// [`open`](Self::open).  Coalescing on open uses a two-phase header write
/// so a crash mid-coalesce also produces only orphans.
///
/// # Example
///
/// ```no_run
/// use bllist::DynamicBlockList;
///
/// // The total on-disk block size is the next power of two ≥ (data + 20 header bytes).
/// // A 5-byte push occupies 32 bytes on disk (5+20=25 → 32, bin 5).
/// let list = DynamicBlockList::open("data.blld")?;
///
/// list.push_front(b"hello")?;           // 5 bytes  → 32-byte block (bin 5)
/// list.push_front(b"a longer record")?; // 15 bytes → 64-byte block (bin 6)
///
/// while let Some(data) = list.pop_front()? {
///     println!("{}", String::from_utf8_lossy(&data));
/// }
/// // prints "a longer record", then "hello"
/// # Ok::<(), bllist::Error>(())
/// ```
///
/// # Thread safety
///
/// `DynamicBlockList` is `Send + Sync`.  Header mutations (`alloc`, `free`,
/// `root`, `push_front`, `pop_front`, `pop_front_into`) are serialised
/// through an internal `Mutex`.  Block-only operations (`write`, `read`,
/// `read_into`, `set_next`, `get_next`, `capacity`, `data_len`) do not
/// acquire the mutex and are safe to call concurrently on **different**
/// blocks.  Concurrent access to the **same** block from multiple threads
/// requires external synchronisation.
pub struct DynamicBlockList {
    stack: BStack,
    mu: Mutex<()>,
}

// SAFETY: BStack wraps a raw file descriptor.  All concurrent file access goes
// through pread/pwrite (Unix) or ReadFile/WriteFile (Windows) at disjoint
// offsets, which is safe across threads.
unsafe impl Send for DynamicBlockList {}
unsafe impl Sync for DynamicBlockList {}

impl fmt::Debug for DynamicBlockList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamicBlockList")
            .field("num_bins", &NUM_BINS)
            .finish()
    }
}

impl fmt::Display for DynamicBlockList {
    /// Formats as `DynamicBlockList`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DynamicBlockList")
    }
}

impl DynamicBlockList {
    // ── constructor ───────────────────────────────────────────────────────────

    /// Open or create a `DynamicBlockList` backed by `path`.
    ///
    /// If the file does not exist it is created and initialised with a fresh
    /// header.  If it already exists the header magic and version are
    /// validated — files created by [`FixedBlockList`](crate::FixedBlockList)
    /// (magic `"BLLS"`) are **rejected** with [`Error::Corruption`], as are
    /// version-1 dynamic files (the on-disk format changed in version 2).
    ///
    /// After the header is validated, all free and orphaned blocks are
    /// collected via sequential scan, adjacent free blocks are coalesced where
    /// possible, and all bin free-lists are rebuilt from scratch.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] for any underlying I/O failure, or
    /// [`Error::Corruption`] if the header magic or version is wrong.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let stack = BStack::open(path)?;
        let total = stack.len()?;

        if total == 0 {
            let hdr = DynHeader {
                root: 0,
                bin_heads: [0u64; NUM_BINS],
            };
            let offset = stack.push(&hdr.to_bytes())?;
            debug_assert_eq!(
                offset, 0,
                "dynamic-list header must land at logical offset 0"
            );
            return Ok(Self {
                stack,
                mu: Mutex::new(()),
            });
        }

        if total < HEADER_SIZE {
            return Err(Error::Corruption(format!(
                "file payload is {total} bytes, too small for the {HEADER_SIZE}-byte \
                 dynamic-list header"
            )));
        }

        let mut hdr_buf = [0u8; 272];
        stack.get_into(0, &mut hdr_buf)?;
        let mut header = DynHeader::from_bytes(&hdr_buf)?;

        Self::recover_orphans(&stack, &mut header, total)?;

        Ok(Self {
            stack,
            mu: Mutex::new(()),
        })
    }

    // ── allocation ────────────────────────────────────────────────────────────

    /// Allocate a new block with at least `size` bytes of payload capacity.
    ///
    /// The actual on-disk footprint is [`block_size_for(size)`](Self::block_size_for)
    /// bytes; the payload capacity is `block_size - 20`.  If the appropriate
    /// bin's free list is non-empty the head of that list is returned.
    /// Otherwise a block from a larger bin is split (up to [`MAX_SPLIT`] levels
    /// above), or the file is extended if no suitable free block exists.
    ///
    /// The returned block has `data_len = 0`; call [`write`](Self::write) to
    /// store data.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DataTooLarge`] if `size` exceeds the maximum payload
    /// capacity, or [`Error::Io`] on failure.
    pub fn alloc(&self, size: usize) -> Result<DynBlockRef, Error> {
        let bs = Self::block_size_for(size);
        if bs > MAX_BLOCK_SIZE {
            return Err(Error::DataTooLarge {
                capacity: MAX_BLOCK_SIZE - BLOCK_HEADER_SIZE,
                provided: size,
            });
        }
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        self.alloc_locked(bs, &mut header)
    }

    /// Return a block to the free list for its bin.
    ///
    /// The block's payload is zeroed and `data_len` is set to 0 before it is
    /// linked into the bin matching its total on-disk size.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidBlock`] if `block` does not point to a valid
    /// block offset, or [`Error::Io`] on failure.
    pub fn free(&self, block: DynBlockRef) -> Result<(), Error> {
        self.validate_block_offset(block.0)?;
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        self.free_locked(block, &mut header)
    }

    // ── payload I/O ───────────────────────────────────────────────────────────

    /// Write `data` into `block`'s payload field.
    ///
    /// `data.len()` must be ≤ the block's payload capacity (`block_size − 20`).
    /// The block's `data_len` is updated to `data.len()`; bytes beyond
    /// `data.len()` in the payload field are guaranteed to be zero.  The block
    /// checksum is recomputed and the whole block is written atomically in a
    /// single [`bstack::BStack::set`] call.
    ///
    /// Does **not** acquire the header mutex; safe to call concurrently on
    /// different blocks.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DataTooLarge`] if `data.len() > payload capacity`,
    /// [`Error::InvalidBlock`] for a bad offset, or [`Error::Io`] on failure.
    pub fn write(&self, block: DynBlockRef, data: &[u8]) -> Result<(), Error> {
        self.validate_block_offset(block.0)?;
        // Read next(8) + block_size(4) — skip the 4-byte checksum at the front.
        let mut fields = [0u8; 12];
        self.stack.get_into(block.0 + 4, &mut fields)?;
        let next = u64::from_le_bytes(fields[0..8].try_into().unwrap());
        let block_size = u32::from_le_bytes(fields[8..12].try_into().unwrap()) as usize;
        let payload_cap = block_size.saturating_sub(BLOCK_HEADER_SIZE);
        if data.len() > payload_cap {
            return Err(Error::DataTooLarge {
                capacity: payload_cap,
                provided: data.len(),
            });
        }
        self.write_block_raw(block.0, block_size, next, data.len() as u32, data)
    }

    /// Read the payload of `block`, returning only the `data_len` bytes that
    /// were written (not the full capacity).
    ///
    /// The checksum is verified before returning.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ChecksumMismatch`], [`Error::InvalidBlock`], or
    /// [`Error::Io`].
    pub fn read(&self, block: DynBlockRef) -> Result<Vec<u8>, Error> {
        self.validate_block_offset(block.0)?;
        let (_, _, data) = Self::read_block_full_static(&self.stack, block.0)?;
        Ok(data)
    }

    /// Zero-copy variant of [`read`](Self::read).
    ///
    /// Fills `buf[0..data_len]` directly from the file. `buf.len()` must be
    /// ≥ `data_len`; if shorter the checksum verification will fail because
    /// the zero-padding tail cannot be reconstructed correctly.
    ///
    /// Returns `true` if data was present (`data_len > 0`), `false` if the
    /// block was empty.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ChecksumMismatch`] (including when `buf` is too small),
    /// [`Error::InvalidBlock`], or [`Error::Io`].
    pub fn read_into(&self, block: DynBlockRef, buf: &mut [u8]) -> Result<bool, Error> {
        self.validate_block_offset(block.0)?;

        let mut hdr = [0u8; BLOCK_HEADER_SIZE];
        self.stack.get_into(block.0, &mut hdr)?;
        let stored_crc = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
        let block_size = u32::from_le_bytes(hdr[12..16].try_into().unwrap()) as usize;
        let data_len = u32::from_le_bytes(hdr[16..20].try_into().unwrap()) as usize;
        let payload_cap = block_size.saturating_sub(BLOCK_HEADER_SIZE);

        if data_len > buf.len() {
            return Err(Error::ChecksumMismatch { block: block.0 });
        }

        if data_len > 0 {
            self.stack
                .get_into(block.0 + BLOCK_HEADER_SIZE as u64, &mut buf[0..data_len])?;
        }

        let mut hasher = CrcHasher::new();
        hasher.update(&hdr[4..]); // next(8) + block_size(4) + data_len(4)
        hasher.update(&buf[0..data_len]);
        if payload_cap > data_len {
            hasher.update(&vec![0u8; payload_cap - data_len]);
        }
        if hasher.finalize() != stored_crc {
            return Err(Error::ChecksumMismatch { block: block.0 });
        }

        Ok(data_len > 0)
    }

    // ── structural pointer operations ─────────────────────────────────────────

    /// Update the `next` pointer of `block`.
    ///
    /// Reads the full block, updates the next field, recomputes the CRC, and
    /// writes the whole block back in one [`bstack::BStack::set`] call.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidBlock`] or [`Error::Io`].
    pub fn set_next(&self, block: DynBlockRef, next: Option<DynBlockRef>) -> Result<(), Error> {
        self.validate_block_offset(block.0)?;
        let next_val = next.map(|r| r.0).unwrap_or(0u64);

        let mut bs_buf = [0u8; 4];
        self.stack.get_into(block.0 + 12, &mut bs_buf)?;
        let block_size = u32::from_le_bytes(bs_buf) as usize;

        let mut buf = vec![0u8; block_size];
        self.stack.get_into(block.0, &mut buf)?;
        buf[4..12].copy_from_slice(&next_val.to_le_bytes());
        let crc = crc32fast::hash(&buf[4..]);
        buf[0..4].copy_from_slice(&crc.to_le_bytes());
        self.stack.set(block.0, &buf)?;
        Ok(())
    }

    /// Read the `next` pointer of `block` without verifying the checksum.
    ///
    /// Useful for fast structural traversal.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidBlock`] or [`Error::Io`].
    pub fn get_next(&self, block: DynBlockRef) -> Result<Option<DynBlockRef>, Error> {
        self.validate_block_offset(block.0)?;
        let mut buf = [0u8; 8];
        self.stack.get_into(block.0 + 4, &mut buf)?;
        let next = u64::from_le_bytes(buf);
        Ok(if next == 0 {
            None
        } else {
            Some(DynBlockRef(next))
        })
    }

    // ── list head ─────────────────────────────────────────────────────────────

    /// Return the current head of the active list, or `None` if the list is
    /// empty.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] on failure.
    pub fn root(&self) -> Result<Option<DynBlockRef>, Error> {
        let _g = self.mu.lock().unwrap();
        let header = self.read_header_locked()?;
        Ok(if header.root == 0 {
            None
        } else {
            Some(DynBlockRef(header.root))
        })
    }

    // ── block metadata ────────────────────────────────────────────────────────

    /// Return the payload capacity of `block` in bytes (`block_size − 20`).
    ///
    /// May be larger than the bytes actually stored; use
    /// [`data_len`](Self::data_len) for the number of bytes written.
    ///
    /// Does not verify the checksum.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidBlock`] or [`Error::Io`].
    pub fn capacity(&self, block: DynBlockRef) -> Result<usize, Error> {
        self.validate_block_offset(block.0)?;
        let mut buf = [0u8; 4];
        self.stack.get_into(block.0 + 12, &mut buf)?;
        let block_size = u32::from_le_bytes(buf) as usize;
        Ok(block_size.saturating_sub(BLOCK_HEADER_SIZE))
    }

    /// Return the number of payload bytes currently stored in `block`.
    ///
    /// Reflects the last successful [`write`](Self::write).  At most
    /// [`capacity`](Self::capacity).
    ///
    /// Does not verify the checksum.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidBlock`] or [`Error::Io`].
    pub fn data_len(&self, block: DynBlockRef) -> Result<usize, Error> {
        self.validate_block_offset(block.0)?;
        let mut buf = [0u8; 4];
        self.stack.get_into(block.0 + 16, &mut buf)?;
        Ok(u32::from_le_bytes(buf) as usize)
    }

    // ── convenience list operations ───────────────────────────────────────────

    /// Allocate a block, write `data` into it, and prepend it to the active
    /// list.
    ///
    /// Returns a handle to the newly allocated block.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DataTooLarge`] if `data.len()` exceeds the maximum
    /// payload capacity, or [`Error::Io`] on failure.
    pub fn push_front(&self, data: &[u8]) -> Result<DynBlockRef, Error> {
        let bs = Self::block_size_for(data.len());
        if bs > MAX_BLOCK_SIZE {
            return Err(Error::DataTooLarge {
                capacity: MAX_BLOCK_SIZE - BLOCK_HEADER_SIZE,
                provided: data.len(),
            });
        }
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        let old_root = header.root;

        let new_block = self.alloc_locked(bs, &mut header)?;
        self.write_block_raw(new_block.0, bs, old_root, data.len() as u32, data)?;
        header.root = new_block.0;
        self.write_header_locked(&header)?;

        Ok(new_block)
    }

    /// Unlink the head of the active list, read its payload, free the block,
    /// and return the payload.
    ///
    /// Returns `None` if the list is empty.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ChecksumMismatch`] or [`Error::Io`].
    pub fn pop_front(&self) -> Result<Option<Vec<u8>>, Error> {
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        if header.root == 0 {
            return Ok(None);
        }
        let old_root = header.root;
        let (next, data) = self.read_block_full(old_root)?;

        header.root = next;
        self.write_header_locked(&header)?;
        self.free_locked(DynBlockRef(old_root), &mut header)?;
        Ok(Some(data))
    }

    /// Zero-copy variant of [`pop_front`](Self::pop_front).
    ///
    /// `buf.len()` must be ≥ the head block's `data_len`; if shorter the
    /// checksum verification fails.
    ///
    /// Returns `true` if an item was popped, `false` if the list was empty.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ChecksumMismatch`] or [`Error::Io`].
    pub fn pop_front_into(&self, buf: &mut [u8]) -> Result<bool, Error> {
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        if header.root == 0 {
            return Ok(false);
        }
        let old_root = header.root;

        self.read_into(DynBlockRef(old_root), buf)?;

        let mut next_buf = [0u8; 8];
        self.stack.get_into(old_root + 4, &mut next_buf)?;
        let next = u64::from_le_bytes(next_buf);

        header.root = next;
        self.write_header_locked(&header)?;
        self.free_locked(DynBlockRef(old_root), &mut header)?;
        Ok(true)
    }

    // ── utility ───────────────────────────────────────────────────────────────

    /// Return the smallest power-of-two total block size that can hold `size`
    /// bytes of payload, including the 20-byte block header.
    ///
    /// The minimum returned value is 32 (2^[`MIN_BIN`]).
    ///
    /// ```
    /// use bllist::DynamicBlockList;
    /// assert_eq!(DynamicBlockList::block_size_for(0),  32); // 0+20=20 → 32
    /// assert_eq!(DynamicBlockList::block_size_for(1),  32); // 1+20=21 → 32
    /// assert_eq!(DynamicBlockList::block_size_for(12), 32); // 12+20=32 → 32
    /// assert_eq!(DynamicBlockList::block_size_for(13), 64); // 13+20=33 → 64
    /// assert_eq!(DynamicBlockList::block_size_for(44), 64); // 44+20=64 → 64
    /// assert_eq!(DynamicBlockList::block_size_for(45), 128); // 45+20=65 → 128
    /// ```
    pub const fn block_size_for(size: usize) -> usize {
        let total = BLOCK_HEADER_SIZE + size;
        let min = 1usize << MIN_BIN;
        if total <= min {
            return min;
        }
        let mut p = min;
        while p < total {
            p <<= 1;
        }
        p
    }

    // ── private helpers ───────────────────────────────────────────────────────

    fn validate_block_offset(&self, offset: u64) -> Result<(), Error> {
        if offset < HEADER_SIZE {
            return Err(Error::InvalidBlock);
        }
        Ok(())
    }

    fn read_header_locked(&self) -> Result<DynHeader, Error> {
        let mut buf = [0u8; 272];
        self.stack.get_into(0, &mut buf)?;
        DynHeader::from_bytes(&buf)
    }

    fn write_header_locked(&self, header: &DynHeader) -> Result<(), Error> {
        self.stack.set(0, &header.to_bytes())?;
        Ok(())
    }

    /// Write a zeroed free block with the given `block_size` and `next` pointer.
    fn write_free_block_raw(&self, offset: u64, block_size: usize, next: u64) -> Result<(), Error> {
        let mut buf = vec![0u8; block_size];
        buf[4..12].copy_from_slice(&next.to_le_bytes());
        buf[12..16].copy_from_slice(&(block_size as u32).to_le_bytes());
        // data_len = 0, payload = zeros
        let crc = crc32fast::hash(&buf[4..]);
        buf[0..4].copy_from_slice(&crc.to_le_bytes());
        self.stack.set(offset, &buf)?;
        Ok(())
    }

    /// Pop from bin's free list or grow the file. Caller holds `mu`.
    ///
    /// Searches bins `[target_bin, target_bin + MAX_SPLIT]` for a free block.
    /// If found in a higher bin, splits it down.  Falls through to file
    /// extension when no free block is available within the search range.
    fn alloc_locked(
        &self,
        block_size: usize,
        header: &mut DynHeader,
    ) -> Result<DynBlockRef, Error> {
        let target_bin = bin_index(block_size);

        // Try exact bin first.
        if header.bin_heads[target_bin] != 0 {
            let bh = header.bin_heads[target_bin];
            let mut next_buf = [0u8; 8];
            self.stack.get_into(bh + 4, &mut next_buf)?;
            header.bin_heads[target_bin] = u64::from_le_bytes(next_buf);
            self.write_header_locked(header)?;
            return Ok(DynBlockRef(bh));
        }

        // Search up to MAX_SPLIT levels above for a block to split.
        let search_limit = (target_bin + MAX_SPLIT).min(NUM_BINS - 1);
        for k in (target_bin + 1)..=search_limit {
            if header.bin_heads[k] != 0 {
                let bh = header.bin_heads[k];
                let mut next_buf = [0u8; 8];
                self.stack.get_into(bh + 4, &mut next_buf)?;
                // Pop from bin k (not yet flushed; split_to_bin flushes once at end).
                header.bin_heads[k] = u64::from_le_bytes(next_buf);
                return self.split_to_bin(bh, k, target_bin, header);
            }
        }

        // No free block available — extend the file.
        let mut buf = vec![0u8; block_size];
        buf[12..16].copy_from_slice(&(block_size as u32).to_le_bytes());
        // next = 0, data_len = 0, payload = zeros
        let crc = crc32fast::hash(&buf[4..]);
        buf[0..4].copy_from_slice(&crc.to_le_bytes());
        let offset = self.stack.push(&buf)?;
        Ok(DynBlockRef(offset))
    }

    /// Split a block at `offset` (currently in `from_bin`) down to `to_bin`.
    ///
    /// For each level from `from_bin` down to `to_bin + 1`:
    /// 1. Write the upper half as a free block in `cur_bin - 1`.
    /// 2. Write the lower half with the halved `block_size`.
    ///
    /// The lower half at `offset` is returned as the allocated block.
    /// One header flush is issued at the end.  Caller must have already
    /// removed the original block from `from_bin` in `header` (in memory,
    /// not yet flushed).
    fn split_to_bin(
        &self,
        offset: u64,
        from_bin: usize,
        to_bin: usize,
        header: &mut DynHeader,
    ) -> Result<DynBlockRef, Error> {
        // The lower half always stays at `offset`; its size halves each iteration.
        // For each level, the upper half is written as a free block in the bin
        // one below the current level, then the lower half's block_size is shrunk.
        // Writing the lower half first keeps the sequential scan consistent if
        // we crash before writing the upper half.
        let mut cur_size = 1usize << from_bin;
        while bin_index(cur_size) > to_bin {
            let half = cur_size / 2;
            let upper = offset + half as u64;

            let upper_bin = bin_index(half);
            let old_head = header.bin_heads[upper_bin];
            // Shrink lower half first so the sequential scan stays valid on crash.
            self.write_free_block_raw(offset, half, 0)?;
            // Write upper half as a free block linked into its bin.
            self.write_free_block_raw(upper, half, old_head)?;
            header.bin_heads[upper_bin] = upper;

            cur_size = half;
        }

        // One header flush covers all the updated bin heads.
        self.write_header_locked(header)?;

        Ok(DynBlockRef(offset))
    }

    /// Zero the block and link it into the appropriate bin. Caller holds `mu`.
    fn free_locked(&self, block: DynBlockRef, header: &mut DynHeader) -> Result<(), Error> {
        let mut bs_buf = [0u8; 4];
        self.stack.get_into(block.0 + 12, &mut bs_buf)?;
        let block_size = u32::from_le_bytes(bs_buf) as usize;
        let bin = bin_index(block_size);
        let old_bin_head = header.bin_heads[bin];

        self.write_free_block_raw(block.0, block_size, old_bin_head)?;

        header.bin_heads[bin] = block.0;
        self.write_header_locked(header)?;
        Ok(())
    }

    /// Build a complete block buffer and write it in one `set` call.
    fn write_block_raw(
        &self,
        offset: u64,
        block_size: usize,
        next: u64,
        data_len: u32,
        data: &[u8],
    ) -> Result<(), Error> {
        let mut buf = vec![0u8; block_size];
        buf[4..12].copy_from_slice(&next.to_le_bytes());
        buf[12..16].copy_from_slice(&(block_size as u32).to_le_bytes());
        buf[16..20].copy_from_slice(&data_len.to_le_bytes());
        buf[20..20 + data.len()].copy_from_slice(data);
        let crc = crc32fast::hash(&buf[4..]);
        buf[0..4].copy_from_slice(&crc.to_le_bytes());
        self.stack.set(offset, &buf)?;
        Ok(())
    }

    /// Read, CRC-verify, and return `(next, block_size, data)` for a block.
    fn read_block_full_static(stack: &BStack, offset: u64) -> Result<(u64, usize, Vec<u8>), Error> {
        let mut hdr = [0u8; BLOCK_HEADER_SIZE];
        stack.get_into(offset, &mut hdr)?;
        let stored_crc = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
        let block_size = u32::from_le_bytes(hdr[12..16].try_into().unwrap()) as usize;
        let data_len = u32::from_le_bytes(hdr[16..20].try_into().unwrap()) as usize;

        if block_size < (1 << MIN_BIN)
            || !block_size.is_power_of_two()
            || block_size > MAX_BLOCK_SIZE
        {
            return Err(Error::Corruption(format!(
                "block at offset {offset} has invalid block_size {block_size}"
            )));
        }
        let payload_cap = block_size - BLOCK_HEADER_SIZE;
        if data_len > payload_cap {
            return Err(Error::Corruption(format!(
                "block at offset {offset}: data_len {data_len} > payload capacity {payload_cap}"
            )));
        }

        let payload = stack.get(
            offset + BLOCK_HEADER_SIZE as u64,
            offset + block_size as u64,
        )?;
        if payload.len() != payload_cap {
            return Err(Error::InvalidBlock);
        }

        let mut hasher = CrcHasher::new();
        hasher.update(&hdr[4..]);
        hasher.update(&payload);
        if hasher.finalize() != stored_crc {
            return Err(Error::ChecksumMismatch { block: offset });
        }

        let next = u64::from_le_bytes(hdr[4..12].try_into().unwrap());
        let data = payload[..data_len].to_vec();
        Ok((next, block_size, data))
    }

    fn read_block_full(&self, offset: u64) -> Result<(u64, Vec<u8>), Error> {
        Self::read_block_full_static(&self.stack, offset).map(|(next, _, data)| (next, data))
    }

    /// Sequential scan + coalesce + rebuild all bin free-lists.
    ///
    /// Walk the active list (with CRC verification), then scan every block
    /// slot in the file.  All non-active slots (free + orphans) are collected,
    /// adjacent runs whose combined size is a power of two are merged, and the
    /// bin free-lists are rebuilt from scratch with a two-phase header write
    /// (zero first, populate second) for crash safety.
    fn recover_orphans(stack: &BStack, header: &mut DynHeader, total: u64) -> Result<(), Error> {
        if total <= HEADER_SIZE {
            return Ok(());
        }

        let max_steps = ((total - HEADER_SIZE) / BLOCK_HEADER_SIZE as u64 + 1) as usize;

        // Walk active list (verifies CRC).
        let mut active: HashSet<u64> = HashSet::new();
        let mut cur = header.root;
        let mut steps = 0usize;
        while cur != 0 {
            if steps >= max_steps {
                return Err(Error::Corruption("cycle detected in active list".into()));
            }
            let (next, _, _) = Self::read_block_full_static(stack, cur)?;
            active.insert(cur);
            cur = next;
            steps += 1;
        }

        // Sequential scan: collect all non-active blocks as (offset, block_size).
        let mut free_blocks: Vec<(u64, usize)> = Vec::new();
        let mut scan = HEADER_SIZE;
        while scan < total {
            if scan + BLOCK_HEADER_SIZE as u64 > total {
                break;
            }
            let mut bs_buf = [0u8; 4];
            stack.get_into(scan + 12, &mut bs_buf)?;
            let block_size = u32::from_le_bytes(bs_buf) as usize;

            if block_size < (1 << MIN_BIN)
                || !block_size.is_power_of_two()
                || block_size > MAX_BLOCK_SIZE
            {
                return Err(Error::Corruption(format!(
                    "block at offset {scan} has invalid block_size {block_size} during scan"
                )));
            }

            let block_end = scan + block_size as u64;
            if block_end > total {
                break;
            }

            if !active.contains(&scan) {
                free_blocks.push((scan, block_size));
            }

            scan = block_end;
        }

        if free_blocks.is_empty() {
            return Ok(());
        }

        // One-pass coalescing: find maximal runs of physically adjacent free
        // blocks; merge the run if its total size is a power of two.
        let mut merged: Vec<(u64, usize)> = Vec::new();
        let mut i = 0;
        while i < free_blocks.len() {
            let run_start = free_blocks[i].0;
            let mut run_total = free_blocks[i].1;
            let mut j = i + 1;

            while j < free_blocks.len() {
                let prev_end = free_blocks[j - 1].0 + free_blocks[j - 1].1 as u64;
                if prev_end != free_blocks[j].0 {
                    break;
                }
                run_total += free_blocks[j].1;
                j += 1;
            }

            if j > i + 1 && run_total.is_power_of_two() {
                merged.push((run_start, run_total));
            } else {
                for &block in free_blocks.iter().take(j).skip(i) {
                    merged.push(block);
                }
            }
            i = j;
        }

        // Phase 1: zero all bin heads in the header and flush.
        // If we crash after this, all non-active blocks are orphans on next open.
        header.bin_heads = [0u64; NUM_BINS];
        stack.set(0, &header.to_bytes())?;

        // Phase 2: write each (possibly merged) free block with a new next
        // pointer linking it into its bin, then update bin_heads in memory.
        for &(off, bs) in &merged {
            let bin = bin_index(bs);
            let old_head = header.bin_heads[bin];
            let mut buf = vec![0u8; bs];
            buf[4..12].copy_from_slice(&old_head.to_le_bytes());
            buf[12..16].copy_from_slice(&(bs as u32).to_le_bytes());
            // data_len = 0, payload = zeros
            let crc = crc32fast::hash(&buf[4..]);
            buf[0..4].copy_from_slice(&crc.to_le_bytes());
            stack.set(off, &buf)?;
            header.bin_heads[bin] = off;
        }

        // Phase 3: write the fully populated header.
        stack.set(0, &header.to_bytes())?;

        Ok(())
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn tmp(label: &str) -> std::path::PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "bllist_dyn_{}_{}_{}.blld",
            std::process::id(),
            label,
            n
        ));
        p
    }

    // ── block_size_for ────────────────────────────────────────────────────────

    #[test]
    fn block_size_for_values() {
        // total = size + 20, rounded to next power of two (min 32)
        assert_eq!(DynamicBlockList::block_size_for(0), 32); // 20 → 32
        assert_eq!(DynamicBlockList::block_size_for(1), 32); // 21 → 32
        assert_eq!(DynamicBlockList::block_size_for(12), 32); // 32 → 32
        assert_eq!(DynamicBlockList::block_size_for(13), 64); // 33 → 64
        assert_eq!(DynamicBlockList::block_size_for(44), 64); // 64 → 64
        assert_eq!(DynamicBlockList::block_size_for(45), 128); // 65 → 128
        assert_eq!(DynamicBlockList::block_size_for(108), 128); // 128 → 128
        assert_eq!(DynamicBlockList::block_size_for(109), 256); // 129 → 256
    }

    #[test]
    fn bin_index_values() {
        assert_eq!(bin_index(32), 5);
        assert_eq!(bin_index(64), 6);
        assert_eq!(bin_index(128), 7);
        assert_eq!(bin_index(1024), 10);
    }

    // ── open / fresh file ─────────────────────────────────────────────────────

    #[test]
    fn fresh_open_empty() {
        let path = tmp("fresh");
        let list = DynamicBlockList::open(&path).unwrap();
        assert_eq!(list.root().unwrap(), None);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_rejects_fixed_list_file() {
        let path = tmp("crosstype");
        {
            crate::FixedBlockList::<52>::open(&path).unwrap();
        }
        let err = DynamicBlockList::open(&path).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
        let _ = std::fs::remove_file(&path);
    }

    // ── alloc / free / bin reuse ──────────────────────────────────────────────

    #[test]
    fn alloc_free_reuse_same_bin() {
        let path = tmp("reuse");
        let list = DynamicBlockList::open(&path).unwrap();

        // alloc(1) → block_size=32, bin 5
        let b0 = list.alloc(1).unwrap();
        let b1 = list.alloc(1).unwrap();
        let b2 = list.alloc(1).unwrap();

        list.free(b1).unwrap();
        // Next alloc of the same size should reuse b1.
        let b3 = list.alloc(1).unwrap();
        assert_eq!(b3, b1);

        let _ = (b0, b2, b3);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn alloc_different_sizes_different_bins() {
        let path = tmp("bins");
        let list = DynamicBlockList::open(&path).unwrap();

        // alloc(12) → 12+20=32 → block_size=32, payload_cap=12
        let small = list.alloc(12).unwrap();
        // alloc(44) → 44+20=64 → block_size=64, payload_cap=44
        let large = list.alloc(44).unwrap();

        assert_eq!(list.capacity(small).unwrap(), 12);
        assert_eq!(list.capacity(large).unwrap(), 44);

        let _ = (small, large);
        let _ = std::fs::remove_file(&path);
    }

    // ── splitting ─────────────────────────────────────────────────────────────

    #[test]
    fn split_one_level() {
        let path = tmp("split1");
        let list = DynamicBlockList::open(&path).unwrap();

        // Alloc a bin-6 block (block_size=64) and free it.
        let large = list.alloc(44).unwrap(); // 44+20=64, bin 6
        let large_off = large.0;
        list.free(large).unwrap();

        // Alloc bin-5 (block_size=32). With MAX_SPLIT=3, split from bin 6.
        let small = list.alloc(1).unwrap(); // 1+20=21 → 32, bin 5
        assert_eq!(small.0, large_off); // lower half
        assert_eq!(list.capacity(small).unwrap(), 12); // 32-20=12

        // Upper half (also bin-5) should be reusable.
        let small2 = list.alloc(1).unwrap();
        assert_eq!(small2.0, large_off + 32);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn split_two_levels() {
        let path = tmp("split2");
        let list = DynamicBlockList::open(&path).unwrap();

        // Alloc a bin-7 block (block_size=128) and free it.
        let big = list.alloc(108).unwrap(); // 108+20=128, bin 7
        let big_off = big.0;
        list.free(big).unwrap();

        // Alloc bin-5 (block_size=32). Splits bin-7 → bin-6 → bin-5 (2 levels ≤ MAX_SPLIT).
        let s = list.alloc(1).unwrap();
        assert_eq!(s.0, big_off);
        assert_eq!(list.capacity(s).unwrap(), 12);

        // Remaining pieces: one bin-6 at big_off+64, one bin-5 at big_off+32.
        let s2 = list.alloc(1).unwrap(); // bin 5 → reuse big_off+32
        assert_eq!(s2.0, big_off + 32);

        let m = list.alloc(44).unwrap(); // bin 6 → reuse big_off+64
        assert_eq!(m.0, big_off + 64);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn no_split_beyond_max_split() {
        let path = tmp("nosplit");
        let list = DynamicBlockList::open(&path).unwrap();

        // Alloc a bin-(5+MAX_SPLIT+1) block = bin-9 (block_size=512) and free it.
        let big = list.alloc(492).unwrap(); // 492+20=512, bin 9
        let big_off = big.0;
        list.free(big).unwrap();

        // Alloc bin-5. Distance = 9-5 = 4 > MAX_SPLIT=3 → should extend file, not split.
        let s = list.alloc(1).unwrap();
        assert_ne!(s.0, big_off); // file extended, not split from big

        let _ = std::fs::remove_file(&path);
    }

    // ── write / read round-trip ───────────────────────────────────────────────

    #[test]
    fn write_read_roundtrip() {
        let path = tmp("rw");
        let list = DynamicBlockList::open(&path).unwrap();
        // alloc(14) → 14+20=34 → block_size=64, payload_cap=44
        let block = list.alloc(14).unwrap();

        list.write(block, b"hello dynamic!").unwrap();

        let out = list.read(block).unwrap();
        assert_eq!(out, b"hello dynamic!");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn write_updates_data_len() {
        let path = tmp("datalen");
        let list = DynamicBlockList::open(&path).unwrap();
        // alloc(12) → block_size=32, payload_cap=12
        let block = list.alloc(12).unwrap();

        assert_eq!(list.data_len(block).unwrap(), 0);
        list.write(block, b"five!").unwrap();
        assert_eq!(list.data_len(block).unwrap(), 5);
        assert_eq!(list.capacity(block).unwrap(), 12);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn overwrite_shorter_zeroes_tail() {
        let path = tmp("overwrite");
        let list = DynamicBlockList::open(&path).unwrap();
        let block = list.alloc(12).unwrap(); // payload_cap=12

        list.write(block, b"longer text!").unwrap(); // 12 bytes
        list.write(block, b"short").unwrap();

        let out = list.read(block).unwrap();
        assert_eq!(out, b"short");
        assert_eq!(list.data_len(block).unwrap(), 5);

        let _ = std::fs::remove_file(&path);
    }

    // ── read_into (zero-copy) ─────────────────────────────────────────────────

    #[test]
    fn read_into_exact_length() {
        let path = tmp("read_into");
        let list = DynamicBlockList::open(&path).unwrap();
        let block = list.alloc(12).unwrap();
        list.write(block, b"abcdefgh").unwrap();

        let mut buf = vec![0u8; 8];
        let had_data = list.read_into(block, &mut buf).unwrap();
        assert!(had_data);
        assert_eq!(buf, b"abcdefgh");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn read_into_oversized_buf_ok() {
        let path = tmp("read_into_big");
        let list = DynamicBlockList::open(&path).unwrap();
        let block = list.alloc(12).unwrap();
        list.write(block, b"hi").unwrap();

        let mut buf = vec![0xFFu8; 12];
        let had_data = list.read_into(block, &mut buf).unwrap();
        assert!(had_data);
        assert_eq!(&buf[0..2], b"hi");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn read_into_short_buf_fails_crc() {
        let path = tmp("read_into_crc");
        let list = DynamicBlockList::open(&path).unwrap();
        let block = list.alloc(12).unwrap();
        list.write(block, b"twelve bytes").unwrap(); // 12 bytes

        let mut buf = vec![0u8; 4];
        let err = list.read_into(block, &mut buf).unwrap_err();
        assert!(matches!(err, Error::ChecksumMismatch { .. }));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn read_into_empty_block_returns_false() {
        let path = tmp("read_into_empty");
        let list = DynamicBlockList::open(&path).unwrap();
        let block = list.alloc(1).unwrap();

        let mut buf = vec![0u8; 12];
        let had_data = list.read_into(block, &mut buf).unwrap();
        assert!(!had_data);

        let _ = std::fs::remove_file(&path);
    }

    // ── checksum mismatch detection ───────────────────────────────────────────

    #[test]
    fn checksum_mismatch_on_corrupt_block() {
        let path = tmp("crc");
        {
            let list = DynamicBlockList::open(&path).unwrap();
            list.push_front(b"integrity check").unwrap();
        }

        {
            let stack = BStack::open(&path).unwrap();
            let block_offset = HEADER_SIZE;
            let payload_offset = block_offset + BLOCK_HEADER_SIZE as u64;
            let mut byte = [0u8; 1];
            stack.get_into(payload_offset, &mut byte).unwrap();
            byte[0] ^= 0xFF;
            stack.set(payload_offset, &byte).unwrap();
        }

        let err = DynamicBlockList::open(&path).unwrap_err();
        assert!(matches!(err, Error::ChecksumMismatch { .. }));

        let _ = std::fs::remove_file(&path);
    }

    // ── set_next / get_next ───────────────────────────────────────────────────

    #[test]
    fn set_get_next() {
        let path = tmp("next");
        let list = DynamicBlockList::open(&path).unwrap();
        let b0 = list.alloc(1).unwrap();
        let b1 = list.alloc(1).unwrap();

        assert_eq!(list.get_next(b0).unwrap(), None);

        list.set_next(b0, Some(b1)).unwrap();
        assert_eq!(list.get_next(b0).unwrap(), Some(b1));

        list.set_next(b0, None).unwrap();
        assert_eq!(list.get_next(b0).unwrap(), None);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn set_next_preserves_payload() {
        let path = tmp("next_payload");
        let list = DynamicBlockList::open(&path).unwrap();
        let b0 = list.alloc(12).unwrap();
        let b1 = list.alloc(12).unwrap();

        list.write(b0, b"preserved!!!").unwrap();
        list.set_next(b0, Some(b1)).unwrap();

        let out = list.read(b0).unwrap();
        assert_eq!(out, b"preserved!!!");

        let _ = std::fs::remove_file(&path);
    }

    // ── push_front / pop_front (LIFO) ─────────────────────────────────────────

    #[test]
    fn push_pop_lifo() {
        let path = tmp("lifo");
        let list = DynamicBlockList::open(&path).unwrap();

        list.push_front(b"first").unwrap();
        list.push_front(b"second longer").unwrap();
        list.push_front(b"third").unwrap();

        assert_eq!(list.pop_front().unwrap().unwrap(), b"third");
        assert_eq!(list.pop_front().unwrap().unwrap(), b"second longer");
        assert_eq!(list.pop_front().unwrap().unwrap(), b"first");
        assert_eq!(list.pop_front().unwrap(), None);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn push_pop_mixed_sizes() {
        let path = tmp("mixed");
        let list = DynamicBlockList::open(&path).unwrap();

        list.push_front(&[0u8; 1]).unwrap(); // block_size=32
        list.push_front(&[1u8; 100]).unwrap(); // 100+20=120 → block_size=128
        list.push_front(&[2u8; 10]).unwrap(); // 10+20=30 → block_size=32

        let d3 = list.pop_front().unwrap().unwrap();
        assert_eq!(d3, vec![2u8; 10]);
        let d2 = list.pop_front().unwrap().unwrap();
        assert_eq!(d2, vec![1u8; 100]);
        let d1 = list.pop_front().unwrap().unwrap();
        assert_eq!(d1, vec![0u8; 1]);

        let _ = std::fs::remove_file(&path);
    }

    // ── pop_front_into (zero-copy) ────────────────────────────────────────────

    #[test]
    fn pop_front_into_basic() {
        let path = tmp("pop_into");
        let list = DynamicBlockList::open(&path).unwrap();
        list.push_front(b"hello").unwrap();

        let mut buf = vec![0u8; 5];
        assert!(list.pop_front_into(&mut buf).unwrap());
        assert_eq!(buf, b"hello");

        assert!(!list.pop_front_into(&mut buf).unwrap());

        let _ = std::fs::remove_file(&path);
    }

    // ── pop_front on empty list ───────────────────────────────────────────────

    #[test]
    fn pop_front_empty() {
        let path = tmp("pop_empty");
        let list = DynamicBlockList::open(&path).unwrap();
        assert_eq!(list.pop_front().unwrap(), None);
        let _ = std::fs::remove_file(&path);
    }

    // ── orphan recovery ───────────────────────────────────────────────────────

    #[test]
    fn orphan_recovery() {
        let path = tmp("orphan");
        let orphan_offset;
        {
            let stack = BStack::open(&path).unwrap();
            let hdr = DynHeader {
                root: 0,
                bin_heads: [0u64; NUM_BINS],
            };
            let off = stack.push(&hdr.to_bytes()).unwrap();
            assert_eq!(off, 0);

            // Push a block with block_size=32 (bin 5) but don't update root.
            let bs: usize = 32;
            let mut block_buf = vec![0u8; bs];
            block_buf[12..16].copy_from_slice(&(bs as u32).to_le_bytes());
            let crc = crc32fast::hash(&block_buf[4..]);
            block_buf[0..4].copy_from_slice(&crc.to_le_bytes());
            orphan_offset = stack.push(&block_buf).unwrap();
        }

        // Reopen: orphan should be reclaimed into bin 5 (block_size=32=2^5).
        let list = DynamicBlockList::open(&path).unwrap();
        assert_eq!(list.root().unwrap(), None);

        // alloc(1) → block_size=32 → bin 5 → must return the recovered orphan.
        let b = list.alloc(1).unwrap();
        assert_eq!(b.0, orphan_offset);

        let _ = std::fs::remove_file(&path);
    }

    // ── coalescing on open ────────────────────────────────────────────────────

    #[test]
    fn coalesce_two_adjacent_free_blocks() {
        let path = tmp("coalesce2");
        {
            let list = DynamicBlockList::open(&path).unwrap();
            // Allocate two bin-5 blocks (block_size=32 each).
            let b0 = list.alloc(1).unwrap();
            let b1 = list.alloc(1).unwrap();
            // They are adjacent (b0 at HEADER_SIZE, b1 at HEADER_SIZE+32).
            assert_eq!(b1.0, b0.0 + 32);
            // Free both → two adjacent bin-5 blocks in the file.
            list.free(b0).unwrap();
            list.free(b1).unwrap();
        }

        // Reopen: two adjacent 32-byte blocks should coalesce into one 64-byte block.
        let list = DynamicBlockList::open(&path).unwrap();
        // Allocating bin-6 (block_size=64) should reuse the merged block.
        let big = list.alloc(44).unwrap(); // 44+20=64
        assert_eq!(big.0, HEADER_SIZE); // coalesced block starts at same offset as b0

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn coalesce_three_adjacent_free_blocks() {
        let path = tmp("coalesce3");
        let b0_off;
        {
            let list = DynamicBlockList::open(&path).unwrap();
            // Allocate bin-8 (block_size=256), bin-9 (block_size=512), bin-8 (256).
            let b0 = list.alloc(236).unwrap(); // 236+20=256, bin 8
            let b1 = list.alloc(492).unwrap(); // 492+20=512, bin 9
            let b2 = list.alloc(236).unwrap(); // 256, bin 8
            b0_off = b0.0;
            // Verify adjacency.
            assert_eq!(b1.0, b0.0 + 256);
            assert_eq!(b2.0, b0.0 + 768);
            // Free all → 256+512+256 = 1024 = 2^10 → should coalesce.
            list.free(b0).unwrap();
            list.free(b1).unwrap();
            list.free(b2).unwrap();
        }

        // Reopen: 256+512+256=1024 should merge into one bin-10 block.
        let list = DynamicBlockList::open(&path).unwrap();
        let big = list.alloc(1004).unwrap(); // 1004+20=1024, bin 10
        assert_eq!(big.0, b0_off);

        let _ = std::fs::remove_file(&path);
    }

    // ── error paths ───────────────────────────────────────────────────────────

    #[test]
    fn data_too_large_for_block() {
        let path = tmp("toolarge");
        let list = DynamicBlockList::open(&path).unwrap();
        let block = list.alloc(1).unwrap(); // payload_cap=12
        let err = list.write(block, &[0u8; 13]).unwrap_err(); // 13 > 12
        assert!(matches!(err, Error::DataTooLarge { .. }));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn invalid_block_offset() {
        let path = tmp("invalid");
        let list = DynamicBlockList::open(&path).unwrap();
        let err = list.read(DynBlockRef(0)).unwrap_err();
        assert!(matches!(err, Error::InvalidBlock));
        let _ = std::fs::remove_file(&path);
    }

    // ── persistence across reopen ─────────────────────────────────────────────

    #[test]
    fn reopen_persists_data() {
        let path = tmp("reopen");
        {
            let list = DynamicBlockList::open(&path).unwrap();
            list.push_front(b"persisted across reopen").unwrap();
        }
        {
            let list = DynamicBlockList::open(&path).unwrap();
            let data = list.pop_front().unwrap().unwrap();
            assert_eq!(data, b"persisted across reopen");
            assert_eq!(list.pop_front().unwrap(), None);
        }
        let _ = std::fs::remove_file(&path);
    }

    // ── freed blocks are reused after reopen ──────────────────────────────────

    #[test]
    fn free_list_persists_across_reopen() {
        let path = tmp("freelist_reopen");
        let freed_offset;
        {
            let list = DynamicBlockList::open(&path).unwrap();
            // alloc(12) → block_size=32, bin 5
            let b = list.alloc(12).unwrap();
            freed_offset = b.0;
            list.free(b).unwrap();
        }
        {
            let list = DynamicBlockList::open(&path).unwrap();
            // alloc(12) → block_size=32 → bin 5 → reuse freed block
            let b = list.alloc(12).unwrap();
            assert_eq!(b.0, freed_offset);
        }
        let _ = std::fs::remove_file(&path);
    }
}
