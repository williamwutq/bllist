//! Variable-size block linked-list allocator backed by a single BStack file.
//!
//! [`DynamicBlockList`] stores blocks of arbitrary size using a bin-based
//! free-list allocator inspired by dlmalloc.  Each allocation is satisfied
//! from the smallest bin whose capacity is ≥ the requested size.  If no
//! suitable free block exists the file is extended.
//!
//! ## File layout
//!
//! ```text
//! ┌──────────────────────────┬───────────────────────────────────────────────┐
//! │  BStack header (16 B)    │  bllist-dynamic header (272 B, logical off 0) │
//! │  "BSTK" magic + clen     │  "BLLD" + version + root + bin_heads[32]      │
//! ├──────────────────────────┴───────────────────────────────────────────────┤
//! │  Block (variable size)                                                    │
//! │  checksum(4) │ next(8) │ capacity(4) │ data_len(4) │ payload(capacity B) │
//! ├──────────────────────────────────────────────────────────────────────────┤
//! │  Block …                                                                  │
//! └──────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! **bllist-dynamic header** (272 bytes at logical offset 0):
//! - `[0..4]`    — magic `"BLLD"` (distinguishes dynamic files from `FixedBlockList` files)
//! - `[4..8]`    — version `u32 LE = 1`
//! - `[8..16]`   — root `u64 LE` (0 = empty)
//! - `[16..272]` — 32 × `u64 LE` bin free-list heads (bin *k* holds free blocks
//!   with capacity 2^k; 0 = empty bin)
//!
//! **Block on-disk layout** (20 + capacity bytes):
//! - `[0..4]`       — CRC32 of bytes `[4..20+capacity]` (next + capacity + data_len + full payload)
//! - `[4..12]`      — next `u64 LE` (logical offset of next block; 0 = null)
//! - `[12..16]`     — capacity `u32 LE` (allocated payload bytes; always a power of 2)
//! - `[16..20]`     — data_len `u32 LE` (bytes actually written; ≤ capacity)
//! - `[20..20+cap]` — payload (data_len bytes of data, then zeros)
//!
//! ## Bin allocator
//!
//! There are 32 bins.  Bin *k* holds free blocks whose capacity equals 2^k.
//! `capacity_for(n)` rounds *n* up to the next power of two (minimum 1) to
//! determine which bin an allocation comes from.  Freed blocks are returned to
//! the bin matching their capacity.
//!
//! ## Crash safety
//!
//! Same guarantees as [`FixedBlockList`](crate::FixedBlockList): every mutation
//! is durable before returning; orphaned blocks are reclaimed on the next
//! [`DynamicBlockList::open`].  Orphan recovery scans the file sequentially,
//! using the `capacity` field of each block to step from one block to the next.
//!
//! ## Cross-type safety
//!
//! `DynamicBlockList::open` rejects files with magic `"BLLS"` (fixed-list
//! files) and vice versa.  The two list types **cannot** share a file.

use std::collections::HashSet;
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
pub const VERSION: u32 = 1;

/// Size of the bllist-dynamic file header at logical offset 0 (bytes).
///
/// `4` (magic) + `4` (version) + `8` (root) + `32 × 8` (bin heads) = 272.
pub const HEADER_SIZE: u64 = 272;

/// Number of power-of-two free-list bins.
///
/// Bin *k* holds blocks with capacity 2^k.  With 32 bins the largest
/// addressable capacity is 2^31 bytes (2 GiB).
pub const NUM_BINS: usize = 32;

/// Byte size of the per-block header: checksum(4) + next(8) + capacity(4) + data_len(4).
pub const BLOCK_HEADER_SIZE: usize = 20;

/// Maximum capacity a single block may have (2^31 bytes).
const MAX_CAPACITY: usize = 1 << 31;

// ── helpers ───────────────────────────────────────────────────────────────────

/// Return the bin index for a power-of-two capacity (i.e. log2(capacity)).
#[inline]
fn bin_index(capacity: usize) -> usize {
    capacity.trailing_zeros() as usize
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

// ── DynamicBlockList ──────────────────────────────────────────────────────────

/// A durable, crash-safe singly-linked list of variable-size blocks backed by
/// a single BStack file.
///
/// Blocks can hold any payload size up to 2^31 bytes.  Internally each
/// allocation is rounded up to the next power of two so freed blocks can be
/// reused exactly by future allocations of the same or smaller size.
///
/// # Thread safety
///
/// `DynamicBlockList` is `Send + Sync`.  Header mutations (alloc, free, root
/// updates) are serialised through an internal `Mutex`.  Block-only reads and
/// writes (`write`, `read`, `set_next`, …) do not acquire the mutex and are
/// safe to call concurrently on different blocks.
pub struct DynamicBlockList {
    stack: BStack,
    mu: Mutex<()>,
}

// SAFETY: BStack wraps a raw file descriptor.  All concurrent file access goes
// through pread/pwrite (Unix) or ReadFile/WriteFile (Windows) at disjoint
// offsets, which is safe across threads.
unsafe impl Send for DynamicBlockList {}
unsafe impl Sync for DynamicBlockList {}

impl std::fmt::Debug for DynamicBlockList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicBlockList")
            .field("num_bins", &NUM_BINS)
            .finish()
    }
}

impl DynamicBlockList {
    // ── constructor ───────────────────────────────────────────────────────────

    /// Open or create a `DynamicBlockList` backed by `path`.
    ///
    /// If the file does not exist it is created and initialised with a fresh
    /// header.  If it already exists the header magic is validated — files
    /// created by [`FixedBlockList`](crate::FixedBlockList) (magic `"BLLS"`)
    /// are **rejected** with [`Error::Corruption`].
    ///
    /// After the header is validated, orphan recovery is performed: every block
    /// slot not reachable from the active list or any bin free list is
    /// reclaimed into the appropriate bin.
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
    /// The actual capacity of the returned block is `capacity_for(size)` (the
    /// next power of two ≥ `size`, minimum 1).  If the appropriate bin's free
    /// list is non-empty the head of that list is returned; otherwise the file
    /// is extended.
    ///
    /// The newly allocated block has `data_len = 0` and all payload bytes
    /// zeroed.  Call [`write`](Self::write) to store data.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DataTooLarge`] if `size` exceeds 2^31, or
    /// [`Error::Io`] on failure.
    pub fn alloc(&self, size: usize) -> Result<DynBlockRef, Error> {
        let cap = Self::capacity_for(size);
        if cap > MAX_CAPACITY {
            return Err(Error::DataTooLarge {
                capacity: MAX_CAPACITY,
                provided: size,
            });
        }
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        self.alloc_locked(cap, &mut header)
    }

    /// Return a block to the free list for its bin.
    ///
    /// The block's payload is zeroed and `data_len` is set to 0 before it is
    /// linked into the bin matching its capacity.
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
    /// `data.len()` must be ≤ the block's capacity.  The block's `data_len` is
    /// updated to `data.len()`; bytes beyond `data.len()` in the payload field
    /// are guaranteed to be zero.  The block checksum is recomputed and the
    /// whole block is written atomically in a single [`bstack::BStack::set`]
    /// call.
    ///
    /// Does **not** acquire the header mutex; safe to call concurrently on
    /// different blocks.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DataTooLarge`] if `data.len() > capacity`,
    /// [`Error::InvalidBlock`] for a bad offset, or [`Error::Io`] on failure.
    pub fn write(&self, block: DynBlockRef, data: &[u8]) -> Result<(), Error> {
        self.validate_block_offset(block.0)?;
        // Read next(8) + capacity(4) — skip the 4-byte checksum at the front.
        let mut fields = [0u8; 12];
        self.stack.get_into(block.0 + 4, &mut fields)?;
        let next = u64::from_le_bytes(fields[0..8].try_into().unwrap());
        let cap = u32::from_le_bytes(fields[8..12].try_into().unwrap()) as usize;
        if data.len() > cap {
            return Err(Error::DataTooLarge {
                capacity: cap,
                provided: data.len(),
            });
        }
        self.write_block_raw(block.0, cap, next, data.len() as u32, data)
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
        let cap = u32::from_le_bytes(hdr[12..16].try_into().unwrap()) as usize;
        let data_len = u32::from_le_bytes(hdr[16..20].try_into().unwrap()) as usize;

        // If buf is too small we can't fill data correctly; CRC will fail.
        if data_len > buf.len() {
            return Err(Error::ChecksumMismatch { block: block.0 });
        }

        if data_len > 0 {
            self.stack
                .get_into(block.0 + BLOCK_HEADER_SIZE as u64, &mut buf[0..data_len])?;
        }

        let mut hasher = CrcHasher::new();
        hasher.update(&hdr[4..]); // next(8) + capacity(4) + data_len(4)
        hasher.update(&buf[0..data_len]);
        if cap > data_len {
            hasher.update(&vec![0u8; cap - data_len]);
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

        // Read capacity to know the full block size.
        let mut cap_buf = [0u8; 4];
        self.stack.get_into(block.0 + 12, &mut cap_buf)?;
        let cap = u32::from_le_bytes(cap_buf) as usize;

        let block_size = BLOCK_HEADER_SIZE + cap;
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

    /// Return the allocated payload capacity of `block` in bytes.
    ///
    /// Always a power of two.  May be larger than the bytes actually stored;
    /// use [`data_len`](Self::data_len) for the number of bytes written.
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
        Ok(u32::from_le_bytes(buf) as usize)
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
    /// Returns [`Error::DataTooLarge`] if `data.len()` exceeds 2^31, or
    /// [`Error::Io`] on failure.
    pub fn push_front(&self, data: &[u8]) -> Result<DynBlockRef, Error> {
        let cap = Self::capacity_for(data.len());
        if cap > MAX_CAPACITY {
            return Err(Error::DataTooLarge {
                capacity: MAX_CAPACITY,
                provided: data.len(),
            });
        }
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        let old_root = header.root;

        // alloc_locked updates the bin head and flushes the header.
        let new_block = self.alloc_locked(cap, &mut header)?;
        // Write data + next pointer atomically (single set call).
        self.write_block_raw(new_block.0, cap, old_root, data.len() as u32, data)?;
        // Link as new root.
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

        // Advance root; crash here → old_root becomes an orphan, recovered on open().
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

        // read_into does not acquire mu — no deadlock.
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

    /// Return the smallest power-of-two capacity that can hold `size` bytes.
    ///
    /// `capacity_for(0)` returns 1.
    ///
    /// ```
    /// use bllist::DynamicBlockList;
    /// assert_eq!(DynamicBlockList::capacity_for(0),  1);
    /// assert_eq!(DynamicBlockList::capacity_for(1),  1);
    /// assert_eq!(DynamicBlockList::capacity_for(5),  8);
    /// assert_eq!(DynamicBlockList::capacity_for(8),  8);
    /// assert_eq!(DynamicBlockList::capacity_for(9),  16);
    /// ```
    pub const fn capacity_for(size: usize) -> usize {
        if size <= 1 {
            return 1;
        }
        let mut p = 1usize;
        while p < size {
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

    /// Pop from the bin's free list or grow the file. Caller holds `mu`.
    fn alloc_locked(&self, cap: usize, header: &mut DynHeader) -> Result<DynBlockRef, Error> {
        let bin = bin_index(cap);
        if header.bin_heads[bin] != 0 {
            let bh = header.bin_heads[bin];
            let mut next_buf = [0u8; 8];
            self.stack.get_into(bh + 4, &mut next_buf)?;
            header.bin_heads[bin] = u64::from_le_bytes(next_buf);
            self.write_header_locked(header)?;
            Ok(DynBlockRef(bh))
        } else {
            let block_size = BLOCK_HEADER_SIZE + cap;
            let mut buf = vec![0u8; block_size];
            buf[12..16].copy_from_slice(&(cap as u32).to_le_bytes());
            // next = 0, data_len = 0, payload = zeros
            let crc = crc32fast::hash(&buf[4..]);
            buf[0..4].copy_from_slice(&crc.to_le_bytes());
            let offset = self.stack.push(&buf)?;
            Ok(DynBlockRef(offset))
        }
    }

    /// Zero the block, link it into the appropriate bin, flush header. Caller holds `mu`.
    fn free_locked(&self, block: DynBlockRef, header: &mut DynHeader) -> Result<(), Error> {
        let mut cap_buf = [0u8; 4];
        self.stack.get_into(block.0 + 12, &mut cap_buf)?;
        let cap = u32::from_le_bytes(cap_buf) as usize;
        let bin = bin_index(cap);
        let old_bin_head = header.bin_heads[bin];

        let block_size = BLOCK_HEADER_SIZE + cap;
        let mut buf = vec![0u8; block_size];
        buf[4..12].copy_from_slice(&old_bin_head.to_le_bytes());
        buf[12..16].copy_from_slice(&cap_buf); // preserve capacity
        // data_len = 0, payload = zeros
        let crc = crc32fast::hash(&buf[4..]);
        buf[0..4].copy_from_slice(&crc.to_le_bytes());
        self.stack.set(block.0, &buf)?;

        header.bin_heads[bin] = block.0;
        self.write_header_locked(header)?;
        Ok(())
    }

    /// Build a complete block buffer and write it in one `set` call.
    fn write_block_raw(
        &self,
        offset: u64,
        cap: usize,
        next: u64,
        data_len: u32,
        data: &[u8],
    ) -> Result<(), Error> {
        let block_size = BLOCK_HEADER_SIZE + cap;
        let mut buf = vec![0u8; block_size];
        buf[4..12].copy_from_slice(&next.to_le_bytes());
        buf[12..16].copy_from_slice(&(cap as u32).to_le_bytes());
        buf[16..20].copy_from_slice(&data_len.to_le_bytes());
        buf[20..20 + data.len()].copy_from_slice(data);
        let crc = crc32fast::hash(&buf[4..]);
        buf[0..4].copy_from_slice(&crc.to_le_bytes());
        self.stack.set(offset, &buf)?;
        Ok(())
    }

    /// Read, CRC-verify, and return `(next, cap, data)` for a block.
    fn read_block_full_static(stack: &BStack, offset: u64) -> Result<(u64, usize, Vec<u8>), Error> {
        let mut hdr = [0u8; BLOCK_HEADER_SIZE];
        stack.get_into(offset, &mut hdr)?;
        let stored_crc = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
        let cap = u32::from_le_bytes(hdr[12..16].try_into().unwrap()) as usize;
        let data_len = u32::from_le_bytes(hdr[16..20].try_into().unwrap()) as usize;

        if cap == 0 || !cap.is_power_of_two() || cap > MAX_CAPACITY {
            return Err(Error::Corruption(format!(
                "block at offset {offset} has invalid capacity {cap}"
            )));
        }
        if data_len > cap {
            return Err(Error::Corruption(format!(
                "block at offset {offset}: data_len {data_len} > capacity {cap}"
            )));
        }

        let payload = stack.get(
            offset + BLOCK_HEADER_SIZE as u64,
            offset + BLOCK_HEADER_SIZE as u64 + cap as u64,
        )?;
        if payload.len() != cap {
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
        Ok((next, cap, data))
    }

    fn read_block_full(&self, offset: u64) -> Result<(u64, Vec<u8>), Error> {
        Self::read_block_full_static(&self.stack, offset).map(|(next, _, data)| (next, data))
    }

    /// Scan all committed block slots and reclaim any orphans into their bins.
    fn recover_orphans(stack: &BStack, header: &mut DynHeader, total: u64) -> Result<(), Error> {
        if total <= HEADER_SIZE {
            return Ok(());
        }

        // Upper bound on steps for cycle detection.
        let max_steps = ((total - HEADER_SIZE) / BLOCK_HEADER_SIZE as u64 + 1) as usize;

        // Walk active list.
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

        // Walk every bin's free list.
        let mut free_set: HashSet<u64> = HashSet::new();
        for k in 0..NUM_BINS {
            cur = header.bin_heads[k];
            steps = 0;
            while cur != 0 {
                if steps >= max_steps {
                    return Err(Error::Corruption(format!(
                        "cycle detected in bin {k} free list"
                    )));
                }
                let (next, _, _) = Self::read_block_full_static(stack, cur)?;
                free_set.insert(cur);
                cur = next;
                steps += 1;
            }
        }

        // Sequential scan: step through committed blocks using their capacity.
        let mut offset = HEADER_SIZE;
        let mut found_orphan = false;
        while offset < total {
            if offset + BLOCK_HEADER_SIZE as u64 > total {
                break;
            }
            let mut cap_buf = [0u8; 4];
            stack.get_into(offset + 12, &mut cap_buf)?;
            let cap = u32::from_le_bytes(cap_buf) as usize;

            if cap == 0 || !cap.is_power_of_two() || cap > MAX_CAPACITY {
                return Err(Error::Corruption(format!(
                    "block at offset {offset} has invalid capacity {cap} during orphan scan"
                )));
            }

            let block_end = offset + BLOCK_HEADER_SIZE as u64 + cap as u64;
            if block_end > total {
                break;
            }

            if !active.contains(&offset) && !free_set.contains(&offset) {
                let bin = bin_index(cap);
                let old_bin_head = header.bin_heads[bin];
                let block_size = BLOCK_HEADER_SIZE + cap;
                let mut buf = vec![0u8; block_size];
                buf[4..12].copy_from_slice(&old_bin_head.to_le_bytes());
                buf[12..16].copy_from_slice(&cap_buf);
                let crc = crc32fast::hash(&buf[4..]);
                buf[0..4].copy_from_slice(&crc.to_le_bytes());
                stack.set(offset, &buf)?;
                header.bin_heads[bin] = offset;
                found_orphan = true;
            }

            offset = block_end;
        }

        if found_orphan {
            stack.set(0, &header.to_bytes())?;
        }

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

    // ── capacity_for ──────────────────────────────────────────────────────────

    #[test]
    fn capacity_for_values() {
        assert_eq!(DynamicBlockList::capacity_for(0), 1);
        assert_eq!(DynamicBlockList::capacity_for(1), 1);
        assert_eq!(DynamicBlockList::capacity_for(2), 2);
        assert_eq!(DynamicBlockList::capacity_for(3), 4);
        assert_eq!(DynamicBlockList::capacity_for(4), 4);
        assert_eq!(DynamicBlockList::capacity_for(5), 8);
        assert_eq!(DynamicBlockList::capacity_for(8), 8);
        assert_eq!(DynamicBlockList::capacity_for(9), 16);
        assert_eq!(DynamicBlockList::capacity_for(1024), 1024);
        assert_eq!(DynamicBlockList::capacity_for(1025), 2048);
    }

    #[test]
    fn bin_index_values() {
        assert_eq!(bin_index(1), 0);
        assert_eq!(bin_index(2), 1);
        assert_eq!(bin_index(4), 2);
        assert_eq!(bin_index(8), 3);
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

        let b0 = list.alloc(8).unwrap();
        let b1 = list.alloc(8).unwrap();
        let b2 = list.alloc(8).unwrap();

        list.free(b1).unwrap();
        // Next alloc of the same size should reuse b1.
        let b3 = list.alloc(8).unwrap();
        assert_eq!(b3, b1);

        let _ = (b0, b2, b3);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn alloc_different_sizes_independent_bins() {
        let path = tmp("bins");
        let list = DynamicBlockList::open(&path).unwrap();

        let small = list.alloc(4).unwrap();
        let large = list.alloc(64).unwrap();

        assert_eq!(list.capacity(small).unwrap(), 4);
        assert_eq!(list.capacity(large).unwrap(), 64);

        // Freeing large does not affect small bin.
        list.free(large).unwrap();
        let b2 = list.alloc(4).unwrap();
        // Should grow, not reuse large (different bin).
        assert_ne!(b2, large);
        // But alloc(64) should reuse large.
        let b3 = list.alloc(64).unwrap();
        assert_eq!(b3, large);

        let _ = (small, b2, b3);
        let _ = std::fs::remove_file(&path);
    }

    // ── write / read round-trip ───────────────────────────────────────────────

    #[test]
    fn write_read_roundtrip() {
        let path = tmp("rw");
        let list = DynamicBlockList::open(&path).unwrap();
        let block = list.alloc(16).unwrap();

        list.write(block, b"hello dynamic!").unwrap();

        let out = list.read(block).unwrap();
        assert_eq!(out, b"hello dynamic!");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn write_updates_data_len() {
        let path = tmp("datalen");
        let list = DynamicBlockList::open(&path).unwrap();
        let block = list.alloc(32).unwrap();

        assert_eq!(list.data_len(block).unwrap(), 0);
        list.write(block, b"five!").unwrap();
        assert_eq!(list.data_len(block).unwrap(), 5);
        assert_eq!(list.capacity(block).unwrap(), 32);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn overwrite_shorter_zeroes_tail() {
        let path = tmp("overwrite");
        let list = DynamicBlockList::open(&path).unwrap();
        let block = list.alloc(32).unwrap();

        list.write(block, b"longer string here").unwrap();
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
        let block = list.alloc(16).unwrap();
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
        let block = list.alloc(16).unwrap();
        list.write(block, b"hi").unwrap();

        // buf larger than data_len is fine.
        let mut buf = vec![0xFFu8; 16];
        let had_data = list.read_into(block, &mut buf).unwrap();
        assert!(had_data);
        assert_eq!(&buf[0..2], b"hi");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn read_into_short_buf_fails_crc() {
        let path = tmp("read_into_crc");
        let list = DynamicBlockList::open(&path).unwrap();
        let block = list.alloc(16).unwrap();
        list.write(block, b"sixteen_bytes!!!").unwrap();

        // buf smaller than data_len → checksum mismatch.
        let mut buf = vec![0u8; 4];
        let err = list.read_into(block, &mut buf).unwrap_err();
        assert!(matches!(err, Error::ChecksumMismatch { .. }));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn read_into_empty_block_returns_false() {
        let path = tmp("read_into_empty");
        let list = DynamicBlockList::open(&path).unwrap();
        let block = list.alloc(8).unwrap();

        let mut buf = vec![0u8; 8];
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

        // Corrupt one payload byte via BStack.
        {
            let stack = BStack::open(&path).unwrap();
            // Dynamic header = 272 B; first block starts at logical offset 272.
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
        let b0 = list.alloc(8).unwrap();
        let b1 = list.alloc(8).unwrap();

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
        let b0 = list.alloc(16).unwrap();
        let b1 = list.alloc(16).unwrap();

        list.write(b0, b"preserved").unwrap();
        list.set_next(b0, Some(b1)).unwrap();

        let out = list.read(b0).unwrap();
        assert_eq!(out, b"preserved");

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

        // Push items that round to different bin capacities.
        list.push_front(&[0u8; 1]).unwrap(); // cap 1
        list.push_front(&[1u8; 100]).unwrap(); // cap 128
        list.push_front(&[2u8; 10]).unwrap(); // cap 16

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
            // Create a file and simulate a crash: block allocated (file grown)
            // but never linked (header not updated to point root at it).
            let stack = BStack::open(&path).unwrap();
            let hdr = DynHeader {
                root: 0,
                bin_heads: [0u64; NUM_BINS],
            };
            let off = stack.push(&hdr.to_bytes()).unwrap();
            assert_eq!(off, 0);

            // Push a block with capacity=8 but don't update root.
            let cap: usize = 8;
            let block_size = BLOCK_HEADER_SIZE + cap;
            let mut block_buf = vec![0u8; block_size];
            block_buf[12..16].copy_from_slice(&(cap as u32).to_le_bytes());
            let crc = crc32fast::hash(&block_buf[4..]);
            block_buf[0..4].copy_from_slice(&crc.to_le_bytes());
            orphan_offset = stack.push(&block_buf).unwrap();
        }

        // Reopen: orphan should be reclaimed into bin 3 (cap=8 → 2^3).
        let list = DynamicBlockList::open(&path).unwrap();
        assert_eq!(list.root().unwrap(), None);

        // alloc(8) must return the recovered orphan.
        let b = list.alloc(8).unwrap();
        assert_eq!(b.0, orphan_offset);

        let _ = std::fs::remove_file(&path);
    }

    // ── error paths ───────────────────────────────────────────────────────────

    #[test]
    fn data_too_large_for_block() {
        let path = tmp("toolarge");
        let list = DynamicBlockList::open(&path).unwrap();
        let block = list.alloc(8).unwrap();
        let err = list.write(block, &[0u8; 9]).unwrap_err();
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
            let b = list.alloc(16).unwrap();
            freed_offset = b.0;
            list.free(b).unwrap();
        }
        {
            let list = DynamicBlockList::open(&path).unwrap();
            let b = list.alloc(16).unwrap();
            assert_eq!(b.0, freed_offset);
        }
        let _ = std::fs::remove_file(&path);
    }
}
