use std::collections::HashSet;
use std::fmt;
use std::path::Path;
use std::sync::Mutex;

use bstack::BStack;
use crc32fast::Hasher as CrcHasher;

use crate::Error;
use crate::block::{self, Block, BlockLayout};

// ── block layout ──────────────────────────────────────────────────────────────

pub(crate) struct DynDblLayout;
impl BlockLayout for DynDblLayout {
    /// prev(8) + next(8) + block_size(4) + data_len(4) = 24 bytes.
    const HEADER_CONTENT_SIZE: usize = 24;
}

// ── on-disk constants ─────────────────────────────────────────────────────────

/// Magic bytes for the doubly-linked dynamic-list file format.
pub const MAGIC: [u8; 4] = *b"BLDD";

/// On-disk format version for the doubly-linked dynamic list.
pub const VERSION: u32 = 1;

/// Number of power-of-two free-list bins (same as the singly-linked variant).
pub const NUM_BINS: usize = 32;

/// Size of the file header at logical offset 0 (bytes).
///
/// `4` (magic) + `4` (version) + `8` (root) + `8` (tail) + `32 × 8` (bin heads) = 280.
pub const HEADER_SIZE: u64 = 280;

/// Byte size of the per-block header: 4-byte checksum + 24-byte header content.
pub const BLOCK_HEADER_SIZE: usize = 4 + DynDblLayout::HEADER_CONTENT_SIZE;

/// Index of the smallest usable bin (2^5 = 32 bytes total; 4-byte payload).
pub const MIN_BIN: usize = 5;

/// Maximum number of bin levels to search above the target bin before extending
/// the file instead of splitting.
pub const MAX_SPLIT: usize = 3;

const MAX_BLOCK_SIZE: usize = 1 << 31;

// ── helpers ───────────────────────────────────────────────────────────────────

#[inline]
fn bin_index(block_size: usize) -> usize {
    block_size.trailing_zeros() as usize
}

// ── DynDblHeader ──────────────────────────────────────────────────────────────

struct DynDblHeader {
    root: u64,
    tail: u64,
    bin_heads: [u64; NUM_BINS],
}

impl DynDblHeader {
    fn from_bytes(buf: &[u8; 280]) -> Result<Self, Error> {
        if &buf[0..4] == b"BLLS" || &buf[0..4] == b"BLLD" || &buf[0..4] == b"BLDF" {
            return Err(Error::Corruption(format!(
                "file magic {:?} belongs to a different list type; use the matching open method",
                std::str::from_utf8(&buf[0..4]).unwrap_or("?")
            )));
        }
        if buf[0..4] != MAGIC {
            return Err(Error::Corruption(format!(
                "invalid magic: expected {:?} (\"BLDD\"), found {:?}",
                MAGIC,
                &buf[0..4]
            )));
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if version != VERSION {
            return Err(Error::Corruption(format!(
                "unsupported dynamic-doubly-linked version {version}, expected {VERSION}"
            )));
        }
        let root = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let tail = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        let mut bin_heads = [0u64; NUM_BINS];
        for (k, bh) in bin_heads.iter_mut().enumerate() {
            let s = 24 + k * 8;
            *bh = u64::from_le_bytes(buf[s..s + 8].try_into().unwrap());
        }
        Ok(Self {
            root,
            tail,
            bin_heads,
        })
    }

    fn to_bytes(&self) -> [u8; 280] {
        let mut buf = [0u8; 280];
        buf[0..4].copy_from_slice(&MAGIC);
        buf[4..8].copy_from_slice(&VERSION.to_le_bytes());
        buf[8..16].copy_from_slice(&self.root.to_le_bytes());
        buf[16..24].copy_from_slice(&self.tail.to_le_bytes());
        for (k, bh) in self.bin_heads.iter().enumerate() {
            let s = 24 + k * 8;
            buf[s..s + 8].copy_from_slice(&bh.to_le_bytes());
        }
        buf
    }
}

// ── DynBlockDblRef ────────────────────────────────────────────────────────────

/// A handle to a block in a [`DynamicDblList`], encoded as the block's logical
/// byte offset within the underlying BStack file.
///
/// `DynBlockDblRef` is `Copy` and cheap to store; treat it like a typed index.
/// Offset `0` is never a valid block and is used as a null / end-of-list sentinel.
///
/// Formatted as `@offset` (decimal), `@hex` (lower-case hex via `{:x}`), or
/// `@HEX` (upper-case hex via `{:X}`). The `#` flag adds `0x` after the `@`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DynBlockDblRef(pub u64);

crate::block::impl_block_ref!(DynBlockDblRef);

impl DynBlockDblRef {
    /// Return the logical byte offset of the first payload byte for this block.
    ///
    /// This is `self.0 + 28` (the 28-byte block header always precedes the
    /// payload).  No file access is performed.
    ///
    /// For the offset past the last *written* byte (respecting `data_len`), use
    /// [`DynamicDblList::data_end`].
    #[inline]
    pub fn data_start(self) -> u64 {
        self.0 + BLOCK_HEADER_SIZE as u64
    }
}

// ── DynamicDblList ────────────────────────────────────────────────────────────

/// A durable, crash-safe **doubly-linked** list of variable-size blocks backed
/// by a single BStack file.
///
/// `DynamicDblList` uses the same bin-based allocator as [`DynamicBlockList`](crate::DynamicBlockList)
/// but adds a `prev` pointer to every block and stores a tail pointer in the
/// file header, enabling O(1) [`push_back`](Self::push_back) and
/// [`pop_back`](Self::pop_back) in addition to the usual front operations.
///
/// # Block layout
///
/// Each block's total on-disk footprint is a power of two (minimum 32 bytes):
///
/// | Offset | Size | Field | Description |
/// |--------|------|-------|-------------|
/// | 0 | 4 | `checksum` | CRC32 of bytes `[4..block_size]` |
/// | 4 | 8 | `prev` | Preceding block offset; `0` = null |
/// | 12 | 8 | `next` | Following block offset; `0` = null |
/// | 20 | 4 | `block_size` | Total bytes on disk (power of two) |
/// | 24 | 4 | `data_len` | Bytes written; ≤ `block_size − 28` |
/// | 28 | `block_size − 28` | `payload` | User data |
///
/// # File format
///
/// Uses magic bytes `"BLDD"`.  The 280-byte file header stores the root pointer,
/// tail pointer, and 32 bin free-list heads.  It cannot be opened by other list
/// types.
///
/// # Iteration
///
/// [`iter`](Self::iter) returns a [`DynDblIter`] implementing both [`Iterator`]
/// and [`DoubleEndedIterator`].
///
/// # Example
///
/// ```no_run
/// use bllist::DynamicDblList;
///
/// let list = DynamicDblList::open("data.bldd")?;
///
/// list.push_back(b"first")?;
/// list.push_back(b"second")?;
///
/// // Forward:
/// for item in list.iter()? {
///     println!("{}", String::from_utf8_lossy(&item?));
/// }
///
/// // Backward via DoubleEndedIterator:
/// for item in list.iter()?.rev() {
///     println!("{}", String::from_utf8_lossy(&item?));
/// }
/// # Ok::<(), bllist::Error>(())
/// ```
pub struct DynamicDblList {
    stack: BStack,
    mu: Mutex<()>,
}

// SAFETY: BStack wraps a raw file descriptor; all concurrent access uses
// pread/pwrite at disjoint offsets.
unsafe impl Send for DynamicDblList {}
unsafe impl Sync for DynamicDblList {}

impl fmt::Debug for DynamicDblList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamicDblList")
            .field("num_bins", &NUM_BINS)
            .finish()
    }
}

impl fmt::Display for DynamicDblList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DynamicDblList")
    }
}

impl DynamicDblList {
    // ── constructor ───────────────────────────────────────────────────────────

    /// Open or create a `DynamicDblList` backed by `path`.
    ///
    /// Validates the magic (`"BLDD"`) and version, rejects files from other
    /// list types, and performs crash recovery (orphan reclamation, coalescing,
    /// and tail rebuild) on existing files.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] for I/O failures or [`Error::Corruption`] if the
    /// header magic or version is wrong.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let stack = BStack::open(path)?;
        let total = stack.len()?;

        if total == 0 {
            let hdr = DynDblHeader {
                root: 0,
                tail: 0,
                bin_heads: [0u64; NUM_BINS],
            };
            let offset = stack.push(&hdr.to_bytes())?;
            debug_assert_eq!(
                offset, 0,
                "DynamicDblList header must land at logical offset 0"
            );
            return Ok(Self {
                stack,
                mu: Mutex::new(()),
            });
        }

        if total < HEADER_SIZE {
            return Err(Error::Corruption(format!(
                "file payload is {total} bytes, too small for the {HEADER_SIZE}-byte header"
            )));
        }

        let mut hdr_buf = [0u8; 280];
        stack.get_into(0, &mut hdr_buf)?;
        let mut header = DynDblHeader::from_bytes(&hdr_buf)?;

        Self::recover_orphans(&stack, &mut header, total)?;

        Ok(Self {
            stack,
            mu: Mutex::new(()),
        })
    }

    // ── allocation ────────────────────────────────────────────────────────────

    /// Allocate a block with at least `size` bytes of payload capacity.
    ///
    /// The actual footprint is [`block_size_for(size)`](Self::block_size_for).
    /// If the exact bin has a free block it is returned; otherwise a block from
    /// a larger bin is split (up to [`MAX_SPLIT`] levels), or the file is
    /// extended.
    ///
    /// The returned block has `data_len = 0`; call [`write`](Self::write) to
    /// store data.
    pub fn alloc(&self, size: usize) -> Result<DynBlockDblRef, Error> {
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

    /// Return `block` to the free list for its bin.
    ///
    /// If the block is the last block in the file, the BStack is shrunk.
    /// Otherwise the payload is zeroed and the block is linked into the
    /// appropriate bin.
    pub fn free(&self, block: DynBlockDblRef) -> Result<(), Error> {
        self.validate_block_offset(block.0)?;
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        self.free_locked(block, &mut header)
    }

    // ── payload I/O ───────────────────────────────────────────────────────────

    /// Write `data` into `block`'s payload field.
    ///
    /// `data.len()` must be ≤ `block_size − 28`.  The block's `data_len` is
    /// updated; the checksum is recomputed atomically.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DataTooLarge`] if `data.len()` exceeds the block's
    /// payload capacity.
    pub fn write(&self, block: DynBlockDblRef, data: &[u8]) -> Result<(), Error> {
        self.validate_block_offset(block.0)?;
        let b = Block::<DynDblLayout>::new(block.0);
        let hc = b.read_header_unchecked(&self.stack)?;
        let block_size = u32::from_le_bytes(hc[16..20].try_into().unwrap()) as usize;
        let payload_cap = block_size.saturating_sub(BLOCK_HEADER_SIZE);
        // Preserve prev + next + block_size, update data_len.
        let mut new_hc = [0u8; DynDblLayout::HEADER_CONTENT_SIZE];
        new_hc[0..20].copy_from_slice(&hc[0..20]);
        new_hc[20..24].copy_from_slice(&(data.len() as u32).to_le_bytes());
        b.write(&self.stack, &new_hc, payload_cap, data)
    }

    /// Read `block`'s payload, returning exactly the `data_len` bytes written.
    ///
    /// The checksum is verified before returning.
    pub fn read(&self, block: DynBlockDblRef) -> Result<Vec<u8>, Error> {
        self.validate_block_offset(block.0)?;
        let (_, _, _, data) = Self::read_block_full_static(&self.stack, block.0)?;
        Ok(data)
    }

    /// Zero-copy variant of [`read`](Self::read).
    ///
    /// Fills `buf[0..data_len]` directly from the file.  `buf.len()` must be
    /// ≥ `data_len` for the CRC to pass.
    ///
    /// Returns `true` if data was present, `false` if `data_len == 0`.
    pub fn read_into(&self, block: DynBlockDblRef, buf: &mut [u8]) -> Result<bool, Error> {
        self.validate_block_offset(block.0)?;

        let mut hdr = [0u8; BLOCK_HEADER_SIZE];
        self.stack.get_into(block.0, &mut hdr)?;
        let stored_crc = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
        // block_size at header byte 20, data_len at header byte 24.
        let block_size = u32::from_le_bytes(hdr[20..24].try_into().unwrap()) as usize;
        let data_len = u32::from_le_bytes(hdr[24..28].try_into().unwrap()) as usize;
        let payload_cap = block_size.saturating_sub(BLOCK_HEADER_SIZE);

        if data_len > buf.len() {
            return Err(Error::ChecksumMismatch { block: block.0 });
        }

        if data_len > 0 {
            self.stack
                .get_into(block.0 + BLOCK_HEADER_SIZE as u64, &mut buf[0..data_len])?;
        }

        let mut hasher = CrcHasher::new();
        hasher.update(&hdr[4..]); // prev + next + block_size + data_len
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

    /// Update the `next` pointer of `block`.  The payload, prev, block_size,
    /// and data_len fields are preserved; the checksum is recomputed atomically.
    pub fn set_next(
        &self,
        block: DynBlockDblRef,
        next: Option<DynBlockDblRef>,
    ) -> Result<(), Error> {
        self.validate_block_offset(block.0)?;
        self.update_next(block.0, next.map(|r| r.0).unwrap_or(0))
    }

    /// Update the `prev` pointer of `block`.  The payload, next, block_size,
    /// and data_len fields are preserved; the checksum is recomputed atomically.
    pub fn set_prev(
        &self,
        block: DynBlockDblRef,
        prev: Option<DynBlockDblRef>,
    ) -> Result<(), Error> {
        self.validate_block_offset(block.0)?;
        self.update_prev(block.0, prev.map(|r| r.0).unwrap_or(0))
    }

    /// Read the `next` pointer without verifying the checksum.
    pub fn get_next(&self, block: DynBlockDblRef) -> Result<Option<DynBlockDblRef>, Error> {
        self.validate_block_offset(block.0)?;
        let hc = Block::<DynDblLayout>::new(block.0).read_header_unchecked(&self.stack)?;
        let next = u64::from_le_bytes(hc[8..16].try_into().unwrap());
        Ok(if next == 0 {
            None
        } else {
            Some(DynBlockDblRef(next))
        })
    }

    /// Read the `prev` pointer without verifying the checksum.
    pub fn get_prev(&self, block: DynBlockDblRef) -> Result<Option<DynBlockDblRef>, Error> {
        self.validate_block_offset(block.0)?;
        let hc = Block::<DynDblLayout>::new(block.0).read_header_unchecked(&self.stack)?;
        let prev = u64::from_le_bytes(hc[0..8].try_into().unwrap());
        Ok(if prev == 0 {
            None
        } else {
            Some(DynBlockDblRef(prev))
        })
    }

    // ── list head / tail ──────────────────────────────────────────────────────

    /// Return the current head of the active list, or `None` if empty.
    pub fn root(&self) -> Result<Option<DynBlockDblRef>, Error> {
        let _g = self.mu.lock().unwrap();
        let header = self.read_header_locked()?;
        Ok(if header.root == 0 {
            None
        } else {
            Some(DynBlockDblRef(header.root))
        })
    }

    /// Return the current tail of the active list, or `None` if empty.
    pub fn tail(&self) -> Result<Option<DynBlockDblRef>, Error> {
        let _g = self.mu.lock().unwrap();
        let header = self.read_header_locked()?;
        Ok(if header.tail == 0 {
            None
        } else {
            Some(DynBlockDblRef(header.tail))
        })
    }

    // ── block metadata ────────────────────────────────────────────────────────

    /// Return the payload capacity of `block` in bytes (`block_size − 28`).
    pub fn capacity(&self, block: DynBlockDblRef) -> Result<usize, Error> {
        self.validate_block_offset(block.0)?;
        let mut buf = [0u8; 4];
        self.stack.get_into(block.0 + 20, &mut buf)?; // block_size at byte 20
        let block_size = u32::from_le_bytes(buf) as usize;
        Ok(block_size.saturating_sub(BLOCK_HEADER_SIZE))
    }

    /// Return the number of payload bytes currently stored in `block`.
    pub fn data_len(&self, block: DynBlockDblRef) -> Result<usize, Error> {
        self.validate_block_offset(block.0)?;
        let mut buf = [0u8; 4];
        self.stack.get_into(block.0 + 24, &mut buf)?; // data_len at byte 24
        Ok(u32::from_le_bytes(buf) as usize)
    }

    /// Return the logical byte offset of the first payload byte of `block`.
    pub fn data_start(&self, block: DynBlockDblRef) -> Result<u64, Error> {
        self.validate_block_offset(block.0)?;
        Ok(block.data_start())
    }

    /// Return the logical byte offset one past the last written payload byte.
    pub fn data_end(&self, block: DynBlockDblRef) -> Result<u64, Error> {
        self.validate_block_offset(block.0)?;
        let mut buf = [0u8; 4];
        self.stack.get_into(block.0 + 24, &mut buf)?;
        let data_len = u32::from_le_bytes(buf) as u64;
        Ok(block.data_start() + data_len)
    }

    // ── convenience list operations ───────────────────────────────────────────

    /// Allocate a block, write `data`, and prepend it to the active list.
    pub fn push_front(&self, data: &[u8]) -> Result<DynBlockDblRef, Error> {
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
        self.write_block_raw(new_block.0, bs, 0, old_root, data.len() as u32, data)?;
        if old_root != 0 {
            self.update_prev(old_root, new_block.0)?;
        }
        header.root = new_block.0;
        if header.tail == 0 {
            header.tail = new_block.0;
        }
        self.write_header_locked(&header)?;
        Ok(new_block)
    }

    /// Allocate a block, write `data`, and append it to the active list.
    pub fn push_back(&self, data: &[u8]) -> Result<DynBlockDblRef, Error> {
        let bs = Self::block_size_for(data.len());
        if bs > MAX_BLOCK_SIZE {
            return Err(Error::DataTooLarge {
                capacity: MAX_BLOCK_SIZE - BLOCK_HEADER_SIZE,
                provided: data.len(),
            });
        }
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        let old_tail = header.tail;

        let new_block = self.alloc_locked(bs, &mut header)?;
        self.write_block_raw(new_block.0, bs, old_tail, 0, data.len() as u32, data)?;
        if old_tail != 0 {
            self.update_next(old_tail, new_block.0)?;
        }
        header.tail = new_block.0;
        if header.root == 0 {
            header.root = new_block.0;
        }
        self.write_header_locked(&header)?;
        Ok(new_block)
    }

    /// Unlink the head, read its payload, free the block, and return the data.
    ///
    /// Returns `None` if the list is empty.
    pub fn pop_front(&self) -> Result<Option<Vec<u8>>, Error> {
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        if header.root == 0 {
            return Ok(None);
        }
        let old_root = header.root;
        let (_, next, data) = self.read_block_front(old_root)?;

        header.root = next;
        if next != 0 {
            self.update_prev(next, 0)?;
        } else {
            header.tail = 0;
        }
        self.write_header_locked(&header)?;
        self.free_locked(DynBlockDblRef(old_root), &mut header)?;
        Ok(Some(data))
    }

    /// Unlink the tail, read its payload, free the block, and return the data.
    ///
    /// Returns `None` if the list is empty.
    pub fn pop_back(&self) -> Result<Option<Vec<u8>>, Error> {
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        if header.tail == 0 {
            return Ok(None);
        }
        let old_tail = header.tail;
        let (prev, data) = self.read_block_back(old_tail)?;

        header.tail = prev;
        if prev != 0 {
            self.update_next(prev, 0)?;
        } else {
            header.root = 0;
        }
        self.write_header_locked(&header)?;
        self.free_locked(DynBlockDblRef(old_tail), &mut header)?;
        Ok(Some(data))
    }

    /// Zero-copy variant of [`pop_front`](Self::pop_front).
    ///
    /// `buf.len()` must be ≥ the head block's `data_len`; returns `true` if an
    /// item was popped, `false` if the list was empty.
    pub fn pop_front_into(&self, buf: &mut [u8]) -> Result<bool, Error> {
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        if header.root == 0 {
            return Ok(false);
        }
        let old_root = header.root;

        self.read_into(DynBlockDblRef(old_root), buf)?;

        let mut next_buf = [0u8; 8];
        // next is at block offset 12 (after checksum 4 + prev 8).
        self.stack.get_into(old_root + 12, &mut next_buf)?;
        let next = u64::from_le_bytes(next_buf);

        header.root = next;
        if next != 0 {
            self.update_prev(next, 0)?;
        } else {
            header.tail = 0;
        }
        self.write_header_locked(&header)?;
        self.free_locked(DynBlockDblRef(old_root), &mut header)?;
        Ok(true)
    }

    /// Zero-copy variant of [`pop_back`](Self::pop_back).
    ///
    /// `buf.len()` must be ≥ the tail block's `data_len`; returns `true` if an
    /// item was popped, `false` if the list was empty.
    pub fn pop_back_into(&self, buf: &mut [u8]) -> Result<bool, Error> {
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        if header.tail == 0 {
            return Ok(false);
        }
        let old_tail = header.tail;

        self.read_into(DynBlockDblRef(old_tail), buf)?;

        let mut prev_buf = [0u8; 8];
        // prev is at block offset 4 (after checksum).
        self.stack.get_into(old_tail + 4, &mut prev_buf)?;
        let prev = u64::from_le_bytes(prev_buf);

        header.tail = prev;
        if prev != 0 {
            self.update_next(prev, 0)?;
        } else {
            header.root = 0;
        }
        self.write_header_locked(&header)?;
        self.free_locked(DynBlockDblRef(old_tail), &mut header)?;
        Ok(true)
    }

    // ── utility ───────────────────────────────────────────────────────────────

    /// Return a shared reference to the underlying [`BStack`].
    ///
    /// Only read-only operations (`get`, `get_into`, `peek`, `len`) are safe to
    /// call on the returned handle.  Mutating BStack operations can silently
    /// corrupt the list structure.
    pub fn bstack(&self) -> &BStack {
        &self.stack
    }

    /// Return the smallest power-of-two total block size that can hold `size`
    /// bytes of payload, including the 28-byte block header.
    ///
    /// The minimum returned value is 32 (2^[`MIN_BIN`]).
    ///
    /// ```
    /// use bllist::DynamicDblList;
    /// assert_eq!(DynamicDblList::block_size_for(0),  32); // 0+28=28 → 32
    /// assert_eq!(DynamicDblList::block_size_for(4),  32); // 4+28=32 → 32
    /// assert_eq!(DynamicDblList::block_size_for(5),  64); // 5+28=33 → 64
    /// assert_eq!(DynamicDblList::block_size_for(36), 64); // 36+28=64 → 64
    /// assert_eq!(DynamicDblList::block_size_for(37), 128); // 37+28=65 → 128
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

    fn read_header_locked(&self) -> Result<DynDblHeader, Error> {
        let mut buf = [0u8; 280];
        self.stack.get_into(0, &mut buf)?;
        DynDblHeader::from_bytes(&buf)
    }

    fn write_header_locked(&self, header: &DynDblHeader) -> Result<(), Error> {
        self.stack.set(0, &header.to_bytes())?;
        Ok(())
    }

    /// Write a free block: prev=0, next=`next`, block_size=`block_size`, data_len=0, payload=0.
    fn write_free_block_raw(&self, offset: u64, block_size: usize, next: u64) -> Result<(), Error> {
        let mut hc = [0u8; DynDblLayout::HEADER_CONTENT_SIZE];
        // prev = 0 (already zeroed)
        hc[8..16].copy_from_slice(&next.to_le_bytes());
        hc[16..20].copy_from_slice(&(block_size as u32).to_le_bytes());
        // data_len = 0 (already zeroed)
        Block::<DynDblLayout>::new(offset).write(
            &self.stack,
            &hc,
            block_size.saturating_sub(BLOCK_HEADER_SIZE),
            &[],
        )
    }

    fn alloc_locked(
        &self,
        block_size: usize,
        header: &mut DynDblHeader,
    ) -> Result<DynBlockDblRef, Error> {
        let target_bin = bin_index(block_size);

        if header.bin_heads[target_bin] != 0 {
            let bh = header.bin_heads[target_bin];
            let mut next_buf = [0u8; 8];
            // next at block offset 12 (checksum 4 + prev 8).
            self.stack.get_into(bh + 12, &mut next_buf)?;
            header.bin_heads[target_bin] = u64::from_le_bytes(next_buf);
            self.write_header_locked(header)?;
            return Ok(DynBlockDblRef(bh));
        }

        let search_limit = (target_bin + MAX_SPLIT).min(NUM_BINS - 1);
        for k in (target_bin + 1)..=search_limit {
            if header.bin_heads[k] != 0 {
                let bh = header.bin_heads[k];
                let mut next_buf = [0u8; 8];
                self.stack.get_into(bh + 12, &mut next_buf)?;
                header.bin_heads[k] = u64::from_le_bytes(next_buf);
                return self.split_to_bin(bh, k, target_bin, header);
            }
        }

        // Extend the file.
        let mut buf = vec![0u8; block_size];
        // block_size field at byte 20 within the block.
        buf[20..24].copy_from_slice(&(block_size as u32).to_le_bytes());
        block::write_checksum(&mut buf);
        let offset = self.stack.push(&buf)?;
        Ok(DynBlockDblRef(offset))
    }

    fn split_to_bin(
        &self,
        offset: u64,
        from_bin: usize,
        to_bin: usize,
        header: &mut DynDblHeader,
    ) -> Result<DynBlockDblRef, Error> {
        let mut cur_size = 1usize << from_bin;
        while bin_index(cur_size) > to_bin {
            let half = cur_size / 2;
            let upper = offset + half as u64;
            let upper_bin = bin_index(half);
            let old_head = header.bin_heads[upper_bin];
            self.write_free_block_raw(offset, half, 0)?;
            self.write_free_block_raw(upper, half, old_head)?;
            header.bin_heads[upper_bin] = upper;
            cur_size = half;
        }
        self.write_header_locked(header)?;
        Ok(DynBlockDblRef(offset))
    }

    fn free_locked(&self, block: DynBlockDblRef, header: &mut DynDblHeader) -> Result<(), Error> {
        let mut bs_buf = [0u8; 4];
        self.stack.get_into(block.0 + 20, &mut bs_buf)?; // block_size at byte 20
        let block_size = u32::from_le_bytes(bs_buf) as usize;
        let total = self.stack.len()?;
        if block.0 + block_size as u64 == total {
            let _ = self.stack.pop(block_size as u64)?;
            return Ok(());
        }
        let bin = bin_index(block_size);
        let old_bin_head = header.bin_heads[bin];
        self.write_free_block_raw(block.0, block_size, old_bin_head)?;
        header.bin_heads[bin] = block.0;
        self.write_header_locked(header)?;
        Ok(())
    }

    /// Write a full block with all fields set explicitly.
    fn write_block_raw(
        &self,
        offset: u64,
        block_size: usize,
        prev: u64,
        next: u64,
        data_len: u32,
        data: &[u8],
    ) -> Result<(), Error> {
        let mut hc = [0u8; DynDblLayout::HEADER_CONTENT_SIZE];
        hc[0..8].copy_from_slice(&prev.to_le_bytes());
        hc[8..16].copy_from_slice(&next.to_le_bytes());
        hc[16..20].copy_from_slice(&(block_size as u32).to_le_bytes());
        hc[20..24].copy_from_slice(&data_len.to_le_bytes());
        Block::<DynDblLayout>::new(offset).write(
            &self.stack,
            &hc,
            block_size.saturating_sub(BLOCK_HEADER_SIZE),
            data,
        )
    }

    /// Read, CRC-verify, and return `(prev, next, block_size, data)`.
    fn read_block_full_static(
        stack: &BStack,
        offset: u64,
    ) -> Result<(u64, u64, usize, Vec<u8>), Error> {
        let mut hdr = [0u8; BLOCK_HEADER_SIZE];
        stack.get_into(offset, &mut hdr)?;
        let block_size = u32::from_le_bytes(hdr[20..24].try_into().unwrap()) as usize;
        let data_len = u32::from_le_bytes(hdr[24..28].try_into().unwrap()) as usize;

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

        let (hc, full_payload) = Block::<DynDblLayout>::new(offset).read(stack, payload_cap)?;
        let prev = u64::from_le_bytes(hc[0..8].try_into().unwrap());
        let next = u64::from_le_bytes(hc[8..16].try_into().unwrap());
        let data = full_payload[..data_len].to_vec();
        Ok((prev, next, block_size, data))
    }

    /// Read a block from the head side: returns `(next, data)`.
    fn read_block_front(&self, offset: u64) -> Result<(u64, u64, Vec<u8>), Error> {
        Self::read_block_full_static(&self.stack, offset)
            .map(|(prev, next, _, data)| (prev, next, data))
    }

    /// Read a block from the tail side: returns `(prev, data)`.
    fn read_block_back(&self, offset: u64) -> Result<(u64, Vec<u8>), Error> {
        Self::read_block_full_static(&self.stack, offset).map(|(prev, _, _, data)| (prev, data))
    }

    fn update_next(&self, offset: u64, next: u64) -> Result<(), Error> {
        let b = Block::<DynDblLayout>::new(offset);
        let hc = b.read_header_unchecked(&self.stack)?;
        let block_size = u32::from_le_bytes(hc[16..20].try_into().unwrap()) as usize;
        let payload_cap = block_size.saturating_sub(BLOCK_HEADER_SIZE);
        let mut new_hc = [0u8; DynDblLayout::HEADER_CONTENT_SIZE];
        new_hc[0..8].copy_from_slice(&hc[0..8]); // preserve prev
        new_hc[8..16].copy_from_slice(&next.to_le_bytes());
        new_hc[16..].copy_from_slice(&hc[16..]); // preserve block_size + data_len
        b.update_header(&self.stack, &new_hc, payload_cap)
    }

    fn update_prev(&self, offset: u64, prev: u64) -> Result<(), Error> {
        let b = Block::<DynDblLayout>::new(offset);
        let hc = b.read_header_unchecked(&self.stack)?;
        let block_size = u32::from_le_bytes(hc[16..20].try_into().unwrap()) as usize;
        let payload_cap = block_size.saturating_sub(BLOCK_HEADER_SIZE);
        let mut new_hc = [0u8; DynDblLayout::HEADER_CONTENT_SIZE];
        new_hc[0..8].copy_from_slice(&prev.to_le_bytes());
        new_hc[8..].copy_from_slice(&hc[8..]); // preserve next + block_size + data_len
        b.update_header(&self.stack, &new_hc, payload_cap)
    }

    fn recover_orphans(stack: &BStack, header: &mut DynDblHeader, total: u64) -> Result<(), Error> {
        if total <= HEADER_SIZE {
            return Ok(());
        }

        let max_steps = ((total - HEADER_SIZE) / BLOCK_HEADER_SIZE as u64 + 1) as usize;

        // Walk active list (CRC-verified), find tail.
        let mut active: HashSet<u64> = HashSet::new();
        let mut actual_tail = 0u64;
        let mut cur = header.root;
        let mut steps = 0usize;
        while cur != 0 {
            if steps >= max_steps {
                return Err(Error::Corruption("cycle detected in active list".into()));
            }
            let (_, next, _, _) = Self::read_block_full_static(stack, cur)?;
            active.insert(cur);
            if next == 0 {
                actual_tail = cur;
            }
            cur = next;
            steps += 1;
        }

        // Rebuild tail.
        let tail_changed = if header.root == 0 {
            let changed = header.tail != 0;
            header.tail = 0;
            changed
        } else {
            let changed = header.tail != actual_tail;
            header.tail = actual_tail;
            changed
        };

        // Sequential scan: collect non-active blocks.
        let mut free_blocks: Vec<(u64, usize)> = Vec::new();
        let mut scan = HEADER_SIZE;
        while scan < total {
            if scan + BLOCK_HEADER_SIZE as u64 > total {
                break;
            }
            let mut bs_buf = [0u8; 4];
            stack.get_into(scan + 20, &mut bs_buf)?; // block_size at byte 20
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
            if tail_changed {
                stack.set(0, &header.to_bytes())?;
            }
            return Ok(());
        }

        // One-pass coalesce: merge adjacent runs whose combined size is a power of two.
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

        // Phase 1: zero all bin heads and flush (crash-safe).
        header.bin_heads = [0u64; NUM_BINS];
        stack.set(0, &header.to_bytes())?;

        // Phase 2: write each free block and rebuild bin_heads.
        for &(off, bs) in &merged {
            let bin = bin_index(bs);
            let old_head = header.bin_heads[bin];
            let mut hc = [0u8; DynDblLayout::HEADER_CONTENT_SIZE];
            // prev = 0, next = old_head, block_size = bs, data_len = 0
            hc[8..16].copy_from_slice(&old_head.to_le_bytes());
            hc[16..20].copy_from_slice(&(bs as u32).to_le_bytes());
            Block::<DynDblLayout>::new(off).write(
                stack,
                &hc,
                bs.saturating_sub(BLOCK_HEADER_SIZE),
                &[],
            )?;
            header.bin_heads[bin] = off;
        }

        // Phase 3: write the populated header (including corrected tail).
        stack.set(0, &header.to_bytes())?;

        Ok(())
    }
}

// ── DynDblIter ────────────────────────────────────────────────────────────────

/// A double-ended iterator over the blocks of a [`DynamicDblList`].
///
/// Each call to [`next`](Iterator::next) reads one block from the head side
/// (CRC-verified) and advances forward; each call to
/// [`next_back`](DoubleEndedIterator::next_back) reads one block from the tail
/// side and advances backward.
///
/// When both cursors converge on the same block, it is yielded exactly once.
///
/// The iterator holds a `&` reference to the list, preventing mutation during
/// traversal.  Obtain one by calling [`DynamicDblList::iter`].
pub struct DynDblIter<'a> {
    list: &'a DynamicDblList,
    front: Option<DynBlockDblRef>,
    back: Option<DynBlockDblRef>,
}

impl<'a> Iterator for DynDblIter<'a> {
    type Item = Result<Vec<u8>, crate::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let block = self.front?;
        if self.back == Some(block) {
            self.front = None;
            self.back = None;
        } else {
            self.front = match self.list.get_next(block) {
                Ok(next) => next,
                Err(e) => {
                    self.front = None;
                    self.back = None;
                    return Some(Err(e));
                }
            };
        }
        Some(self.list.read(block))
    }
}

impl<'a> DoubleEndedIterator for DynDblIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let block = self.back?;
        if self.front == Some(block) {
            self.front = None;
            self.back = None;
        } else {
            self.back = match self.list.get_prev(block) {
                Ok(prev) => prev,
                Err(e) => {
                    self.front = None;
                    self.back = None;
                    return Some(Err(e));
                }
            };
        }
        Some(self.list.read(block))
    }
}

impl DynamicDblList {
    /// Return a double-ended iterator over every block in the active list.
    ///
    /// Each item is `Result<Vec<u8>, Error>` containing exactly the bytes
    /// written to that block.  The iterator stops after the first error.
    ///
    /// Because [`DynDblIter`] implements [`DoubleEndedIterator`], you can use
    /// `.rev()` or mix [`next`](Iterator::next) /
    /// [`next_back`](DoubleEndedIterator::next_back) calls freely.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use bllist::DynamicDblList;
    ///
    /// let list = DynamicDblList::open("data.bldd")?;
    /// list.push_back(b"alpha")?;
    /// list.push_back(b"beta")?;
    ///
    /// for item in list.iter()?.rev() {
    ///     println!("{}", String::from_utf8_lossy(&item?));
    /// }
    /// # Ok::<(), bllist::Error>(())
    /// ```
    pub fn iter(&self) -> Result<DynDblIter<'_>, crate::Error> {
        let _g = self.mu.lock().unwrap();
        let header = self.read_header_locked()?;
        let front = if header.root == 0 {
            None
        } else {
            Some(DynBlockDblRef(header.root))
        };
        let back = if header.tail == 0 {
            None
        } else {
            Some(DynBlockDblRef(header.tail))
        };
        Ok(DynDblIter {
            list: self,
            front,
            back,
        })
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
            "bllist_dyndbl_{}_{}_{}.bldd",
            std::process::id(),
            label,
            n
        ));
        p
    }

    // ── block_size_for ────────────────────────────────────────────────────────

    #[test]
    fn block_size_for_values() {
        assert_eq!(DynamicDblList::block_size_for(0), 32); // 28+0=28 → 32
        assert_eq!(DynamicDblList::block_size_for(4), 32); // 28+4=32 → 32
        assert_eq!(DynamicDblList::block_size_for(5), 64); // 28+5=33 → 64
        assert_eq!(DynamicDblList::block_size_for(36), 64); // 28+36=64 → 64
        assert_eq!(DynamicDblList::block_size_for(37), 128); // 28+37=65 → 128
        assert_eq!(DynamicDblList::block_size_for(100), 128); // 28+100=128 → 128
        assert_eq!(DynamicDblList::block_size_for(101), 256); // 28+101=129 → 256
    }

    // ── open / fresh file ─────────────────────────────────────────────────────

    #[test]
    fn fresh_open_empty() {
        let path = tmp("fresh");
        let list = DynamicDblList::open(&path).unwrap();
        assert_eq!(list.root().unwrap(), None);
        assert_eq!(list.tail().unwrap(), None);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_rejects_fixed_list_file() {
        let path = tmp("crosstype");
        {
            crate::FixedBlockList::<52>::open(&path).unwrap();
        }
        let err = DynamicDblList::open(&path).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_rejects_singly_dynamic_file() {
        let path = tmp("crosstype_dyn");
        {
            crate::DynamicBlockList::open(&path).unwrap();
        }
        let err = DynamicDblList::open(&path).unwrap_err();
        assert!(matches!(err, Error::Corruption(_)));
        let _ = std::fs::remove_file(&path);
    }

    // ── alloc / free / bin reuse ──────────────────────────────────────────────

    #[test]
    fn alloc_free_reuse_same_bin() {
        let path = tmp("reuse");
        let list = DynamicDblList::open(&path).unwrap();

        let b0 = list.alloc(1).unwrap(); // block_size = 32, bin 5
        let b1 = list.alloc(1).unwrap();
        let b2 = list.alloc(1).unwrap();

        list.free(b1).unwrap();
        let b3 = list.alloc(1).unwrap();
        assert_eq!(b3, b1);

        let _ = (b0, b2);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn alloc_different_sizes() {
        let path = tmp("bins");
        let list = DynamicDblList::open(&path).unwrap();

        let small = list.alloc(4).unwrap(); // 28+4=32 → payload_cap=4
        let large = list.alloc(36).unwrap(); // 28+36=64 → payload_cap=36

        assert_eq!(list.capacity(small).unwrap(), 4);
        assert_eq!(list.capacity(large).unwrap(), 36);

        let _ = (small, large);
        let _ = std::fs::remove_file(&path);
    }

    // ── write / read round-trip ───────────────────────────────────────────────

    #[test]
    fn write_read_roundtrip() {
        let path = tmp("rw");
        let list = DynamicDblList::open(&path).unwrap();
        let block = list.alloc(10).unwrap();

        list.write(block, b"hello dbl!").unwrap();
        let out = list.read(block).unwrap();
        assert_eq!(out, b"hello dbl!");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn write_updates_data_len() {
        let path = tmp("datalen");
        let list = DynamicDblList::open(&path).unwrap();
        let block = list.alloc(4).unwrap(); // payload_cap = 4

        assert_eq!(list.data_len(block).unwrap(), 0);
        list.write(block, b"hi!!").unwrap();
        assert_eq!(list.data_len(block).unwrap(), 4);

        let _ = std::fs::remove_file(&path);
    }

    // ── set_next / get_next / set_prev / get_prev ─────────────────────────────

    #[test]
    fn set_get_pointers() {
        let path = tmp("ptrs");
        let list = DynamicDblList::open(&path).unwrap();
        let b0 = list.alloc(1).unwrap();
        let b1 = list.alloc(1).unwrap();

        assert_eq!(list.get_next(b0).unwrap(), None);
        assert_eq!(list.get_prev(b0).unwrap(), None);

        list.set_next(b0, Some(b1)).unwrap();
        list.set_prev(b1, Some(b0)).unwrap();
        assert_eq!(list.get_next(b0).unwrap(), Some(b1));
        assert_eq!(list.get_prev(b1).unwrap(), Some(b0));

        list.set_next(b0, None).unwrap();
        list.set_prev(b1, None).unwrap();
        assert_eq!(list.get_next(b0).unwrap(), None);
        assert_eq!(list.get_prev(b1).unwrap(), None);

        let _ = std::fs::remove_file(&path);
    }

    // ── push_front / pop_front (LIFO) ─────────────────────────────────────────

    #[test]
    fn push_front_pop_front_lifo() {
        let path = tmp("lifo");
        let list = DynamicDblList::open(&path).unwrap();

        list.push_front(b"first").unwrap();
        list.push_front(b"second longer").unwrap();
        list.push_front(b"third").unwrap();

        assert_eq!(list.pop_front().unwrap().unwrap(), b"third");
        assert_eq!(list.pop_front().unwrap().unwrap(), b"second longer");
        assert_eq!(list.pop_front().unwrap().unwrap(), b"first");
        assert_eq!(list.pop_front().unwrap(), None);
        assert_eq!(list.root().unwrap(), None);
        assert_eq!(list.tail().unwrap(), None);

        let _ = std::fs::remove_file(&path);
    }

    // ── push_back / pop_back ──────────────────────────────────────────────────

    #[test]
    fn push_back_pop_back_lifo() {
        let path = tmp("back_lifo");
        let list = DynamicDblList::open(&path).unwrap();

        list.push_back(b"first").unwrap();
        list.push_back(b"second").unwrap();
        list.push_back(b"third").unwrap();

        assert_eq!(list.pop_back().unwrap().unwrap(), b"third");
        assert_eq!(list.pop_back().unwrap().unwrap(), b"second");
        assert_eq!(list.pop_back().unwrap().unwrap(), b"first");
        assert_eq!(list.pop_back().unwrap(), None);
        assert_eq!(list.root().unwrap(), None);
        assert_eq!(list.tail().unwrap(), None);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn push_back_pop_front_fifo() {
        let path = tmp("fifo");
        let list = DynamicDblList::open(&path).unwrap();

        list.push_back(b"first").unwrap();
        list.push_back(b"second longer").unwrap();
        list.push_back(b"third").unwrap();

        assert_eq!(list.pop_front().unwrap().unwrap(), b"first");
        assert_eq!(list.pop_front().unwrap().unwrap(), b"second longer");
        assert_eq!(list.pop_front().unwrap().unwrap(), b"third");
        assert_eq!(list.pop_front().unwrap(), None);

        let _ = std::fs::remove_file(&path);
    }

    // ── root/tail consistency ─────────────────────────────────────────────────

    #[test]
    fn root_tail_one_element() {
        let path = tmp("one_elem");
        let list = DynamicDblList::open(&path).unwrap();
        list.push_back(b"only").unwrap();

        let root = list.root().unwrap().unwrap();
        let tail = list.tail().unwrap().unwrap();
        assert_eq!(root, tail);

        list.pop_front().unwrap();
        assert_eq!(list.root().unwrap(), None);
        assert_eq!(list.tail().unwrap(), None);

        let _ = std::fs::remove_file(&path);
    }

    // ── pop_front_into / pop_back_into ────────────────────────────────────────

    #[test]
    fn pop_front_into_basic() {
        let path = tmp("pop_front_into");
        let list = DynamicDblList::open(&path).unwrap();
        list.push_front(b"hello").unwrap();

        let mut buf = vec![0u8; 5];
        assert!(list.pop_front_into(&mut buf).unwrap());
        assert_eq!(buf, b"hello");
        assert!(!list.pop_front_into(&mut buf).unwrap());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pop_back_into_basic() {
        let path = tmp("pop_back_into");
        let list = DynamicDblList::open(&path).unwrap();
        list.push_back(b"world").unwrap();

        let mut buf = vec![0u8; 5];
        assert!(list.pop_back_into(&mut buf).unwrap());
        assert_eq!(buf, b"world");
        assert!(!list.pop_back_into(&mut buf).unwrap());

        let _ = std::fs::remove_file(&path);
    }

    // ── splitting ─────────────────────────────────────────────────────────────

    #[test]
    fn split_one_level() {
        let path = tmp("split1");
        let list = DynamicDblList::open(&path).unwrap();

        let large = list.alloc(36).unwrap(); // 28+36=64, bin 6
        let large_off = large.0;
        list.free(large).unwrap();

        let small = list.alloc(1).unwrap(); // 28+1=29 → 32, bin 5 (split from bin 6)
        assert_eq!(small.0, large_off);
        assert_eq!(list.capacity(small).unwrap(), 4); // 32-28=4

        let small2 = list.alloc(1).unwrap();
        assert_eq!(small2.0, large_off + 32);

        let _ = std::fs::remove_file(&path);
    }

    // ── double-ended iterator ─────────────────────────────────────────────────

    #[test]
    fn iter_forward() {
        let path = tmp("iter_fwd");
        let list = DynamicDblList::open(&path).unwrap();
        list.push_back(b"alpha").unwrap();
        list.push_back(b"beta").unwrap();
        list.push_back(b"gamma").unwrap();

        let items: Vec<_> = list.iter().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(items[0], b"alpha");
        assert_eq!(items[1], b"beta");
        assert_eq!(items[2], b"gamma");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn iter_backward() {
        let path = tmp("iter_bwd");
        let list = DynamicDblList::open(&path).unwrap();
        list.push_back(b"alpha").unwrap();
        list.push_back(b"beta").unwrap();
        list.push_back(b"gamma").unwrap();

        let items: Vec<_> = list.iter().unwrap().rev().map(|r| r.unwrap()).collect();
        assert_eq!(items[0], b"gamma");
        assert_eq!(items[1], b"beta");
        assert_eq!(items[2], b"alpha");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn iter_mixed_ends() {
        let path = tmp("iter_mixed");
        let list = DynamicDblList::open(&path).unwrap();
        list.push_back(b"A").unwrap();
        list.push_back(b"B").unwrap();
        list.push_back(b"C").unwrap();
        list.push_back(b"D").unwrap();

        let mut it = list.iter().unwrap();
        assert_eq!(it.next().unwrap().unwrap(), b"A");
        assert_eq!(it.next_back().unwrap().unwrap(), b"D");
        assert_eq!(it.next().unwrap().unwrap(), b"B");
        assert_eq!(it.next_back().unwrap().unwrap(), b"C");
        assert!(it.next().is_none());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn iter_mixed_ends_odd() {
        let path = tmp("iter_odd");
        let list = DynamicDblList::open(&path).unwrap();
        list.push_back(b"A").unwrap();
        list.push_back(b"B").unwrap();
        list.push_back(b"C").unwrap();

        let mut it = list.iter().unwrap();
        assert_eq!(it.next().unwrap().unwrap(), b"A");
        assert_eq!(it.next_back().unwrap().unwrap(), b"C");
        assert_eq!(it.next().unwrap().unwrap(), b"B"); // last element, yielded from front
        assert!(it.next().is_none());
        assert!(it.next_back().is_none());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn iter_empty() {
        let path = tmp("iter_empty");
        let list = DynamicDblList::open(&path).unwrap();
        let mut it = list.iter().unwrap();
        assert!(it.next().is_none());
        assert!(it.next_back().is_none());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn iter_single() {
        let path = tmp("iter_single");
        let list = DynamicDblList::open(&path).unwrap();
        list.push_back(b"only").unwrap();

        let mut it = list.iter().unwrap();
        assert_eq!(it.next().unwrap().unwrap(), b"only");
        assert!(it.next().is_none());
        assert!(it.next_back().is_none());

        let _ = std::fs::remove_file(&path);
    }

    // ── orphan recovery ───────────────────────────────────────────────────────

    #[test]
    fn orphan_recovery() {
        let path = tmp("orphan");
        let orphan_offset;
        {
            let stack = BStack::open(&path).unwrap();
            let hdr = DynDblHeader {
                root: 0,
                tail: 0,
                bin_heads: [0u64; NUM_BINS],
            };
            let off = stack.push(&hdr.to_bytes()).unwrap();
            assert_eq!(off, 0);

            let bs: usize = 32;
            let mut block_buf = vec![0u8; bs];
            block_buf[20..24].copy_from_slice(&(bs as u32).to_le_bytes()); // block_size
            let crc = crc32fast::hash(&block_buf[4..]);
            block_buf[0..4].copy_from_slice(&crc.to_le_bytes());
            orphan_offset = stack.push(&block_buf).unwrap();
        }

        let list = DynamicDblList::open(&path).unwrap();
        assert_eq!(list.root().unwrap(), None);
        let b = list.alloc(1).unwrap();
        assert_eq!(b.0, orphan_offset);

        let _ = std::fs::remove_file(&path);
    }

    // ── coalescing on open ────────────────────────────────────────────────────

    #[test]
    fn coalesce_two_adjacent_free_blocks() {
        let path = tmp("coalesce2");
        {
            let list = DynamicDblList::open(&path).unwrap();
            let b0 = list.alloc(1).unwrap(); // 32 bytes, bin 5
            let b1 = list.alloc(1).unwrap(); // 32 bytes, bin 5
            assert_eq!(b1.0, b0.0 + 32);
            let _wall = list.push_front(b"wall").unwrap();
            list.free(b0).unwrap();
            list.free(b1).unwrap();
        }

        let list = DynamicDblList::open(&path).unwrap();
        // Two adjacent 32-byte blocks coalesce into one 64-byte block.
        let big = list.alloc(36).unwrap(); // 28+36=64, bin 6
        assert_eq!(big.0, HEADER_SIZE);

        let _ = std::fs::remove_file(&path);
    }

    // ── persistence across reopen ─────────────────────────────────────────────

    #[test]
    fn reopen_persists_data() {
        let path = tmp("reopen");
        {
            let list = DynamicDblList::open(&path).unwrap();
            list.push_back(b"persisted across reopen").unwrap();
        }
        {
            let list = DynamicDblList::open(&path).unwrap();
            let data = list.pop_front().unwrap().unwrap();
            assert_eq!(data, b"persisted across reopen");
            assert_eq!(list.pop_front().unwrap(), None);
        }
        let _ = std::fs::remove_file(&path);
    }

    // ── error paths ───────────────────────────────────────────────────────────

    #[test]
    fn data_too_large() {
        let path = tmp("toolarge");
        let list = DynamicDblList::open(&path).unwrap();
        let block = list.alloc(1).unwrap(); // payload_cap = 4
        let err = list.write(block, &[0u8; 5]).unwrap_err();
        assert!(matches!(err, Error::DataTooLarge { .. }));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn invalid_block_offset() {
        let path = tmp("invalid");
        let list = DynamicDblList::open(&path).unwrap();
        let err = list.read(DynBlockDblRef(0)).unwrap_err();
        assert!(matches!(err, Error::InvalidBlock));
        let _ = std::fs::remove_file(&path);
    }
}
