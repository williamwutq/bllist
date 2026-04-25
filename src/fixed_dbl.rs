use std::collections::HashSet;
use std::path::Path;
use std::sync::Mutex;
use std::{fmt, vec};

use crate::Error;
use crate::block::{self, Block, BlockLayout};
use bstack::BStack;

// ── block layout ──────────────────────────────────────────────────────────────

pub(crate) struct FixedDblLayout;
impl BlockLayout for FixedDblLayout {
    /// prev pointer (8 bytes) + next pointer (8 bytes).
    const HEADER_CONTENT_SIZE: usize = 16;
}

// ── on-disk constants ─────────────────────────────────────────────────────────

const MAGIC: [u8; 4] = *b"BLDF";
const VERSION: u32 = 1;

/// Size of the bllist-doubly-fixed file header at logical offset 0 (bytes).
///
/// `4` (magic) + `4` (version) + `8` (root) + `8` (tail) + `8` (free_head) = 32.
const HEADER_SIZE: u64 = 32;

/// Byte size of the per-block header: 4-byte checksum + 8-byte prev + 8-byte next.
const BLOCK_HEADER_SIZE: usize = 4 + FixedDblLayout::HEADER_CONTENT_SIZE;

// ── BlockDblRef ───────────────────────────────────────────────────────────────

/// A handle to a block in a [`FixedDblList`], encoded as the block's logical
/// byte offset within the underlying BStack file.
///
/// `BlockDblRef` is `Copy` and cheap to store; treat it like a typed index. An
/// offset of `0` is never a valid block (logical offset 0 is the file header)
/// and is used internally to represent *null* / end-of-list.
///
/// Formatted as `@offset` (decimal), `@hex` (lower-case hex via `{:x}`), or
/// `@HEX` (upper-case hex via `{:X}`). The `#` flag adds `0x` after the `@`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlockDblRef(pub u64);

crate::block::impl_block_ref!(BlockDblRef);

// ── Header (in-memory mirror of the 32-byte on-disk header) ──────────────────

struct Header {
    root: u64,
    tail: u64,
    free_head: u64,
}

impl Header {
    fn from_bytes(buf: &[u8; 32]) -> Result<Self, Error> {
        if buf[0..4] != MAGIC {
            return Err(Error::Corruption(format!(
                "invalid magic bytes: expected {:?} (\"BLDF\"), found {:?}",
                MAGIC,
                &buf[0..4]
            )));
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if version != VERSION {
            return Err(Error::Corruption(format!(
                "unsupported fixed-doubly-linked version {version}, expected {VERSION}"
            )));
        }
        Ok(Self {
            root: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            tail: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
            free_head: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
        })
    }

    fn to_bytes(&self) -> [u8; 32] {
        let mut buf = [0u8; 32];
        buf[0..4].copy_from_slice(&MAGIC);
        buf[4..8].copy_from_slice(&VERSION.to_le_bytes());
        buf[8..16].copy_from_slice(&self.root.to_le_bytes());
        buf[16..24].copy_from_slice(&self.tail.to_le_bytes());
        buf[24..32].copy_from_slice(&self.free_head.to_le_bytes());
        buf
    }
}

// ── FixedDblList ──────────────────────────────────────────────────────────────

/// A crash-safe, checksummed **doubly-linked** list of fixed-size blocks stored
/// in a single [BStack] file.
///
/// The const generic `PAYLOAD_CAPACITY` is the number of **payload bytes** per
/// block. Each block occupies `PAYLOAD_CAPACITY + 20` bytes on disk:
///
/// | Offset | Size | Field | Description |
/// |--------|------|-------|-------------|
/// | 0 | 4 | `checksum` | CRC32 of bytes `[4..PAYLOAD_CAPACITY+20]` |
/// | 4 | 8 | `prev` | Logical offset of the preceding block; `0` = null |
/// | 12 | 8 | `next` | Logical offset of the following block; `0` = null |
/// | 20 | `PAYLOAD_CAPACITY` | `payload` | User data, zero-padded to capacity |
///
/// # File format
///
/// The on-disk file uses magic bytes `"BLDF"` and cannot be opened by
/// [`FixedBlockList`](crate::FixedBlockList) (magic `"BLLS"`) or either dynamic
/// list type.  The file header (32 bytes at logical offset 0) stores the root
/// pointer, the tail pointer, and the free-list head pointer.
///
/// # Operations
///
/// In addition to [`push_front`](Self::push_front) / [`pop_front`](Self::pop_front)
/// (same semantics as the singly-linked list), `FixedDblList` supports
/// [`push_back`](Self::push_back) / [`pop_back`](Self::pop_back) in O(1) via
/// the stored tail pointer.
///
/// # Iteration
///
/// [`iter`](Self::iter) returns a [`FixedDblIter`] that implements both
/// [`Iterator`] (forward, head→tail) and [`DoubleEndedIterator`] (backward,
/// tail→head).  The iterator captures the root and tail at construction time;
/// the list may not be mutated while the iterator is alive (enforced by the
/// `&` reference).
///
/// # Thread safety
///
/// `FixedDblList` is `Send + Sync`. All header-mutating operations are
/// serialised through an internal [`Mutex`].
///
/// # Example
///
/// ```no_run
/// use bllist::FixedDblList;
///
/// let list = FixedDblList::<52>::open("data.bldf")?;
///
/// list.push_back(b"first")?;
/// list.push_back(b"second")?;
/// list.push_back(b"third")?;
///
/// // Forward iteration: first, second, third.
/// for item in list.iter()? {
///     println!("{}", String::from_utf8_lossy(&item?));
/// }
///
/// // Backward iteration (DoubleEndedIterator).
/// let rev: Vec<_> = list.iter()?.rev().collect();
///
/// assert_eq!(list.pop_front()?.as_deref(), Some(&b"first"[..]));
/// assert_eq!(list.pop_back()?.as_deref(), Some(&b"third"[..]));
/// # Ok::<(), bllist::Error>(())
/// ```
pub struct FixedDblList<const PAYLOAD_CAPACITY: usize> {
    stack: BStack,
    mu: Mutex<()>,
}

impl<const PAYLOAD_CAPACITY: usize> fmt::Debug for FixedDblList<PAYLOAD_CAPACITY> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FixedDblList")
            .field("payload_capacity", &PAYLOAD_CAPACITY)
            .field("block_size", &(PAYLOAD_CAPACITY + BLOCK_HEADER_SIZE))
            .finish_non_exhaustive()
    }
}

impl<const PAYLOAD_CAPACITY: usize> fmt::Display for FixedDblList<PAYLOAD_CAPACITY> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FixedDblList<{PAYLOAD_CAPACITY}>")
    }
}

impl<const PAYLOAD_CAPACITY: usize> FixedDblList<PAYLOAD_CAPACITY> {
    // ── public API ────────────────────────────────────────────────────────────

    /// Open or create the file at `path` as a [`FixedDblList`].
    ///
    /// * **New file** – creates the file, writes the header, and returns an
    ///   empty list.
    /// * **Existing file** – validates the header (magic `"BLDF"`, version),
    ///   then performs crash recovery: walks the active and free lists,
    ///   reclaims orphaned block slots, and rebuilds the tail pointer.
    ///
    /// # Errors
    ///
    /// | Error | Cause |
    /// |-------|-------|
    /// | [`Error::Io`] | File could not be opened, created, or locked |
    /// | [`Error::Corruption`] | Wrong magic bytes or unsupported version |
    /// | [`Error::ChecksumMismatch`] | A block in the active list has a bad CRC |
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        const {
            assert!(
                PAYLOAD_CAPACITY > 0,
                "PAYLOAD_CAPACITY must be greater than 0 \
                 (block header already occupies 20 bytes)"
            )
        };

        let stack = BStack::open(path)?;
        let total = stack.len()?;

        if total == 0 {
            let h = Header {
                root: 0,
                tail: 0,
                free_head: 0,
            };
            let offset = stack.push(&h.to_bytes())?;
            debug_assert_eq!(offset, 0, "bllist-dbl header must land at logical offset 0");
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

        let mut hdr_buf = [0u8; 32];
        stack.get_into(0, &mut hdr_buf)?;
        let mut header = Header::from_bytes(&hdr_buf)?;

        Self::recover_orphans(&stack, &mut header, total)?;

        Ok(Self {
            stack,
            mu: Mutex::new(()),
        })
    }

    /// Returns the number of payload bytes available in each block.
    pub const fn payload_capacity() -> usize {
        PAYLOAD_CAPACITY
    }

    /// Allocate a new block from the free list, or grow the file if the free
    /// list is empty.
    ///
    /// The returned [`BlockDblRef`] points to an initialised block whose payload
    /// is zeroed and whose prev/next pointers are null (or stale free-list values
    /// if reclaimed from the free list).  Call [`write`](Self::write) to store
    /// data and [`set_next`](Self::set_next) / [`set_prev`](Self::set_prev) to
    /// link it into a list.  [`push_front`](Self::push_front) and
    /// [`push_back`](Self::push_back) handle linking automatically.
    pub fn alloc(&self) -> Result<BlockDblRef, Error> {
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        self.alloc_locked(&mut header)
    }

    /// Return `block` to the free list.
    ///
    /// If the block is the last block in the file it is popped from the BStack,
    /// shrinking the file.  Otherwise the block is zeroed and linked into the
    /// free list.  After this call the `BlockDblRef` is invalid.
    pub fn free(&self, block: BlockDblRef) -> Result<(), Error> {
        self.validate_block_offset(block.0)?;
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        self.free_locked(block, &mut header)
    }

    /// Write `data` into the payload of `block`.
    ///
    /// The prev and next pointers are preserved.  Bytes beyond `data.len()` are
    /// zeroed.  The checksum is recomputed atomically.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DataTooLarge`] if `data.len() > PAYLOAD_CAPACITY`.
    pub fn write(&self, block: BlockDblRef, data: &[u8]) -> Result<(), Error> {
        self.validate_block_offset(block.0)?;
        Block::<FixedDblLayout>::new(block.0).write_payload(&self.stack, PAYLOAD_CAPACITY, data)
    }

    /// Read the payload of `block` into a freshly allocated `Vec<u8>`.
    ///
    /// Always returns `PAYLOAD_CAPACITY` bytes (zero-padded beyond the last
    /// write).  The checksum is verified before returning.
    pub fn read(&self, block: BlockDblRef) -> Result<Vec<u8>, Error> {
        let mut buf = vec![0u8; PAYLOAD_CAPACITY];
        self.read_into(block, &mut buf)?;
        Ok(buf)
    }

    /// Zero-copy variant of [`read`](Self::read).
    ///
    /// Reads the payload directly into `buf` after CRC verification.
    /// `buf.len()` must be ≤ `PAYLOAD_CAPACITY`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DataTooLarge`] if `buf.len() > PAYLOAD_CAPACITY`, or
    /// [`Error::ChecksumMismatch`] on CRC failure.
    pub fn read_into(&self, block: BlockDblRef, buf: &mut [u8]) -> Result<(), Error> {
        self.validate_block_offset(block.0)?;
        Block::<FixedDblLayout>::new(block.0).read_payload_into(
            &self.stack,
            PAYLOAD_CAPACITY,
            buf,
        )?;
        Ok(())
    }

    /// Update the next-block pointer of `block`.  The payload and prev pointer
    /// are preserved.  The checksum is recomputed atomically.
    pub fn set_next(&self, block: BlockDblRef, next: Option<BlockDblRef>) -> Result<(), Error> {
        self.validate_block_offset(block.0)?;
        self.update_next(block.0, next.map(|r| r.0).unwrap_or(0))
    }

    /// Update the prev-block pointer of `block`.  The payload and next pointer
    /// are preserved.  The checksum is recomputed atomically.
    pub fn set_prev(&self, block: BlockDblRef, prev: Option<BlockDblRef>) -> Result<(), Error> {
        self.validate_block_offset(block.0)?;
        self.update_prev(block.0, prev.map(|r| r.0).unwrap_or(0))
    }

    /// Return the next-block pointer of `block`, or `None` if `block` is the
    /// tail.  Does **not** verify the checksum.
    pub fn get_next(&self, block: BlockDblRef) -> Result<Option<BlockDblRef>, Error> {
        self.validate_block_offset(block.0)?;
        let hc = Block::<FixedDblLayout>::new(block.0).read_header_unchecked(&self.stack)?;
        let next = u64::from_le_bytes(hc[8..16].try_into().unwrap());
        Ok(if next == 0 {
            None
        } else {
            Some(BlockDblRef(next))
        })
    }

    /// Return the prev-block pointer of `block`, or `None` if `block` is the
    /// head.  Does **not** verify the checksum.
    pub fn get_prev(&self, block: BlockDblRef) -> Result<Option<BlockDblRef>, Error> {
        self.validate_block_offset(block.0)?;
        let hc = Block::<FixedDblLayout>::new(block.0).read_header_unchecked(&self.stack)?;
        let prev = u64::from_le_bytes(hc[0..8].try_into().unwrap());
        Ok(if prev == 0 {
            None
        } else {
            Some(BlockDblRef(prev))
        })
    }

    /// Return the head of the active list, or `None` if the list is empty.
    pub fn root(&self) -> Result<Option<BlockDblRef>, Error> {
        let _g = self.mu.lock().unwrap();
        let header = self.read_header_locked()?;
        Ok(if header.root == 0 {
            None
        } else {
            Some(BlockDblRef(header.root))
        })
    }

    /// Return the tail of the active list, or `None` if the list is empty.
    pub fn tail(&self) -> Result<Option<BlockDblRef>, Error> {
        let _g = self.mu.lock().unwrap();
        let header = self.read_header_locked()?;
        Ok(if header.tail == 0 {
            None
        } else {
            Some(BlockDblRef(header.tail))
        })
    }

    /// Allocate a block, write `data` to it, and prepend it to the list.
    ///
    /// The new block becomes the new head.  If the list was empty, it also
    /// becomes the tail.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DataTooLarge`] if `data.len() > PAYLOAD_CAPACITY`.
    pub fn push_front(&self, data: &[u8]) -> Result<BlockDblRef, Error> {
        if data.len() > PAYLOAD_CAPACITY {
            return Err(Error::DataTooLarge {
                capacity: PAYLOAD_CAPACITY,
                provided: data.len(),
            });
        }
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        let old_root = header.root;

        let new_block = self.alloc_locked(&mut header)?;
        // Write new block: prev=0, next=old_root, data.
        self.write_block_full(new_block.0, 0, old_root, data)?;
        // Point old root's prev to new block.
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

    /// Allocate a block, write `data` to it, and append it to the list.
    ///
    /// The new block becomes the new tail.  If the list was empty, it also
    /// becomes the head.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DataTooLarge`] if `data.len() > PAYLOAD_CAPACITY`.
    pub fn push_back(&self, data: &[u8]) -> Result<BlockDblRef, Error> {
        if data.len() > PAYLOAD_CAPACITY {
            return Err(Error::DataTooLarge {
                capacity: PAYLOAD_CAPACITY,
                provided: data.len(),
            });
        }
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        let old_tail = header.tail;

        let new_block = self.alloc_locked(&mut header)?;
        // Write new block: prev=old_tail, next=0, data.
        self.write_block_full(new_block.0, old_tail, 0, data)?;
        // Point old tail's next to new block.
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

    /// Remove and return the payload of the head block, or `None` if empty.
    pub fn pop_front(&self) -> Result<Option<Vec<u8>>, Error> {
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        if header.root == 0 {
            return Ok(None);
        }
        let old_root = header.root;
        let (_, next, payload) = self.read_block_full(old_root)?;

        header.root = next;
        if next != 0 {
            self.update_prev(next, 0)?;
        } else {
            // List is now empty.
            header.tail = 0;
        }
        self.write_header_locked(&header)?;
        self.free_locked(BlockDblRef(old_root), &mut header)?;
        Ok(Some(payload))
    }

    /// Remove and return the payload of the tail block, or `None` if empty.
    pub fn pop_back(&self) -> Result<Option<Vec<u8>>, Error> {
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        if header.tail == 0 {
            return Ok(None);
        }
        let old_tail = header.tail;
        let (prev, _, payload) = self.read_block_full(old_tail)?;

        header.tail = prev;
        if prev != 0 {
            self.update_next(prev, 0)?;
        } else {
            // List is now empty.
            header.root = 0;
        }
        self.write_header_locked(&header)?;
        self.free_locked(BlockDblRef(old_tail), &mut header)?;
        Ok(Some(payload))
    }

    /// Zero-copy variant of [`pop_front`](Self::pop_front).
    ///
    /// Reads the head block payload into `buf`, unlinks the head, and frees
    /// the block.  Returns `true` if a block was popped, `false` if the list
    /// was empty.
    ///
    /// `buf.len()` must be ≤ `PAYLOAD_CAPACITY`.
    pub fn pop_front_into(&self, buf: &mut [u8]) -> Result<bool, Error> {
        if buf.len() > PAYLOAD_CAPACITY {
            return Err(Error::DataTooLarge {
                capacity: PAYLOAD_CAPACITY,
                provided: buf.len(),
            });
        }
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        if header.root == 0 {
            return Ok(false);
        }
        let old_root = header.root;

        let hc = Block::<FixedDblLayout>::new(old_root).read_payload_into(
            &self.stack,
            PAYLOAD_CAPACITY,
            buf,
        )?;
        let next = u64::from_le_bytes(hc[8..16].try_into().unwrap());

        header.root = next;
        if next != 0 {
            self.update_prev(next, 0)?;
        } else {
            header.tail = 0;
        }
        self.write_header_locked(&header)?;
        self.free_locked(BlockDblRef(old_root), &mut header)?;
        Ok(true)
    }

    /// Zero-copy variant of [`pop_back`](Self::pop_back).
    ///
    /// Returns `true` if a block was popped, `false` if the list was empty.
    /// `buf.len()` must be ≤ `PAYLOAD_CAPACITY`.
    pub fn pop_back_into(&self, buf: &mut [u8]) -> Result<bool, Error> {
        if buf.len() > PAYLOAD_CAPACITY {
            return Err(Error::DataTooLarge {
                capacity: PAYLOAD_CAPACITY,
                provided: buf.len(),
            });
        }
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        if header.tail == 0 {
            return Ok(false);
        }
        let old_tail = header.tail;

        let hc = Block::<FixedDblLayout>::new(old_tail).read_payload_into(
            &self.stack,
            PAYLOAD_CAPACITY,
            buf,
        )?;
        let prev = u64::from_le_bytes(hc[0..8].try_into().unwrap());

        header.tail = prev;
        if prev != 0 {
            self.update_next(prev, 0)?;
        } else {
            header.root = 0;
        }
        self.write_header_locked(&header)?;
        self.free_locked(BlockDblRef(old_tail), &mut header)?;
        Ok(true)
    }

    // ── private helpers ───────────────────────────────────────────────────────

    fn validate_block_offset(&self, offset: u64) -> Result<(), Error> {
        let block_size = (PAYLOAD_CAPACITY + BLOCK_HEADER_SIZE) as u64;
        if offset < HEADER_SIZE || !(offset - HEADER_SIZE).is_multiple_of(block_size) {
            return Err(Error::InvalidBlock);
        }
        Ok(())
    }

    fn read_header_locked(&self) -> Result<Header, Error> {
        let mut buf = [0u8; 32];
        self.stack.get_into(0, &mut buf)?;
        Header::from_bytes(&buf)
    }

    fn write_header_locked(&self, header: &Header) -> Result<(), Error> {
        self.stack.set(0, &header.to_bytes())?;
        Ok(())
    }

    fn alloc_locked(&self, header: &mut Header) -> Result<BlockDblRef, Error> {
        if header.free_head != 0 {
            let fh = header.free_head;
            let mut next_buf = [0u8; 8];
            // In a free block: header_content = [prev=0(8)][next=free_link(8)].
            // next is at block offset 4+8 = 12.
            self.stack.get_into(fh + 12, &mut next_buf)?;
            header.free_head = u64::from_le_bytes(next_buf);
            self.write_header_locked(header)?;
            Ok(BlockDblRef(fh))
        } else {
            let block_size = PAYLOAD_CAPACITY + BLOCK_HEADER_SIZE;
            let mut buf = vec![0u8; block_size];
            block::write_checksum(&mut buf);
            let offset = self.stack.push(&buf)?;
            debug_assert!(
                offset >= HEADER_SIZE && (offset - HEADER_SIZE).is_multiple_of(block_size as u64),
                "newly pushed block has misaligned offset {offset}"
            );
            Ok(BlockDblRef(offset))
        }
    }

    fn free_locked(&self, block: BlockDblRef, header: &mut Header) -> Result<(), Error> {
        let block_size = (PAYLOAD_CAPACITY + BLOCK_HEADER_SIZE) as u64;
        let total = self.stack.len()?;
        if block.0 + block_size == total {
            let _ = self.stack.pop(block_size)?;
            return Ok(());
        }
        // Free block: prev=0, next=free_head (the singly-linked free list).
        let mut hc = [0u8; FixedDblLayout::HEADER_CONTENT_SIZE];
        hc[8..16].copy_from_slice(&header.free_head.to_le_bytes());
        Block::<FixedDblLayout>::new(block.0).write(&self.stack, &hc, PAYLOAD_CAPACITY, &[])?;
        header.free_head = block.0;
        self.write_header_locked(header)?;
        Ok(())
    }

    /// Write a block with both `prev` and `next` pointers set.
    fn write_block_full(
        &self,
        offset: u64,
        prev: u64,
        next: u64,
        data: &[u8],
    ) -> Result<(), Error> {
        let mut hc = [0u8; FixedDblLayout::HEADER_CONTENT_SIZE];
        hc[0..8].copy_from_slice(&prev.to_le_bytes());
        hc[8..16].copy_from_slice(&next.to_le_bytes());
        Block::<FixedDblLayout>::new(offset).write(&self.stack, &hc, PAYLOAD_CAPACITY, data)
    }

    /// Read, CRC-verify, and return `(prev, next, payload)` for a block.
    fn read_block_full(&self, offset: u64) -> Result<(u64, u64, Vec<u8>), Error> {
        let (hc, payload) =
            Block::<FixedDblLayout>::new(offset).read(&self.stack, PAYLOAD_CAPACITY)?;
        let prev = u64::from_le_bytes(hc[0..8].try_into().unwrap());
        let next = u64::from_le_bytes(hc[8..16].try_into().unwrap());
        Ok((prev, next, payload))
    }

    /// Read the block's header content, update only the `next` field, and write
    /// back with a fresh CRC.
    fn update_next(&self, offset: u64, next: u64) -> Result<(), Error> {
        let b = Block::<FixedDblLayout>::new(offset);
        let hc = b.read_header_unchecked(&self.stack)?;
        let mut new_hc = [0u8; FixedDblLayout::HEADER_CONTENT_SIZE];
        new_hc[0..8].copy_from_slice(&hc[0..8]); // preserve prev
        new_hc[8..16].copy_from_slice(&next.to_le_bytes());
        b.update_header(&self.stack, &new_hc, PAYLOAD_CAPACITY)
    }

    /// Read the block's header content, update only the `prev` field, and write
    /// back with a fresh CRC.
    fn update_prev(&self, offset: u64, prev: u64) -> Result<(), Error> {
        let b = Block::<FixedDblLayout>::new(offset);
        let hc = b.read_header_unchecked(&self.stack)?;
        let mut new_hc = [0u8; FixedDblLayout::HEADER_CONTENT_SIZE];
        new_hc[0..8].copy_from_slice(&prev.to_le_bytes());
        new_hc[8..16].copy_from_slice(&hc[8..16]); // preserve next
        b.update_header(&self.stack, &new_hc, PAYLOAD_CAPACITY)
    }

    /// Walk the active list from `root`, verify every block's checksum, and
    /// return the offset of the tail block (last block whose next == 0), or
    /// `0` if the list is empty.  Also collects all active offsets.
    fn walk_active(
        stack: &BStack,
        root: u64,
        max_steps: usize,
    ) -> Result<(HashSet<u64>, u64), Error> {
        let mut active: HashSet<u64> = HashSet::new();
        let mut actual_tail = 0u64;
        let mut cur = root;
        let mut steps = 0usize;
        while cur != 0 {
            if steps >= max_steps {
                return Err(Error::Corruption("cycle detected in active list".into()));
            }
            let (hc, _) = Block::<FixedDblLayout>::new(cur).read(stack, PAYLOAD_CAPACITY)?;
            active.insert(cur);
            let next = u64::from_le_bytes(hc[8..16].try_into().unwrap());
            if next == 0 {
                actual_tail = cur;
            }
            cur = next;
            steps += 1;
        }
        Ok((active, actual_tail))
    }

    fn recover_orphans(stack: &BStack, header: &mut Header, total: u64) -> Result<(), Error> {
        if total <= HEADER_SIZE {
            return Ok(());
        }

        let block_size = PAYLOAD_CAPACITY + BLOCK_HEADER_SIZE;
        let num_blocks = ((total - HEADER_SIZE) / block_size as u64) as usize;
        let max_steps = num_blocks + 1;

        let (active, actual_tail) = Self::walk_active(stack, header.root, max_steps)?;

        // Fix tail if the file is corrupt or if this is first open after a crash.
        let tail_changed = if header.root == 0 {
            let changed = header.tail != 0;
            header.tail = 0;
            changed
        } else {
            let changed = header.tail != actual_tail;
            header.tail = actual_tail;
            changed
        };

        // Walk the free list (unchecked; bad-CRC blocks become orphans).
        let mut free_set: HashSet<u64> = HashSet::new();
        let mut cur = header.free_head;
        let mut steps = 0usize;
        while cur != 0 {
            if steps >= max_steps {
                return Err(Error::Corruption("cycle detected in free list".into()));
            }
            free_set.insert(cur);
            let mut next_buf = [0u8; 8];
            // next is at block offset 12 (checksum 4 + prev 8).
            stack.get_into(cur + 12, &mut next_buf)?;
            cur = u64::from_le_bytes(next_buf);
            steps += 1;
        }

        // Enumerate all slots; reclaim orphans into free list.
        let mut found_orphan = false;
        for i in 0..num_blocks as u64 {
            let offset = HEADER_SIZE + i * block_size as u64;
            if active.contains(&offset) || free_set.contains(&offset) {
                continue;
            }
            let mut hc = [0u8; FixedDblLayout::HEADER_CONTENT_SIZE];
            hc[8..16].copy_from_slice(&header.free_head.to_le_bytes()); // next = free_head
            Block::<FixedDblLayout>::new(offset).write(stack, &hc, PAYLOAD_CAPACITY, &[])?;
            header.free_head = offset;
            found_orphan = true;
        }

        if found_orphan || tail_changed {
            stack.set(0, &header.to_bytes())?;
        }

        Ok(())
    }
}

// ── FixedDblIter ──────────────────────────────────────────────────────────────

/// A double-ended iterator over the blocks of a [`FixedDblList`].
///
/// Each call to [`next`](Iterator::next) reads one block from the head side
/// (with checksum verification) and advances forward; each call to
/// [`next_back`](DoubleEndedIterator::next_back) reads one block from the tail
/// side and advances backward.
///
/// Both cursors advance toward each other.  When they converge on the same
/// block, that block is yielded exactly once and iteration ends.
///
/// The iterator holds a `&` reference to the list, preventing mutation during
/// traversal.  Obtain one by calling [`FixedDblList::iter`].
pub struct FixedDblIter<'a, const PAYLOAD_CAPACITY: usize> {
    list: &'a FixedDblList<PAYLOAD_CAPACITY>,
    front: Option<BlockDblRef>,
    back: Option<BlockDblRef>,
}

impl<'a, const PAYLOAD_CAPACITY: usize> Iterator for FixedDblIter<'a, PAYLOAD_CAPACITY> {
    type Item = Result<Vec<u8>, crate::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let block = self.front?;
        if self.back == Some(block) {
            // Last element shared by both cursors — yield once, then done.
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

impl<'a, const PAYLOAD_CAPACITY: usize> DoubleEndedIterator for FixedDblIter<'a, PAYLOAD_CAPACITY> {
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

impl<const PAYLOAD_CAPACITY: usize> FixedDblList<PAYLOAD_CAPACITY> {
    /// Return a double-ended iterator over every block in the active list.
    ///
    /// Each item is `Result<Vec<u8>, Error>` where the `Vec` is always
    /// `PAYLOAD_CAPACITY` bytes long (zero-padded beyond the last write).
    /// The iterator stops after the first error on either end.
    ///
    /// Because [`FixedDblIter`] implements [`DoubleEndedIterator`], you can
    /// use `.rev()` or consume from both ends with alternating
    /// [`next`](Iterator::next) / [`next_back`](DoubleEndedIterator::next_back)
    /// calls.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use bllist::FixedDblList;
    ///
    /// let list = FixedDblList::<52>::open("data.bldf")?;
    /// list.push_back(b"alpha")?;
    /// list.push_back(b"beta")?;
    /// list.push_back(b"gamma")?;
    ///
    /// // Forward:
    /// for item in list.iter()? {
    ///     println!("{}", String::from_utf8_lossy(&item?));
    /// }
    ///
    /// // Backward:
    /// for item in list.iter()?.rev() {
    ///     println!("{}", String::from_utf8_lossy(&item?));
    /// }
    /// # Ok::<(), bllist::Error>(())
    /// ```
    pub fn iter(&self) -> Result<FixedDblIter<'_, PAYLOAD_CAPACITY>, crate::Error> {
        let _g = self.mu.lock().unwrap();
        let header = self.read_header_locked()?;
        let front = if header.root == 0 {
            None
        } else {
            Some(BlockDblRef(header.root))
        };
        let back = if header.tail == 0 {
            None
        } else {
            Some(BlockDblRef(header.tail))
        };
        Ok(FixedDblIter {
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

    type List = FixedDblList<52>;
    const CAP: usize = 52;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn tmp(label: &str) -> std::path::PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "bllist_dbl_{}_{}_{}.bldf",
            std::process::id(),
            label,
            n
        ));
        p
    }

    // ── open / fresh file ─────────────────────────────────────────────────────

    #[test]
    fn fresh_open_empty() {
        let path = tmp("fresh");
        let list = List::open(&path).unwrap();
        assert_eq!(list.root().unwrap(), None);
        assert_eq!(list.tail().unwrap(), None);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn payload_capacity_const() {
        assert_eq!(List::payload_capacity(), CAP);
        assert_eq!(FixedDblList::<116>::payload_capacity(), 116);
    }

    // ── alloc / free / free-list reuse ────────────────────────────────────────

    #[test]
    fn alloc_free_reuse() {
        let path = tmp("alloc");
        let list = List::open(&path).unwrap();

        let b0 = list.alloc().unwrap();
        let b1 = list.alloc().unwrap();
        let b2 = list.alloc().unwrap();

        list.free(b1).unwrap();
        let b3 = list.alloc().unwrap();
        assert_eq!(b3, b1);

        let _ = (b0, b2);
        let _ = std::fs::remove_file(&path);
    }

    // ── write / read round-trip ───────────────────────────────────────────────

    #[test]
    fn write_read_roundtrip() {
        let path = tmp("rw");
        let list = List::open(&path).unwrap();
        let block = list.alloc().unwrap();

        list.write(block, b"hello doubly!").unwrap();
        let out = list.read(block).unwrap();
        assert_eq!(&out[..13], b"hello doubly!");
        assert!(out[13..].iter().all(|&b| b == 0));

        let _ = std::fs::remove_file(&path);
    }

    // ── set_next / get_next / set_prev / get_prev ─────────────────────────────

    #[test]
    fn set_get_pointers() {
        let path = tmp("ptrs");
        let list = List::open(&path).unwrap();
        let b0 = list.alloc().unwrap();
        let b1 = list.alloc().unwrap();

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

    #[test]
    fn set_next_preserves_payload_and_prev() {
        let path = tmp("ptr_preserve");
        let list = List::open(&path).unwrap();
        let b0 = list.alloc().unwrap();
        let b1 = list.alloc().unwrap();

        list.write(b0, b"preserved").unwrap();
        list.set_prev(b0, Some(b1)).unwrap();
        list.set_next(b0, Some(b1)).unwrap();

        let out = list.read(b0).unwrap();
        assert_eq!(&out[..9], b"preserved");
        assert_eq!(list.get_prev(b0).unwrap(), Some(b1));
        assert_eq!(list.get_next(b0).unwrap(), Some(b1));

        let _ = std::fs::remove_file(&path);
    }

    // ── push_front / pop_front (LIFO) ─────────────────────────────────────────

    #[test]
    fn push_front_pop_front_lifo() {
        let path = tmp("lifo");
        let list = List::open(&path).unwrap();

        list.push_front(b"first").unwrap();
        list.push_front(b"second").unwrap();
        list.push_front(b"third").unwrap();

        let d1 = list.pop_front().unwrap().unwrap();
        assert_eq!(&d1[..5], b"third");
        let d2 = list.pop_front().unwrap().unwrap();
        assert_eq!(&d2[..6], b"second");
        let d3 = list.pop_front().unwrap().unwrap();
        assert_eq!(&d3[..5], b"first");
        assert_eq!(list.pop_front().unwrap(), None);
        assert_eq!(list.root().unwrap(), None);
        assert_eq!(list.tail().unwrap(), None);

        let _ = std::fs::remove_file(&path);
    }

    // ── push_back / pop_back (FIFO from back) ─────────────────────────────────

    #[test]
    fn push_back_pop_back_lifo() {
        let path = tmp("back_lifo");
        let list = List::open(&path).unwrap();

        list.push_back(b"first").unwrap();
        list.push_back(b"second").unwrap();
        list.push_back(b"third").unwrap();

        let d1 = list.pop_back().unwrap().unwrap();
        assert_eq!(&d1[..5], b"third");
        let d2 = list.pop_back().unwrap().unwrap();
        assert_eq!(&d2[..6], b"second");
        let d3 = list.pop_back().unwrap().unwrap();
        assert_eq!(&d3[..5], b"first");
        assert_eq!(list.pop_back().unwrap(), None);
        assert_eq!(list.root().unwrap(), None);
        assert_eq!(list.tail().unwrap(), None);

        let _ = std::fs::remove_file(&path);
    }

    // ── push_back / pop_front (FIFO queue) ───────────────────────────────────

    #[test]
    fn push_back_pop_front_fifo() {
        let path = tmp("fifo");
        let list = List::open(&path).unwrap();

        list.push_back(b"first").unwrap();
        list.push_back(b"second").unwrap();
        list.push_back(b"third").unwrap();

        let d1 = list.pop_front().unwrap().unwrap();
        assert_eq!(&d1[..5], b"first");
        let d2 = list.pop_front().unwrap().unwrap();
        assert_eq!(&d2[..6], b"second");
        let d3 = list.pop_front().unwrap().unwrap();
        assert_eq!(&d3[..5], b"third");
        assert_eq!(list.pop_front().unwrap(), None);

        let _ = std::fs::remove_file(&path);
    }

    // ── root/tail consistency ─────────────────────────────────────────────────

    #[test]
    fn root_tail_one_element() {
        let path = tmp("one_elem");
        let list = List::open(&path).unwrap();
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
        let list = List::open(&path).unwrap();
        list.push_front(b"hello").unwrap();

        let mut buf = vec![0u8; 5];
        assert!(list.pop_front_into(&mut buf).unwrap());
        assert_eq!(&buf, b"hello");
        assert!(!list.pop_front_into(&mut buf).unwrap());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn pop_back_into_basic() {
        let path = tmp("pop_back_into");
        let list = List::open(&path).unwrap();
        list.push_back(b"world").unwrap();

        let mut buf = vec![0u8; 5];
        assert!(list.pop_back_into(&mut buf).unwrap());
        assert_eq!(&buf, b"world");
        assert!(!list.pop_back_into(&mut buf).unwrap());

        let _ = std::fs::remove_file(&path);
    }

    // ── double-ended iterator ─────────────────────────────────────────────────

    #[test]
    fn iter_forward() {
        let path = tmp("iter_fwd");
        let list = List::open(&path).unwrap();
        list.push_back(b"a").unwrap();
        list.push_back(b"b").unwrap();
        list.push_back(b"c").unwrap();

        let items: Vec<_> = list.iter().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(items[0][0], b'a');
        assert_eq!(items[1][0], b'b');
        assert_eq!(items[2][0], b'c');

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn iter_backward() {
        let path = tmp("iter_bwd");
        let list = List::open(&path).unwrap();
        list.push_back(b"a").unwrap();
        list.push_back(b"b").unwrap();
        list.push_back(b"c").unwrap();

        let items: Vec<_> = list.iter().unwrap().rev().map(|r| r.unwrap()).collect();
        assert_eq!(items[0][0], b'c');
        assert_eq!(items[1][0], b'b');
        assert_eq!(items[2][0], b'a');

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn iter_mixed_ends_four_elements() {
        let path = tmp("iter_mixed");
        let list = List::open(&path).unwrap();
        list.push_back(b"A").unwrap();
        list.push_back(b"B").unwrap();
        list.push_back(b"C").unwrap();
        list.push_back(b"D").unwrap();

        let mut it = list.iter().unwrap();
        assert_eq!(it.next().unwrap().unwrap()[0], b'A');
        assert_eq!(it.next_back().unwrap().unwrap()[0], b'D');
        assert_eq!(it.next().unwrap().unwrap()[0], b'B');
        assert_eq!(it.next_back().unwrap().unwrap()[0], b'C');
        assert!(it.next().is_none());
        assert!(it.next_back().is_none());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn iter_mixed_ends_odd_elements() {
        let path = tmp("iter_odd");
        let list = List::open(&path).unwrap();
        list.push_back(b"A").unwrap();
        list.push_back(b"B").unwrap();
        list.push_back(b"C").unwrap();

        let mut it = list.iter().unwrap();
        assert_eq!(it.next().unwrap().unwrap()[0], b'A');
        assert_eq!(it.next_back().unwrap().unwrap()[0], b'C');
        // B is the last remaining element — yield from front side.
        assert_eq!(it.next().unwrap().unwrap()[0], b'B');
        assert!(it.next().is_none());
        assert!(it.next_back().is_none());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn iter_empty_list() {
        let path = tmp("iter_empty");
        let list = List::open(&path).unwrap();
        let mut it = list.iter().unwrap();
        assert!(it.next().is_none());
        assert!(it.next_back().is_none());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn iter_single_element() {
        let path = tmp("iter_single");
        let list = List::open(&path).unwrap();
        list.push_back(b"only").unwrap();

        let mut it = list.iter().unwrap();
        assert_eq!(&it.next().unwrap().unwrap()[..4], b"only");
        assert!(it.next().is_none());
        assert!(it.next_back().is_none());

        let _ = std::fs::remove_file(&path);
    }

    // ── checksum mismatch detection ───────────────────────────────────────────

    #[test]
    fn checksum_mismatch_on_corrupt_block() {
        let path = tmp("crc");
        {
            let list = List::open(&path).unwrap();
            list.push_front(b"integrity").unwrap();
        }

        {
            let stack = BStack::open(&path).unwrap();
            let block_offset: u64 = HEADER_SIZE;
            // Corrupt the first payload byte.
            let mut byte = [0u8; 1];
            stack
                .get_into(block_offset + BLOCK_HEADER_SIZE as u64, &mut byte)
                .unwrap();
            byte[0] ^= 0xFF;
            stack
                .set(block_offset + BLOCK_HEADER_SIZE as u64, &byte)
                .unwrap();
        }

        let err = List::open(&path).unwrap_err();
        assert!(matches!(err, Error::ChecksumMismatch { .. }));

        let _ = std::fs::remove_file(&path);
    }

    // ── orphan recovery ───────────────────────────────────────────────────────

    #[test]
    fn orphan_recovery() {
        let path = tmp("orphan");
        {
            let stack = BStack::open(&path).unwrap();
            let hdr = Header {
                root: 0,
                tail: 0,
                free_head: 0,
            };
            let off = stack.push(&hdr.to_bytes()).unwrap();
            assert_eq!(off, 0);

            // Push a raw block (PAYLOAD_CAPACITY=52 + 20 = 72 bytes).
            let block_size = CAP + BLOCK_HEADER_SIZE;
            let mut block = vec![0u8; block_size];
            let crc = crc32fast::hash(&block[4..]);
            block[0..4].copy_from_slice(&crc.to_le_bytes());
            stack.push(&block).unwrap();
        }
        let list = List::open(&path).unwrap();
        assert_eq!(list.root().unwrap(), None);
        // Orphan recovered; alloc returns offset HEADER_SIZE.
        let b = list.alloc().unwrap();
        assert_eq!(b.0, HEADER_SIZE);

        let _ = std::fs::remove_file(&path);
    }

    // ── persistence across reopen ─────────────────────────────────────────────

    #[test]
    fn reopen_persists_data() {
        let path = tmp("reopen");
        {
            let list = List::open(&path).unwrap();
            list.push_back(b"persisted").unwrap();
        }
        {
            let list = List::open(&path).unwrap();
            let data = list.pop_front().unwrap().unwrap();
            assert_eq!(&data[..9], b"persisted");
            assert_eq!(list.pop_front().unwrap(), None);
        }
        let _ = std::fs::remove_file(&path);
    }

    // ── error paths ───────────────────────────────────────────────────────────

    #[test]
    fn data_too_large() {
        let path = tmp("toolarge");
        let list = List::open(&path).unwrap();
        let block = list.alloc().unwrap();
        let err = list.write(block, &vec![0u8; CAP + 1]).unwrap_err();
        assert!(matches!(err, Error::DataTooLarge { .. }));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn invalid_block_offset() {
        let path = tmp("invalid");
        let list = List::open(&path).unwrap();
        let err = list.read(BlockDblRef(1)).unwrap_err();
        assert!(matches!(err, Error::InvalidBlock));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn invalid_block_header_offset() {
        let path = tmp("invalid_hdr");
        let list = List::open(&path).unwrap();
        let err = list.read(BlockDblRef(0)).unwrap_err();
        assert!(matches!(err, Error::InvalidBlock));
        let _ = std::fs::remove_file(&path);
    }
}
