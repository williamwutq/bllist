use std::collections::HashSet;
use std::fmt;
use std::path::Path;
use std::sync::Mutex;

use bstack::BStack;
use crc32fast::Hasher as CrcHasher;

use crate::Error;

// ── on-disk constants ────────────────────────────────────────────────────────

/// Magic bytes for the bllist persistent header.
const MAGIC: [u8; 4] = *b"BLLS";
/// On-disk format version stored in the bllist header.
const VERSION: u32 = 1;
/// Size of the bllist header at logical offset 0 within the BStack payload.
const HEADER_SIZE: u64 = 24;
/// Byte size of the per-block header: 4-byte checksum + 8-byte next pointer.
const BLOCK_HEADER_SIZE: usize = 12;

// ── BlockRef ─────────────────────────────────────────────────────────────────

/// A handle to a block in a [`FixedBlockList`], encoded as the block's logical
/// byte offset within the underlying BStack file.
///
/// `BlockRef` is `Copy` and cheap to store; treat it like a typed index. An
/// offset of `0` is never a valid block (logical offset 0 is the bllist file
/// header) and is used internally to represent *null* / end-of-list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlockRef(pub u64);

impl fmt::Display for BlockRef {
    /// Formats the block reference as `@offset` (decimal).
    ///
    /// Use `{:x}` / `{:#x}` for hexadecimal output via [`LowerHex`](fmt::LowerHex).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "@{}", self.0)
    }
}

impl fmt::LowerHex for BlockRef {
    /// Formats the block offset in lower-case hexadecimal.
    ///
    /// Respects the `#` flag: `{:#x}` produces `@0x110`, `{:x}` produces `@110`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("@")?;
        fmt::LowerHex::fmt(&self.0, f)
    }
}

impl fmt::UpperHex for BlockRef {
    /// Formats the block offset in upper-case hexadecimal.
    ///
    /// Respects the `#` flag: `{:#X}` produces `@0x110`, `{:X}` produces `@110`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("@")?;
        fmt::UpperHex::fmt(&self.0, f)
    }
}

impl From<u64> for BlockRef {
    /// Create a `BlockRef` from a raw logical byte offset.
    ///
    /// No validation is performed; the offset is not checked against the file.
    /// Use [`FixedBlockList::alloc`] to obtain a valid reference.
    fn from(offset: u64) -> Self {
        BlockRef(offset)
    }
}

impl From<BlockRef> for u64 {
    /// Extract the raw logical byte offset from a `BlockRef`.
    fn from(r: BlockRef) -> u64 {
        r.0
    }
}

// ── Header (in-memory mirror of the 24-byte on-disk bllist header) ───────────

struct Header {
    root: u64,
    free_list_head: u64,
}

impl Header {
    fn from_bytes(buf: &[u8; 24]) -> Result<Self, Error> {
        if buf[0..4] != MAGIC {
            return Err(Error::Corruption(format!(
                "invalid magic bytes: expected {:?}, found {:?}",
                MAGIC,
                &buf[0..4]
            )));
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if version != VERSION {
            return Err(Error::Corruption(format!(
                "unsupported bllist version {version}, expected {VERSION}"
            )));
        }
        Ok(Self {
            root: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
            free_list_head: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
        })
    }

    fn to_bytes(&self) -> [u8; 24] {
        let mut buf = [0u8; 24];
        buf[0..4].copy_from_slice(&MAGIC);
        buf[4..8].copy_from_slice(&VERSION.to_le_bytes());
        buf[8..16].copy_from_slice(&self.root.to_le_bytes());
        buf[16..24].copy_from_slice(&self.free_list_head.to_le_bytes());
        buf
    }
}

// ── FixedBlockList ────────────────────────────────────────────────────────────

/// A crash-safe, checksummed singly-linked list of fixed-size blocks stored in
/// a single [BStack] file.
///
/// The const generic `PAYLOAD_CAPACITY` is the number of **payload bytes** per
/// block. Each block occupies `PAYLOAD_CAPACITY + 12` bytes on disk:
///
/// | Offset | Size | Field | Description |
/// |--------|------|-------|-------------|
/// | 0 | 4 | `checksum` | CRC32 of bytes `[4..PAYLOAD_CAPACITY+12]` (next + payload) |
/// | 4 | 8 | `next` | Logical offset of the next block; `0` = null |
/// | 12 | `PAYLOAD_CAPACITY` | `payload` | User data, zero-padded to capacity |
///
/// The underlying file is a valid BStack file. After the BStack header (16
/// physical bytes, transparent to callers) comes a 24-byte bllist header
/// (`"BLLS"` magic, version, root offset, free-list-head offset), followed by
/// contiguously-packed blocks.
///
/// # Crash safety
///
/// All mutations (`write`, `set_next`, `alloc`, `free`, `push_front`,
/// `pop_front`) flush durably to disk before returning. On the next [`open`]
/// the file is scanned for *orphaned* blocks (allocated but unreachable from
/// either the active list or the free list) and they are silently reclaimed.
///
/// # Thread safety
///
/// `FixedBlockList` is `Send + Sync`. Concurrent reads are efficient (BStack
/// uses `pread` on Unix/Windows). Header-mutating operations are serialised
/// through an internal [`Mutex`].
///
/// # Example
///
/// ```no_run
/// use bllist::FixedBlockList;
///
/// // 52 bytes of payload per block (64 bytes total on disk).
/// let list = FixedBlockList::<52>::open("data.blls")?;
///
/// list.push_front(b"hello")?;
/// list.push_front(b"world")?;
///
/// while let Some(data) = list.pop_front()? {
///     println!("{}", String::from_utf8_lossy(&data));
/// }
/// // prints "world", then "hello"
/// # Ok::<(), bllist::Error>(())
/// ```
///
/// [`open`]: FixedBlockList::open
pub struct FixedBlockList<const PAYLOAD_CAPACITY: usize> {
    stack: BStack,
    /// Serialises all header read-modify-write sequences.
    mu: Mutex<()>,
}

impl<const PAYLOAD_CAPACITY: usize> fmt::Debug for FixedBlockList<PAYLOAD_CAPACITY> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FixedBlockList")
            .field("payload_capacity", &PAYLOAD_CAPACITY)
            .field("block_size", &(PAYLOAD_CAPACITY + BLOCK_HEADER_SIZE))
            .finish_non_exhaustive()
    }
}

impl<const PAYLOAD_CAPACITY: usize> fmt::Display for FixedBlockList<PAYLOAD_CAPACITY> {
    /// Formats as `FixedBlockList<N>` where `N` is the payload capacity in bytes.
    ///
    /// Useful for logging which list configuration is in use.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FixedBlockList<{PAYLOAD_CAPACITY}>")
    }
}

impl<const PAYLOAD_CAPACITY: usize> FixedBlockList<PAYLOAD_CAPACITY> {
    // ── public API ────────────────────────────────────────────────────────────

    /// Open or create the file at `path` as a [`FixedBlockList`].
    ///
    /// * **New file** – creates the file, writes the bllist header, and returns
    ///   an empty list.
    /// * **Existing file** – validates the bllist header (magic bytes,
    ///   version), then performs lightweight crash recovery:
    ///   1. Walk the active list from root, verifying every block's checksum.
    ///   2. Walk the free list, verifying every block's checksum.
    ///   3. Enumerate all block slots in the file; any slot reachable from
    ///      neither list is an *orphan* and is linked into the free list.
    ///
    /// The underlying BStack acquires an exclusive advisory file lock, so a
    /// second `open` from the same or another process returns
    /// [`Error::Io`] with kind `WouldBlock`.
    ///
    /// # Errors
    ///
    /// | Error | Cause |
    /// |-------|-------|
    /// | [`Error::Io`] | File could not be opened, created, or locked |
    /// | [`Error::Corruption`] | Wrong magic bytes or unsupported version |
    /// | [`Error::ChecksumMismatch`] | A block in the active or free list has a bad CRC |
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        // Compile-time check: PAYLOAD_CAPACITY must be at least 1.
        const {
            assert!(
                PAYLOAD_CAPACITY > 0,
                "PAYLOAD_CAPACITY must be greater than 0 \
                 (block header already occupies 12 bytes)"
            )
        };

        let stack = BStack::open(path)?;
        let total = stack.len()?;

        if total == 0 {
            let h = Header {
                root: 0,
                free_list_head: 0,
            };
            let offset = stack.push(&h.to_bytes())?;
            debug_assert_eq!(offset, 0, "bllist header must land at logical offset 0");
            return Ok(Self {
                stack,
                mu: Mutex::new(()),
            });
        }

        if total < HEADER_SIZE {
            return Err(Error::Corruption(format!(
                "file payload is {total} bytes, too small for the 24-byte bllist header"
            )));
        }

        let mut hdr_buf = [0u8; 24];
        stack.get_into(0, &mut hdr_buf)?;
        let mut header = Header::from_bytes(&hdr_buf)?;

        Self::recover_orphans(&stack, &mut header, total)?;

        Ok(Self {
            stack,
            mu: Mutex::new(()),
        })
    }

    /// Returns the number of payload bytes available in each block.
    ///
    /// Equal to the `PAYLOAD_CAPACITY` const generic parameter. Each block
    /// occupies `PAYLOAD_CAPACITY + 12` bytes on disk (the extra 12 bytes hold
    /// the 4-byte CRC32 checksum and the 8-byte next pointer).
    ///
    /// `PAYLOAD_CAPACITY` must be `> 0`; a value of `0` is rejected at
    /// compile time.
    pub const fn payload_capacity() -> usize {
        PAYLOAD_CAPACITY
    }

    /// Allocate a new block from the free list, or grow the file if empty.
    ///
    /// The returned [`BlockRef`] points to an initialised block whose payload
    /// is zeroed and whose next pointer is null. Call [`write`] to populate it
    /// and [`set_next`] (or [`push_front`]) to link it into a list.
    ///
    /// [`write`]: Self::write
    /// [`set_next`]: Self::set_next
    /// [`push_front`]: Self::push_front
    pub fn alloc(&self) -> Result<BlockRef, Error> {
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        self.alloc_locked(&mut header)
    }

    /// Return `block` to the free list.
    ///
    /// The block's payload is zeroed on disk before being linked into the free
    /// list. After this call the [`BlockRef`] is invalid; reading from it will
    /// likely return [`Error::ChecksumMismatch`] or stale data.
    pub fn free(&self, block: BlockRef) -> Result<(), Error> {
        self.validate_block_offset(block.0)?;
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        self.free_locked(block, &mut header)
    }

    /// Write `data` into the payload of `block`.
    ///
    /// The existing next pointer is preserved. Bytes of the payload beyond
    /// `data.len()` are zeroed. The checksum is recomputed and the entire
    /// block is written atomically via a single `bstack::set` call.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DataTooLarge`] if `data.len() > PAYLOAD_CAPACITY`.
    pub fn write(&self, block: BlockRef, data: &[u8]) -> Result<(), Error> {
        if data.len() > PAYLOAD_CAPACITY {
            return Err(Error::DataTooLarge {
                capacity: PAYLOAD_CAPACITY,
                provided: data.len(),
            });
        }
        self.validate_block_offset(block.0)?;

        let block_size = PAYLOAD_CAPACITY + BLOCK_HEADER_SIZE;
        let mut buf = vec![0u8; block_size];
        // Preserve the current next pointer (8 bytes at block.0 + 4).
        self.stack.get_into(block.0 + 4, &mut buf[4..12])?;
        buf[12..12 + data.len()].copy_from_slice(data);
        // buf[12 + data.len()..] stays zero — padding.
        let crc = crc32fast::hash(&buf[4..]);
        buf[0..4].copy_from_slice(&crc.to_le_bytes());
        self.stack.set(block.0, &buf)?;
        Ok(())
    }

    /// Read the payload of `block` into a freshly allocated [`Vec<u8>`].
    ///
    /// The vector is always `PAYLOAD_CAPACITY` bytes long. Bytes beyond the
    /// last [`write`] will be zero. The checksum is verified before returning.
    ///
    /// # Errors
    ///
    /// Returns [`Error::ChecksumMismatch`] if the block's CRC is wrong.
    ///
    /// [`write`]: Self::write
    pub fn read(&self, block: BlockRef) -> Result<Vec<u8>, Error> {
        let mut buf = vec![0u8; PAYLOAD_CAPACITY];
        self.read_into(block, &mut buf)?;
        Ok(buf)
    }

    /// Zero-copy variant of [`read`](Self::read).
    ///
    /// Reads the block's payload directly from the file into `buf` without an
    /// intermediate heap allocation. The checksum is verified: the CRC covers
    /// `next` (8 bytes) plus the *full* payload field (`PAYLOAD_CAPACITY`
    /// bytes, including any zero-padding beyond `buf.len()`).
    ///
    /// For the CRC check to pass, `buf.len()` must be ≥ the number of
    /// non-zero bytes last written to the block (because this method assumes
    /// the bytes beyond `buf.len()` are zero on disk, which is guaranteed by
    /// [`write`]).
    ///
    /// # Errors
    ///
    /// | Error | Cause |
    /// |-------|-------|
    /// | [`Error::DataTooLarge`] | `buf.len() > PAYLOAD_CAPACITY` |
    /// | [`Error::ChecksumMismatch`] | CRC mismatch |
    ///
    /// [`write`]: Self::write
    pub fn read_into(&self, block: BlockRef, buf: &mut [u8]) -> Result<(), Error> {
        if buf.len() > PAYLOAD_CAPACITY {
            return Err(Error::DataTooLarge {
                capacity: PAYLOAD_CAPACITY,
                provided: buf.len(),
            });
        }
        self.validate_block_offset(block.0)?;

        let mut hdr = [0u8; 12];
        self.stack.get_into(block.0, &mut hdr)?;
        // Read payload directly into the caller's buffer — zero copy.
        self.stack.get_into(block.0 + 12, buf)?;

        let stored = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
        let mut hasher = CrcHasher::new();
        hasher.update(&hdr[4..12]); // next pointer
        hasher.update(buf);
        let tail = PAYLOAD_CAPACITY - buf.len();
        if tail > 0 {
            // write() always pads with zeros, so we hash zeros for the tail.
            hasher.update(&vec![0u8; tail]);
        }
        if hasher.finalize() != stored {
            return Err(Error::ChecksumMismatch { block: block.0 });
        }
        Ok(())
    }

    /// Update the next-block pointer of `block`.
    ///
    /// The payload is preserved. The checksum is recomputed over the new next
    /// pointer and the existing payload, then the entire block is written
    /// atomically.
    pub fn set_next(&self, block: BlockRef, next: Option<BlockRef>) -> Result<(), Error> {
        self.validate_block_offset(block.0)?;
        let next_val = next.map(|r| r.0).unwrap_or(0u64);

        // Read the full block so the existing payload is included in the new CRC.
        let block_size = PAYLOAD_CAPACITY + BLOCK_HEADER_SIZE;
        let mut buf = vec![0u8; block_size];
        self.stack.get_into(block.0, &mut buf)?;
        buf[4..12].copy_from_slice(&next_val.to_le_bytes());
        let crc = crc32fast::hash(&buf[4..]);
        buf[0..4].copy_from_slice(&crc.to_le_bytes());
        self.stack.set(block.0, &buf)?;
        Ok(())
    }

    /// Return the next-block pointer of `block`.
    ///
    /// Returns `None` if the next pointer is null (i.e. `block` is the tail).
    ///
    /// This method does **not** verify the checksum; use [`read`] if
    /// integrity checking is required.
    ///
    /// [`read`]: Self::read
    pub fn get_next(&self, block: BlockRef) -> Result<Option<BlockRef>, Error> {
        self.validate_block_offset(block.0)?;
        let mut next_buf = [0u8; 8];
        self.stack.get_into(block.0 + 4, &mut next_buf)?;
        let next = u64::from_le_bytes(next_buf);
        Ok(if next == 0 {
            None
        } else {
            Some(BlockRef(next))
        })
    }

    /// Return the head of the active list, or `None` if the list is empty.
    pub fn root(&self) -> Result<Option<BlockRef>, Error> {
        let _g = self.mu.lock().unwrap();
        let header = self.read_header_locked()?;
        Ok(if header.root == 0 {
            None
        } else {
            Some(BlockRef(header.root))
        })
    }

    /// Allocate a block, write `data` to it, and prepend it to the list.
    ///
    /// The new block becomes the new head (root). The entire sequence is crash-
    /// safe: if the process is killed after the block is written but before the
    /// root pointer is updated, the new block becomes an orphan and is
    /// reclaimed on the next [`open`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::DataTooLarge`] if `data.len() > PAYLOAD_CAPACITY`.
    ///
    /// [`open`]: Self::open
    pub fn push_front(&self, data: &[u8]) -> Result<BlockRef, Error> {
        if data.len() > PAYLOAD_CAPACITY {
            return Err(Error::DataTooLarge {
                capacity: PAYLOAD_CAPACITY,
                provided: data.len(),
            });
        }
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        let old_root = header.root;

        // alloc_locked updates header.free_list_head and flushes the header.
        let new_block = self.alloc_locked(&mut header)?;
        // Write data + next pointer in a single set() call.
        self.write_block_with_next(new_block.0, old_root, data)?;
        // Link the new block as the root.
        header.root = new_block.0;
        self.write_header_locked(&header)?;

        Ok(new_block)
    }

    /// Remove and return the payload of the head block, or `None` if empty.
    ///
    /// The head is unlinked, its payload is returned, and the block is freed
    /// back to the free list. If the process is killed after unlinking the head
    /// but before freeing the block, the orphaned block is reclaimed on the
    /// next [`open`].
    ///
    /// [`open`]: Self::open
    pub fn pop_front(&self) -> Result<Option<Vec<u8>>, Error> {
        let _g = self.mu.lock().unwrap();
        let mut header = self.read_header_locked()?;
        if header.root == 0 {
            return Ok(None);
        }
        let old_root = header.root;
        let (next, payload) = self.read_block_full(old_root)?;

        // Advance root; crash here → old_root becomes an orphan, recovered on open().
        header.root = next;
        self.write_header_locked(&header)?;
        self.free_locked(BlockRef(old_root), &mut header)?;
        Ok(Some(payload))
    }

    /// Zero-copy variant of [`pop_front`](Self::pop_front).
    ///
    /// Reads the head block payload directly into `buf`, unlinks the head, and
    /// frees the block. Returns `true` if a block was popped, `false` if the
    /// list was empty.
    ///
    /// `buf.len()` must be ≥ the number of non-zero bytes written to the head
    /// block for the checksum to pass (see [`read_into`] for the full
    /// semantics).
    ///
    /// # Errors
    ///
    /// Returns [`Error::DataTooLarge`] if `buf.len() > PAYLOAD_CAPACITY`.
    ///
    /// [`read_into`]: Self::read_into
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

        // read_into does not acquire mu, so this is deadlock-free.
        self.read_into(BlockRef(old_root), buf)?;

        // Read the next pointer separately (read_into does not expose it).
        let mut next_buf = [0u8; 8];
        self.stack.get_into(old_root + 4, &mut next_buf)?;
        let next = u64::from_le_bytes(next_buf);

        header.root = next;
        self.write_header_locked(&header)?;
        self.free_locked(BlockRef(old_root), &mut header)?;
        Ok(true)
    }

    // ── private helpers ───────────────────────────────────────────────────────

    /// Assert that `offset` is a valid block boundary.
    fn validate_block_offset(&self, offset: u64) -> Result<(), Error> {
        let block_size = (PAYLOAD_CAPACITY + BLOCK_HEADER_SIZE) as u64;
        if offset < HEADER_SIZE || !(offset - HEADER_SIZE).is_multiple_of(block_size) {
            return Err(Error::InvalidBlock);
        }
        Ok(())
    }

    /// Read the 24-byte bllist header. Caller must hold `mu`.
    fn read_header_locked(&self) -> Result<Header, Error> {
        let mut buf = [0u8; 24];
        self.stack.get_into(0, &mut buf)?;
        Header::from_bytes(&buf)
    }

    /// Write the 24-byte bllist header. Caller must hold `mu`.
    fn write_header_locked(&self, header: &Header) -> Result<(), Error> {
        self.stack.set(0, &header.to_bytes())?;
        Ok(())
    }

    /// Allocate one block. If the free list is non-empty, pops from it;
    /// otherwise grows the file. Updates `header.free_list_head` and flushes
    /// the header to disk. Caller must hold `mu`.
    fn alloc_locked(&self, header: &mut Header) -> Result<BlockRef, Error> {
        if header.free_list_head != 0 {
            let fh = header.free_list_head;
            let mut next_buf = [0u8; 8];
            self.stack.get_into(fh + 4, &mut next_buf)?;
            header.free_list_head = u64::from_le_bytes(next_buf);
            self.write_header_locked(header)?;
            Ok(BlockRef(fh))
        } else {
            let block_size = PAYLOAD_CAPACITY + BLOCK_HEADER_SIZE;
            let zeros = vec![0u8; block_size];
            let offset = self.stack.push(&zeros)?;
            debug_assert!(
                offset >= HEADER_SIZE && (offset - HEADER_SIZE).is_multiple_of(block_size as u64),
                "newly pushed block has misaligned offset {offset}"
            );
            Ok(BlockRef(offset))
        }
    }

    /// Free `block` by zeroing it, linking it into the free list, and
    /// updating `header.free_list_head`. Caller must hold `mu`.
    fn free_locked(&self, block: BlockRef, header: &mut Header) -> Result<(), Error> {
        let block_size = PAYLOAD_CAPACITY + BLOCK_HEADER_SIZE;
        let mut buf = vec![0u8; block_size];
        buf[4..12].copy_from_slice(&header.free_list_head.to_le_bytes());
        // buf[12..] stays zero — payload cleared on free.
        let crc = crc32fast::hash(&buf[4..]);
        buf[0..4].copy_from_slice(&crc.to_le_bytes());
        self.stack.set(block.0, &buf)?;
        header.free_list_head = block.0;
        self.write_header_locked(header)?;
        Ok(())
    }

    /// Build a block buffer with `next` and `data`, compute its CRC, and
    /// write it atomically with a single `set`. Does NOT acquire `mu`.
    fn write_block_with_next(&self, offset: u64, next: u64, data: &[u8]) -> Result<(), Error> {
        let block_size = PAYLOAD_CAPACITY + BLOCK_HEADER_SIZE;
        let mut buf = vec![0u8; block_size];
        buf[4..12].copy_from_slice(&next.to_le_bytes());
        buf[12..12 + data.len()].copy_from_slice(data);
        let crc = crc32fast::hash(&buf[4..]);
        buf[0..4].copy_from_slice(&crc.to_le_bytes());
        self.stack.set(offset, &buf)?;
        Ok(())
    }

    /// Read a full block, verify its checksum, and return `(next, payload)`.
    /// Does NOT acquire `mu`.
    fn read_block_full(&self, offset: u64) -> Result<(u64, Vec<u8>), Error> {
        let block_size = PAYLOAD_CAPACITY + BLOCK_HEADER_SIZE;
        let buf = self.stack.get(offset, offset + block_size as u64)?;
        if buf.len() != block_size {
            return Err(Error::InvalidBlock);
        }
        let stored = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let computed = crc32fast::hash(&buf[4..]);
        if computed != stored {
            return Err(Error::ChecksumMismatch { block: offset });
        }
        let next = u64::from_le_bytes(buf[4..12].try_into().unwrap());
        let payload = buf[12..].to_vec();
        Ok((next, payload))
    }

    /// Scan all block slots; add any orphaned slot to the free list.
    ///
    /// Called from `open` before the `FixedBlockList` is constructed, so it
    /// takes a `&BStack` directly instead of `&self`.
    fn recover_orphans(stack: &BStack, header: &mut Header, total: u64) -> Result<(), Error> {
        if total <= HEADER_SIZE {
            return Ok(());
        }

        let block_size = PAYLOAD_CAPACITY + BLOCK_HEADER_SIZE;
        let num_blocks = ((total - HEADER_SIZE) / block_size as u64) as usize;
        let max_steps = num_blocks + 1; // cycle-detection bound

        // Walk the active list.
        let mut active: HashSet<u64> = HashSet::new();
        let mut cur = header.root;
        let mut steps = 0usize;
        while cur != 0 {
            if steps >= max_steps {
                return Err(Error::Corruption("cycle detected in active list".into()));
            }
            let mut block_buf = vec![0u8; block_size];
            stack.get_into(cur, &mut block_buf)?;
            let stored = u32::from_le_bytes(block_buf[0..4].try_into().unwrap());
            if crc32fast::hash(&block_buf[4..]) != stored {
                return Err(Error::ChecksumMismatch { block: cur });
            }
            active.insert(cur);
            cur = u64::from_le_bytes(block_buf[4..12].try_into().unwrap());
            steps += 1;
        }

        // Walk the free list.
        let mut free_set: HashSet<u64> = HashSet::new();
        cur = header.free_list_head;
        steps = 0;
        while cur != 0 {
            if steps >= max_steps {
                return Err(Error::Corruption("cycle detected in free list".into()));
            }
            let mut block_buf = vec![0u8; block_size];
            stack.get_into(cur, &mut block_buf)?;
            let stored = u32::from_le_bytes(block_buf[0..4].try_into().unwrap());
            if crc32fast::hash(&block_buf[4..]) != stored {
                return Err(Error::ChecksumMismatch { block: cur });
            }
            free_set.insert(cur);
            cur = u64::from_le_bytes(block_buf[4..12].try_into().unwrap());
            steps += 1;
        }

        // Enumerate all slots; reclaim orphans.
        let mut found_orphan = false;
        for i in 0..num_blocks as u64 {
            let offset = HEADER_SIZE + i * block_size as u64;
            if active.contains(&offset) || free_set.contains(&offset) {
                continue;
            }
            // Orphaned block — zero it and link into free list.
            let mut buf = vec![0u8; block_size];
            buf[4..12].copy_from_slice(&header.free_list_head.to_le_bytes());
            let crc = crc32fast::hash(&buf[4..]);
            buf[0..4].copy_from_slice(&crc.to_le_bytes());
            stack.set(offset, &buf)?;
            header.free_list_head = offset;
            found_orphan = true;
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

    // 52 bytes of payload → 64 bytes per block on disk.
    type List = FixedBlockList<52>;
    const CAP: usize = 52;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn tmp(label: &str) -> std::path::PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "bllist_{}_{}_{}.blls",
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
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn payload_capacity_const() {
        assert_eq!(List::payload_capacity(), CAP);
        assert_eq!(FixedBlockList::<116>::payload_capacity(), 116);
    }

    // ── alloc / free / free-list reuse ────────────────────────────────────────

    #[test]
    fn alloc_free_reuse() {
        let path = tmp("alloc");
        let list = List::open(&path).unwrap();

        let b0 = list.alloc().unwrap();
        let b1 = list.alloc().unwrap();
        let b2 = list.alloc().unwrap();

        // Free the middle block.
        list.free(b1).unwrap();

        // Next alloc should reuse b1.
        let b3 = list.alloc().unwrap();
        assert_eq!(b3, b1);

        drop(list);
        let _ = std::fs::remove_file(&path);
        let _ = (b0, b2, b3);
    }

    // ── write / read round-trip ───────────────────────────────────────────────

    #[test]
    fn write_read_roundtrip() {
        let path = tmp("rw");
        let list = List::open(&path).unwrap();
        let block = list.alloc().unwrap();

        let data = b"hello, bllist!";
        list.write(block, data).unwrap();

        let out = list.read(block).unwrap();
        assert_eq!(&out[..data.len()], data);
        assert!(out[data.len()..].iter().all(|&b| b == 0));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn overwrite_shorter_zeroes_tail() {
        let path = tmp("overwrite");
        let list = List::open(&path).unwrap();
        let block = list.alloc().unwrap();

        list.write(block, b"longer data here!!").unwrap();
        list.write(block, b"short").unwrap();

        let out = list.read(block).unwrap();
        assert_eq!(&out[..5], b"short");
        assert!(out[5..].iter().all(|&b| b == 0));

        let _ = std::fs::remove_file(&path);
    }

    // ── read_into (zero-copy) ─────────────────────────────────────────────────

    #[test]
    fn read_into_full_capacity() {
        let path = tmp("read_into");
        let list = List::open(&path).unwrap();
        let block = list.alloc().unwrap();

        let data: Vec<u8> = (0..CAP as u8).collect();
        list.write(block, &data).unwrap();

        let mut buf = vec![0u8; CAP];
        list.read_into(block, &mut buf).unwrap();
        assert_eq!(buf, data);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn read_into_shorter_than_written_fails_crc() {
        let path = tmp("read_into_crc");
        let list = List::open(&path).unwrap();
        let block = list.alloc().unwrap();

        // Write 20 non-zero bytes; reading into a 5-byte buf means bytes 5-19
        // are non-zero but we assume zero for the tail CRC → mismatch.
        list.write(block, &[0xAB; 20]).unwrap();

        let mut buf = vec![0u8; 5];
        let err = list.read_into(block, &mut buf).unwrap_err();
        assert!(matches!(err, Error::ChecksumMismatch { .. }));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn read_into_exact_written_length_passes() {
        let path = tmp("read_into_exact");
        let list = List::open(&path).unwrap();
        let block = list.alloc().unwrap();

        list.write(block, &[0xAB; 10]).unwrap();

        let mut buf = vec![0u8; 10];
        list.read_into(block, &mut buf).unwrap();
        assert_eq!(buf, vec![0xAB; 10]);

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

        // Corrupt one byte of the payload area via BStack directly.
        {
            let stack = BStack::open(&path).unwrap();
            // Block 0 is at logical offset 24; payload starts at offset 24+12=36.
            let block_offset: u64 = 24;
            let mut byte = [0u8; 1];
            stack.get_into(block_offset + 12, &mut byte).unwrap();
            byte[0] ^= 0xFF;
            stack.set(block_offset + 12, &byte).unwrap();
        }

        // Reopen: the active list walk hits the corrupt block → open() fails.
        let err = List::open(&path).unwrap_err();
        assert!(matches!(err, Error::ChecksumMismatch { .. }));

        let _ = std::fs::remove_file(&path);
    }

    // ── set_next / get_next ───────────────────────────────────────────────────

    #[test]
    fn set_get_next() {
        let path = tmp("next");
        let list = List::open(&path).unwrap();
        let b0 = list.alloc().unwrap();
        let b1 = list.alloc().unwrap();

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
        let list = List::open(&path).unwrap();
        let b0 = list.alloc().unwrap();
        let b1 = list.alloc().unwrap();

        list.write(b0, b"preserved").unwrap();
        list.set_next(b0, Some(b1)).unwrap();

        let out = list.read(b0).unwrap();
        assert_eq!(&out[..9], b"preserved");

        let _ = std::fs::remove_file(&path);
    }

    // ── push_front / pop_front (LIFO) ─────────────────────────────────────────

    #[test]
    fn push_pop_lifo() {
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

        let _ = std::fs::remove_file(&path);
    }

    // ── pop_front_into (zero-copy) ────────────────────────────────────────────

    #[test]
    fn pop_front_into_basic() {
        let path = tmp("pop_into");
        let list = List::open(&path).unwrap();
        list.push_front(b"hello").unwrap();

        let mut buf = vec![0u8; 5];
        let popped = list.pop_front_into(&mut buf).unwrap();
        assert!(popped);
        assert_eq!(&buf, b"hello");

        let popped2 = list.pop_front_into(&mut buf).unwrap();
        assert!(!popped2);

        let _ = std::fs::remove_file(&path);
    }

    // ── pop_front on empty list ───────────────────────────────────────────────

    #[test]
    fn pop_front_empty() {
        let path = tmp("pop_empty");
        let list = List::open(&path).unwrap();
        assert_eq!(list.pop_front().unwrap(), None);
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
                free_list_head: 0,
            };
            let off = stack.push(&hdr.to_bytes()).unwrap();
            assert_eq!(off, 0);
            // Push a raw 64-byte block (PAYLOAD_CAPACITY=52 + 12 = 64).
            let mut block = [0u8; 64];
            let crc = crc32fast::hash(&block[4..]);
            block[0..4].copy_from_slice(&crc.to_le_bytes());
            stack.push(&block).unwrap();
        }
        let list = List::open(&path).unwrap();
        assert_eq!(list.root().unwrap(), None);
        // Orphan was recovered; alloc returns offset 24.
        let b = list.alloc().unwrap();
        assert_eq!(b.0, 24);

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
        let err = list.read(BlockRef(1)).unwrap_err();
        assert!(matches!(err, Error::InvalidBlock));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn invalid_block_header_offset() {
        let path = tmp("invalid_hdr");
        let list = List::open(&path).unwrap();
        let err = list.read(BlockRef(0)).unwrap_err();
        assert!(matches!(err, Error::InvalidBlock));
        let _ = std::fs::remove_file(&path);
    }

    // ── persistence across reopen ─────────────────────────────────────────────

    #[test]
    fn reopen_persists_data() {
        let path = tmp("reopen");
        {
            let list = List::open(&path).unwrap();
            list.push_front(b"persisted").unwrap();
        }
        {
            let list = List::open(&path).unwrap();
            let data = list.pop_front().unwrap().unwrap();
            assert_eq!(&data[..9], b"persisted");
            assert_eq!(list.pop_front().unwrap(), None);
        }
        let _ = std::fs::remove_file(&path);
    }

    // ── write empty data ──────────────────────────────────────────────────────

    #[test]
    fn write_empty_data() {
        let path = tmp("empty_data");
        let list = List::open(&path).unwrap();
        let block = list.alloc().unwrap();
        list.write(block, &[]).unwrap();
        let out = list.read(block).unwrap();
        assert!(out.iter().all(|&b| b == 0));
        let _ = std::fs::remove_file(&path);
    }
}
