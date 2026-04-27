//! Bin-based block allocator with native CRC32 checksumming.
//!
//! This module provides a [`BinAlloc`] implementing [`bstack::BStackAllocator`]
//! that manages variable-size blocks.
//! Every block is protected by a CRC32 checksum.
//!
//! ## File layout
//!
//! ```text
//! ┌──────────────────────────┬───────────────────────────────────────────────┐
//! │  BStack header (16 B)    │  Allocator header (16 B)                      │
//! │  "BSTK" magic + clen     │  "BLA" + version                              │
//! ├──────────────────────────┴───────────────────────────────────────────────┤
//! │  Block (total size = 2^k bytes)                                          │
//! │  checksum(4) │ block_size(4) │ data_len(4) │ payload                     │
//! ├──────────────────────────────────────────────────────────────────────────┤
//! │  Block …                                                                 │
//! └──────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! **Allocator header** (16 bytes at logical offset 16):
//! - `[0..4]`      — CRC32 checksum of bytes `[4..16]`
//! - `[4..7]`      — magic `"BLA"`
//! - `[7]`         — reserved
//! - `[8..12]`     — version `u32`
//! - `[12..16]`    — reserved
//!
//! **Block on-disk layout**:
//! - `[0..4]`           — CRC32 of bytes `[4..block_size]`
//! - `[4..8]`           — `block_size` `u32` (total bytes on disk)
//! - `[8..12]`          — `data_len` `u32` (bytes written)
//! - `[12..block_size]` — payload

use bstack::{BStack, BStackAllocator, BStackSlice};

use crate::Error;

const ALLOCATOR_MAGIC: [u8; 3] = *b"BLA";
const ALLOCATOR_VERSION: u32 = 0;

const RESERVED_SIZE: u64 = 32;
const ALLOC_OFFSET: u64 = 16;
const HEADER_SIZE: u64 = 16;
const BLOCK_HEADER_SIZE: usize = 12;

const MIN_BIN: usize = 5;
const MAX_BLOCK_SIZE: usize = 1 << 31;

#[inline]
fn block_size_for(payload_size: usize) -> usize {
    // Block layout: checksum(4) + block_size(4) + data_len(4) + payload
    let total = BLOCK_HEADER_SIZE + payload_size;
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

fn write_checksum(buf: &mut [u8]) {
    let crc = crc32fast::hash(&buf[4..]);
    buf[0..4].copy_from_slice(&crc.to_le_bytes());
}

fn verify_checksum(buf: &[u8], offset: u64) -> Result<(), Error> {
    let stored = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    if crc32fast::hash(&buf[4..]) != stored {
        return Err(Error::ChecksumMismatch { block: offset });
    }
    Ok(())
}

struct AllocHeader;

impl AllocHeader {
    fn new() -> Self {
        Self
    }

    fn from_bytes(buf: &[u8; HEADER_SIZE as usize]) -> Result<Self, Error> {
        // Skip 4-byte checksum at start
        if buf[4..7] != ALLOCATOR_MAGIC {
            return Err(Error::Corruption("invalid allocator magic".into()));
        }
        let version = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        if version != ALLOCATOR_VERSION {
            return Err(Error::Corruption(format!(
                "unsupported allocator version {version}"
            )));
        }
        Ok(Self)
    }

    fn to_bytes(&self) -> [u8; HEADER_SIZE as usize] {
        let mut buf = [0u8; HEADER_SIZE as usize];
        buf[4..7].copy_from_slice(&ALLOCATOR_MAGIC);
        buf[8..12].copy_from_slice(&ALLOCATOR_VERSION.to_le_bytes());
        buf
    }
}

pub struct BinAlloc {
    stack: BStack,
}

unsafe impl Send for BinAlloc {}
unsafe impl Sync for BinAlloc {}

impl BinAlloc {
    pub fn new(stack: BStack) -> Result<Self, Error> {
        let total = stack.len()?;
        if total == 0 {
            // Push reserved region (32 bytes)
            let reserved = [0u8; RESERVED_SIZE as usize];
            stack.push(&reserved)?;

            // Push allocator header (16 bytes)
            let header = AllocHeader::new();
            let mut buf = vec![0u8; HEADER_SIZE as usize];
            buf.copy_from_slice(&header.to_bytes());
            write_checksum(&mut buf);
            stack.push(&buf)?;

            Ok(Self { stack })
        } else if total < RESERVED_SIZE + HEADER_SIZE {
            Err(Error::Corruption(format!(
                "file too small: {total} bytes, need at least {}",
                RESERVED_SIZE + HEADER_SIZE
            )))
        } else {
            let mut hdr_buf = [0u8; HEADER_SIZE as usize];
            stack.get_into(ALLOC_OFFSET, &mut hdr_buf)?;
            verify_checksum(&hdr_buf, ALLOC_OFFSET)?;
            let _header = AllocHeader::from_bytes(&hdr_buf)?;

            Ok(Self { stack })
        }
    }

    fn alloc_inner(&self, size: usize) -> Result<u64, Error> {
        let bs = block_size_for(size);
        if bs > MAX_BLOCK_SIZE {
            return Err(Error::DataTooLarge {
                capacity: MAX_BLOCK_SIZE - BLOCK_HEADER_SIZE,
                provided: size,
            });
        }

        // Push creates a new block at the end of the payload
        let mut buf = vec![0u8; bs];
        buf[4..8].copy_from_slice(&(bs as u32).to_le_bytes());
        buf[8..12].copy_from_slice(&0u32.to_le_bytes());
        write_checksum(&mut buf);
        let offset = self.stack.push(&buf)?;

        // Return the new block offset
        Ok(offset)
    }

    pub fn read_payload(&self, offset: u64) -> Result<Vec<u8>, Error> {
        let size_offset = 4;
        let mut size_buf = [0u8; 4];
        self.stack
            .get_into(offset + size_offset as u64, &mut size_buf)?;
        let _block_size = u32::from_le_bytes(size_buf) as usize;

        let mut len_buf = [0u8; 4];
        let len_offset = 8;
        self.stack
            .get_into(offset + len_offset as u64, &mut len_buf)?;
        let data_len = u32::from_le_bytes(len_buf) as usize;

        let pay_start = 12;
        let mut result = vec![0u8; data_len];
        if data_len > 0 {
            self.stack
                .get_into(offset + pay_start as u64, &mut result)?;
        }
        Ok(result)
    }

    pub fn read_payload_into(&self, offset: u64, buf: &mut [u8]) -> Result<usize, Error> {
        let size_offset = 4;
        let mut size_buf = [0u8; 4];
        self.stack
            .get_into(offset + size_offset as u64, &mut size_buf)?;
        let block_size = u32::from_le_bytes(size_buf) as usize;

        let mut len_buf = [0u8; 4];
        let len_offset = 8;
        self.stack
            .get_into(offset + len_offset as u64, &mut len_buf)?;
        let data_len = u32::from_le_bytes(len_buf) as usize;

        let payload_cap = block_size - BLOCK_HEADER_SIZE;
        if data_len > payload_cap {
            return Err(Error::ChecksumMismatch { block: offset });
        }

        if data_len > buf.len() {
            return Err(Error::ChecksumMismatch { block: offset });
        }

        if data_len > 0 {
            let pay_start = 12;
            self.stack
                .get_into(offset + pay_start as u64, &mut buf[0..data_len])?;
        }

        let total = block_size;
        let full = self.stack.get(offset, offset + total as u64)?;
        verify_checksum(&full, offset)?;

        Ok(data_len)
    }

    pub fn write_payload(&self, offset: u64, data: &[u8]) -> Result<(), Error> {
        let size_offset = 4;
        let mut size_buf = [0u8; 4];
        self.stack
            .get_into(offset + size_offset as u64, &mut size_buf)?;
        let block_size = u32::from_le_bytes(size_buf) as usize;

        let payload_cap = block_size - BLOCK_HEADER_SIZE;
        if data.len() > payload_cap {
            return Err(Error::DataTooLarge {
                capacity: payload_cap,
                provided: data.len(),
            });
        }

        let total = block_size;
        let mut buf = self.stack.get(offset, offset + total as u64)?;
        verify_checksum(&buf, offset)?;

        let pay_start = 12;
        buf[pay_start..pay_start + data.len()].copy_from_slice(data);

        let len_offset = 8;
        buf[len_offset..len_offset + 4].copy_from_slice(&(data.len() as u32).to_le_bytes());

        write_checksum(&mut buf);
        self.stack.set(offset, &buf)?;
        Ok(())
    }

    pub fn capacity(&self, offset: u64) -> Result<usize, Error> {
        let min_block_offset = ALLOC_OFFSET + HEADER_SIZE;
        if offset < min_block_offset {
            return Err(Error::InvalidBlock);
        }
        let size_offset = 4;
        let mut size_buf = [0u8; 4];
        self.stack
            .get_into(offset + size_offset as u64, &mut size_buf)?;
        let block_size = u32::from_le_bytes(size_buf) as usize;
        Ok(block_size.saturating_sub(BLOCK_HEADER_SIZE))
    }

    pub fn data_len(&self, offset: u64) -> Result<usize, Error> {
        let min_block_offset = ALLOC_OFFSET + HEADER_SIZE;
        if offset < min_block_offset {
            return Err(Error::InvalidBlock);
        }
        let len_offset = 8;
        let mut len_buf = [0u8; 4];
        self.stack
            .get_into(offset + len_offset as u64, &mut len_buf)?;
        Ok(u32::from_le_bytes(len_buf) as usize)
    }

    pub fn data_start(&self, offset: u64) -> u64 {
        offset + 12
    }

    pub fn data_end(&self, offset: u64) -> Result<u64, Error> {
        let data_len = self.data_len(offset)? as u64;
        Ok(self.data_start(offset) + data_len)
    }
}

impl BStackAllocator for BinAlloc {
    fn stack(&self) -> &BStack {
        &self.stack
    }

    fn into_stack(self) -> BStack {
        self.stack
    }

    fn alloc(&self, len: u64) -> Result<BStackSlice<'_, Self>, std::io::Error> {
        let offset = self
            .alloc_inner(len as usize)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let size_offset = 4;
        let mut size_buf = [0u8; 4];
        self.stack.get_into(offset + size_offset, &mut size_buf)?;
        let block_size = u32::from_le_bytes(size_buf) as u64;
        let payload_cap = block_size - 12;
        Ok(BStackSlice::new(self, offset + 12, payload_cap))
    }

    fn realloc<'a>(
        &'a self,
        _slice: BStackSlice<'a, Self>,
        _new_len: u64,
    ) -> Result<BStackSlice<'a, Self>, std::io::Error> {
        Err(std::io::Error::other("realloc not supported"))
    }

    fn dealloc(&self, _slice: BStackSlice<'_, Self>) -> Result<(), std::io::Error> {
        // No-op, no deallocation
        Ok(())
    }

    fn len(&self) -> Result<u64, std::io::Error> {
        let total = self
            .stack
            .len()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        // Subtract reserved + allocator header to get usable space
        Ok(total.saturating_sub(RESERVED_SIZE + HEADER_SIZE))
    }

    fn is_empty(&self) -> Result<bool, std::io::Error> {
        let total = self
            .stack
            .len()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        // Empty if there's no data beyond reserved + allocator header
        Ok(total <= RESERVED_SIZE + HEADER_SIZE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn tmp(label: &str) -> std::path::PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "bllist_alloc_{}_{}_{}.bla",
            std::process::id(),
            label,
            n
        ));
        p
    }

    #[test]
    fn fresh_open_creates_header() {
        let path = tmp("fresh");
        let stack = BStack::open(&path).unwrap();
        let alloc = BinAlloc::new(stack).unwrap();
        assert!(alloc.is_empty().unwrap());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn write_read_roundtrip() {
        let path = tmp("rw");
        let stack = BStack::open(&path).unwrap();
        let alloc = BinAlloc::new(stack).unwrap();

        let slice = alloc.alloc(10).unwrap();
        slice.write(b"hello alloc!").unwrap();

        let data = slice.read().unwrap();
        assert_eq!(data, b"hello alloc!");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn capacity_and_data_len() {
        let path = tmp("cap");
        let stack = BStack::open(&path).unwrap();
        let alloc = BinAlloc::new(stack).unwrap();

        let slice = alloc.alloc(4).unwrap();
        // Block starts at: slice.start() - 12
        // A 4-byte request gets a 32-byte block with 20-byte payload capacity
        assert_eq!(alloc.capacity(slice.start() - 12).unwrap(), 20);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn block_size_for_values() {
        // New layout: 4 (checksum) + 4 (block_size) + 4 (data_len) + payload = 12 bytes overhead
        // Minimum bin = 32 bytes (bin 5), so minimum payload = 20 bytes
        assert_eq!(block_size_for(0), 32); // 12 + 0 <= 32
        assert_eq!(block_size_for(4), 32); // 12 + 4 <= 32
        assert_eq!(block_size_for(20), 32); // 12 + 20 == 32
        assert_eq!(block_size_for(21), 64); // 12 + 21 > 32
    }
}
