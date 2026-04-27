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
//! **Allocator header** (16 bytes at logical offset 32):
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
const ALLOC_OFFSET: u64 = RESERVED_SIZE;
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

#[derive(Debug)]
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
}

impl BStackAllocator for BinAlloc {
    fn stack(&self) -> &BStack {
        &self.stack
    }

    fn into_stack(self) -> BStack {
        self.stack
    }

    fn alloc(&self, len: u64) -> Result<BStackSlice<'_, Self>, std::io::Error> {
        todo!()
    }

    fn realloc<'a>(
        &'a self,
        slice: BStackSlice<'a, Self>,
        new_len: u64,
    ) -> Result<BStackSlice<'a, Self>, std::io::Error> {
        todo!()
    }

    fn dealloc(&self, _slice: BStackSlice<'_, Self>) -> Result<(), std::io::Error> {
        todo!()
    }
}
