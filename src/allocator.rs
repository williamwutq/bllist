//! Bin-based block allocator with native CRC32 checksumming.
//!
//! This module provides a [`BinAlloc`] implementing [`bstack::BStackAllocator`]
//! that manages variable-size blocks.
//! Every block is protected by a CRC32 checksum.
//!
//! ## File layout
//!
//! ```text
//! ┌─────────────────────────┬────────────────────┬───────────────────────────┐
//! │  BStack header (16 B)   │   Unused (32 B)    │  Allocator header (16 B)  │
//! │  "BSTK" magic + clen    │                    │  "ALBL" + version         │
//! ├─────────────────────────┴────────────────────┴───────────────────────────┤
//! │  Block (total size = 2^k bytes)                                          │
//! │  checksum(4) │ block_size(4) │ data_len(4) │ payload                     │
//! ├──────────────────────────────────────────────────────────────────────────┤
//! │  Block …                                                                 │
//! └──────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! **Allocator header** (16 bytes at logical offset 32):
//! - `[0..4]`      — CRC32 checksum of bytes `[4..16]`
//! - `[4..8]`      — magic `"ALBL"`
//! - `[8..12]`     — version `u32`
//! - `[12..16]`    — CRC32 checksum of the bin pointers (bytes `[48..528]` in logical offset)
//!
//! **Block on-disk layout**:
//! - `[0..4]`              — CRC32 of bytes `[4..2^block_class]`
//! - `[4..8]`              — `block_class` `u32` (total bytes on disk, stored as 2^block_class)
//! - `[8..16]`             — `data_len` `u64` (payload bytes written, 2^block_class to indicate in the free list)
//! - `[16..2^block_class]` — payload
//!
//! ## Block size bins
//!
//! Blocks are allocated in bins based on their payload size. The block size is
//! the smallest power of two that can fit the payload plus the 16 bytes of block
//! header. For example, a payload of 100 bytes would require a block size of 128
//! bytes (bin 7), while a payload of 2000 bytes would require a block size of 2048
//! bytes (bin 11).
//!
//! The pointer to the bin (bin 5) is located at logical offset 48, immediately after
//! the allocator header. Bin 6 is located at offset 56, bin 7 at offset 64, and so on.
//! Each bin pointer is 8 bytes little-endian offset to the head of the free list for
//! that bin, or 0 if the bin is empty.
//!
//! The largest bin will be bin 64, which can hold blocks up to 2^64 - 16 bytes, but in
//! practice the maximum block size is limited by the maximum file size of the underlying
//! BStack, fragmentation, and the fact that other blocks exist in the file. This means
//! the total number of bins is 60 (bin 5 to bin 64 inclusive), and the bin pointer file
//! occupies 60 * 8 = 480 bytes, which fits in the logical offset range [48..528]
//! immediately after the allocator header.

use bstack::{BStack, BStackAllocator, BStackSlice, FirstFitBStackAllocator};

use crate::Error;
use std::io;

/// The magic prefix for the allocator header, used to identify the file format
const ALBL_MAGIC_PREFIX: [u8; 4] = *b"ALBL";

/// The version number of the allocator format, encoded as a 4-byte integer.
const ALBL_VERSION: u32 = 0x00010000; // version 0.1

/// Mask to ignore patch version (0.1.x)
const VERSION_MASK: u32 = 0xFFFF0000;

/// Full magic for the allocator
///
/// This is generated at compile time by combining the magic prefix and version number.
/// It is stored in the file header and used to validate the file format and version
const ALBL_MAGIC: [u8; 8] = [
    ALBL_MAGIC_PREFIX[0],
    ALBL_MAGIC_PREFIX[1],
    ALBL_MAGIC_PREFIX[2],
    ALBL_MAGIC_PREFIX[3],
    (ALBL_VERSION & 0xFF) as u8,
    ((ALBL_VERSION >> 8) & 0xFF) as u8,
    ((ALBL_VERSION >> 16) & 0xFF) as u8,
    ((ALBL_VERSION >> 24) & 0xFF) as u8,
];

const RESERVED_SIZE: u64 = 32;
const ALLOC_OFFSET: u64 = RESERVED_SIZE;
const HEADER_SIZE: u64 = 16;
const BIN_POINTERS_OFFSET: u64 = ALLOC_OFFSET + HEADER_SIZE;
const BIN_POINTERS_SIZE: u64 = (MAX_BLOCK_CLASS as u64 - MIN_BIN as u64 + 1) * 8;
const BIN_POINTERS_END: u64 = BIN_POINTERS_OFFSET + BIN_POINTERS_SIZE;
const BLOCK_HEADER_SIZE: u64 = 16;

const MIN_BIN: usize = 5;
const MAX_BLOCK_CLASS: u32 = 64;

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

fn verify_checksum_stdio(buf: &[u8], offset: u64) -> Result<(), io::Error> {
    verify_checksum(buf, offset).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Recomputes and updates the checksum for the block at the given offset.
fn rechecksum(stack: &BStack, start: u64, end: u64) -> Result<(), io::Error> {
    let mut buf = vec![0u8; (end - start) as usize];
    stack.get_into(start, &mut buf)?;
    write_checksum(&mut buf);
    stack.set(start, &buf)?;
    Ok(())
}

/// Helper function to create a buffer of the given length filled with
/// the pattern 0xDEADBEEF for testing purposes.
#[cfg(debug_assertions)]
fn make_deadbeef_vec(len: usize) -> Vec<u8> {
    let l = len.div_ceil(4);
    let mut buf: Vec<u32> = vec![0xDEADBEEF; l];
    // Transmute to u8 vector without copying
    unsafe {
        std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, l * 4).to_vec() // Convert to Vec<u8>
    }
}

/// Helper function to fill a buffer with the pattern 0xDEADBEEF for testing purposes.
#[cfg(debug_assertions)]
fn write_deadbeef(buf: &mut [u8]) {
    let pattern = 0xDEADBEEF_u32.to_le_bytes();
    for chunk in buf.chunks_mut(4) {
        let len = chunk.len();
        chunk.copy_from_slice(&pattern[..len]);
    }
}

/// # ReservedAllocator
///
/// A trait for allocators that reserve a certain amount of space at the beginning of the file
/// for other purposes. This is used to allow the user to use the reserved space for their own
/// metadata, and the allocator will not touch that region. The allocator will start allocating
/// blocks after the reserved region. The reserved region is also not included in the checksum
/// calculations for the allocator header, so the user can freely modify the reserved region
/// without affecting the integrity of the allocator.
///
/// ## Safety
/// The implementor must ensure that the reserved region is not used by the allocator and is not
/// included in the checksum calculations for the allocator header. Since the allocator have total
/// control of the underlying [`BStack`] and the file layout, violating these requirements may
/// lead to undefined behavior such as header corruption, incorrect checksum calculations, and
/// potential security vulnerabilities.
pub unsafe trait ReservedAllocator {
    const RESERVED_SIZE: u64 = 0;
}

unsafe impl ReservedAllocator for BinAlloc {
    const RESERVED_SIZE: u64 = RESERVED_SIZE;
}

// Check this each breaking change of Bstack
/// Safety: Documentation of FirstFitBStackAllocator specifies that 16 bytes are reserved
unsafe impl ReservedAllocator for FirstFitBStackAllocator {
    const RESERVED_SIZE: u64 = 16;
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug)]
pub enum RepairLevel {
    /// No repair, just return errors when corruption is detected
    None,

    /// Repair the header if the header checksum is invalid but the bin pointer checksum is valid
    HeaderIfValidBinPointers,

    /// Repair bin pointer region corruption by zeroing out all bin pointers and treating all bins as empty
    /// then rebuild the free list by traversing the entire file and adding all blocks that are not marked as allocated
    RecollectOnCorruption,

    /// Always repair bin pointer region corruption by zeroing out all bin pointers and treating all bins as empty
    /// on each open and gather orphans. Does not always repair blocks.
    AlwaysRecollect,

    /// RecollectOnCorruption, but also always repair block checksum corruption by trusting block content
    RecollectAndTrustBlocks,

    /// Always repair bin pointer region corruption by zeroing out all bin pointers and treating all bins as empty
    /// on each open, and also always repair block checksum corruption by trusting block content
    AlwaysRecollectAndTrustBlocks,

    /// Force all repairs regardless of checksum validity
    ForceAllRepairs,
}

#[derive(Debug)]
pub struct BinAlloc {
    stack: BStack,
    repair_level: RepairLevel,
}

unsafe impl Send for BinAlloc {}
unsafe impl Sync for BinAlloc {}

impl BinAlloc {
    pub fn new(stack: BStack, repair_level: RepairLevel) -> Result<Self, io::Error> {
        // Get the total size of the file. If repair_level is ForceAllRepairs, we will discard all existing
        // content and treat the file as empty if it's in an unrepairable state
        let total = if repair_level == RepairLevel::ForceAllRepairs {
            let total = stack.len()?;
            if total < RESERVED_SIZE + HEADER_SIZE {
                stack.discard(total)?;
                0
            } else {
                total
            }
        } else {
            stack.len()?
        };
        if total == 0 {
            // Case 1: new file, need to initialize header and bin pointers

            // Push reserved and header
            let mut head_buf = [0u8; (RESERVED_SIZE + HEADER_SIZE) as usize];

            // Write header
            head_buf[RESERVED_SIZE as usize..(RESERVED_SIZE + HEADER_SIZE) as usize]
                .copy_from_slice(&Self::make_header());

            // Write checksum for the header
            write_checksum(
                &mut head_buf[RESERVED_SIZE as usize..(RESERVED_SIZE + HEADER_SIZE) as usize],
            );
            stack.push(&head_buf)?;

            // Push bin pointer region (480 bytes)
            // If this step fails and the previous one succeed, go to case 3 on the next open,
            // which will regenerate the bin pointer region with zeros and extend the file to the
            // full size of the bin pointer region.
            let bin_pointers = [0u8; BIN_POINTERS_SIZE as usize];
            stack.push(&bin_pointers)?;

            Ok(Self {
                stack,
                repair_level,
            })
        } else if total < RESERVED_SIZE + HEADER_SIZE {
            // Case 2: file is too small to contain the header, treat as corruption since
            // it cannot be a file with an allocator
            // Unless repair_level is ForceAllRepairs, in which case we will discard all
            // existing content and treat the file as empty where Case 1 will be taken
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "file too small to contain allocator header: total size {total} bytes, \
                     but at least {} bytes are required",
                    RESERVED_SIZE + HEADER_SIZE
                ),
            ))
        } else if total < RESERVED_SIZE + HEADER_SIZE + BIN_POINTERS_SIZE {
            // Case 3: file is too small to contain the bin pointer region, but large enough to contain the header

            // Regenerate the bin pointer file with all zeros
            let shrink_size = total - (RESERVED_SIZE + HEADER_SIZE);

            // We discard the existing bin pointer file since it should be all zeros
            // This is because the file is physically not large enough to contain any valid blocks
            // Which means all free lists must be empty, so the bin pointers must all be zero
            stack.discard(shrink_size)?;

            // Extend the file to the full size of the bin pointer region with zeros.
            // If this step fail and the previous one succeed, on the next open we will hit
            // the same case except this time discard 0 will be a no-op and this step will be
            // retried.
            stack.extend(BIN_POINTERS_SIZE)?;

            Ok(Self {
                stack,
                repair_level,
            })
        } else {
            // Case 4: file is large enough to contain the header and bin pointer region, validate them

            // Validate header first since it is needed to validate the bin pointer region
            let mut hdr_buf = [0u8; HEADER_SIZE as usize];
            stack.get_into(ALLOC_OFFSET, &mut hdr_buf)?;
            verify_checksum_stdio(&hdr_buf, ALLOC_OFFSET)?;
            let header_validation_result = Self::validate_header(&hdr_buf);

            // Validate checksum for the bin pointer region
            let bin_buf = stack.get(BIN_POINTERS_OFFSET, BIN_POINTERS_END)?;
            let crc = crc32fast::hash(&bin_buf);
            let stored_crc = u32::from_le_bytes(hdr_buf[12..16].try_into().unwrap());
            let should_check_blocks = repair_level >= RepairLevel::RecollectAndTrustBlocks;
            let mut should_rebuild_free_list = repair_level
                >= RepairLevel::AlwaysRecollectAndTrustBlocks
                || repair_level == RepairLevel::AlwaysRecollect;
            if crc != stored_crc {
                // Recover A
                if repair_level >= RepairLevel::RecollectOnCorruption {
                    should_rebuild_free_list = true;
                } else {
                    return Err(Error::stdio_corruption(format!(
                        "CRC32 checksum mismatch for bin pointer region: expected {stored_crc:#010X}, got {crc:#010X}"
                    )));
                }
            } else if header_validation_result.is_err() {
                // Recover X
                if repair_level != RepairLevel::None {
                    rechecksum(&stack, ALLOC_OFFSET, BIN_POINTERS_OFFSET)?;
                } else {
                    return Err(header_validation_result.err().unwrap());
                }
            }

            // Validate all pointers in the bin pointer region. We allow them to be zero (empty bin) or
            // valid block offsets, but not invalid non-zero offsets.
            // Cast to u64 slices for easier processing
            let bin_ptrs = unsafe {
                std::slice::from_raw_parts(
                    bin_buf.as_ptr() as *const u64,
                    (BIN_POINTERS_SIZE / 8) as usize,
                )
            };

            for &ptr in bin_ptrs {
                if !Self::is_valid_block_offset(ptr) {
                    // Recover A
                    if repair_level >= RepairLevel::RecollectOnCorruption {
                        should_rebuild_free_list = true;
                    } else {
                        return Err(Error::stdio_corruption(format!(
                            "invalid block offset {ptr} in bin pointer region"
                        )));
                    }
                }
            }

            if should_check_blocks || should_rebuild_free_list {
                // Tranverse list to fix potential "Recover B"
                if should_check_blocks {
                    for (class, &ptr) in (MIN_BIN as u32..).zip(bin_ptrs.iter()) {
                        let block_size = Self::size_from_class(class);
                        let mut head = ptr;
                        while head != 0 {
                            if !Self::is_valid_block_offset(head) {
                                if repair_level >= RepairLevel::RecollectOnCorruption {
                                    should_rebuild_free_list = true;
                                    // This also aborts the block repair since the list is corrupted
                                    break;
                                } else {
                                    return Err(Error::stdio_corruption(format!(
                                        "invalid block offset {head} in free list traversal"
                                    )));
                                }
                            }

                            let mut vec = stack.get(head, head + BLOCK_HEADER_SIZE + 8)?;
                            let mut write = false;
                            let block_class = u32::from_le_bytes(vec[4..8].try_into().unwrap());
                            let data_len = u64::from_le_bytes(vec[8..16].try_into().unwrap());
                            let next = u64::from_le_bytes(vec[16..24].try_into().unwrap());
                            // Check for correct block_class
                            if block_class != class {
                                if data_len != block_size {
                                    // This is sus
                                    // Check validity of class
                                    if class >= MIN_BIN as u32
                                        && class <= MAX_BLOCK_CLASS
                                        && Self::size_from_class(class) == block_size
                                    {
                                        // This was put into the wrong bin
                                        if repair_level >= RepairLevel::RecollectOnCorruption {
                                            should_rebuild_free_list = true;
                                        }
                                        break;
                                    } else if repair_level
                                        >= RepairLevel::AlwaysRecollectAndTrustBlocks
                                    {
                                        should_rebuild_free_list = true;
                                    } else {
                                        return Err(Error::stdio_corruption(format!(
                                            "invalid block class {} in block at offset {head} in free list traversal for bin class {class}",
                                            block_class
                                        )));
                                    }
                                }
                                // Repairs it
                                vec[4..8].copy_from_slice(&class.to_le_bytes());
                                write = true;
                            }

                            // Check for pausible data_len (not greater than block size - header size)
                            if data_len != block_size {
                                // Set to block_size to indicate free block, and it will be added back to the free list later
                                vec[8..16].copy_from_slice(&block_size.to_le_bytes());
                                write = true;
                            }

                            if write {
                                // Update checksum for the block
                                let mut full_block = stack.get(head, head + block_size)?;
                                #[cfg(debug_assertions)]
                                write_deadbeef(&mut full_block);
                                full_block[..BLOCK_HEADER_SIZE as usize + 8].copy_from_slice(&vec);
                                write_checksum(&mut full_block);
                                stack.set(head, &full_block)?;
                            }

                            head = next;
                        }
                    }
                }

                // Recover orphans
                if should_rebuild_free_list {
                    stack.zero(BIN_POINTERS_OFFSET, BIN_POINTERS_END)?;
                    todo!()
                }
            }

            Ok(Self {
                stack,
                repair_level,
            })
        }
    }

    fn make_header() -> [u8; HEADER_SIZE as usize] {
        let mut buf = [0u8; HEADER_SIZE as usize];
        buf[4..12].copy_from_slice(&ALBL_MAGIC);
        buf
    }

    fn validate_header(buf: &[u8]) -> Result<(), io::Error> {
        // Check prefix
        if buf[4..8] != ALBL_MAGIC_PREFIX {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "unsupported file format with magic prefix {:?}",
                    std::str::from_utf8(&buf[4..8]).unwrap_or(
                        // Use hex if the prefix is not valid UTF-8
                        &format!("0x{:02X?}", &buf[4..8])
                    )
                ),
            ));
        }
        // Parse version
        let version = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        // Support anything 0.1.x, but reject incompatible versions
        if version & VERSION_MASK != ALBL_VERSION & VERSION_MASK {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "incompatible allocator version: found {}, expected {}",
                    version, ALBL_VERSION
                ),
            ));
        }
        Ok(())
    }

    /// Checks if the given offset is a valid block offset, which means it is either zero (empty bin) or
    /// a valid block offset that is at least `BIN_POINTERS_END` and aligned to 32 bytes
    /// (since the minimum block size is 32 bytes) when subtracting BIN_POINTERS_END.
    fn is_valid_block_offset(offset: u64) -> bool {
        offset == 0 || (offset >= BIN_POINTERS_END && (offset - 16).is_multiple_of(32))
    }

    #[inline]
    fn block_class_for(payload_size: u64) -> u32 {
        let total_minus_one = BLOCK_HEADER_SIZE + payload_size - 1;
        let class = total_minus_one.ilog2() + 1;
        if class < MIN_BIN as u32 {
            MIN_BIN as u32
        } else {
            class
        }
    }

    #[inline]
    fn size_from_class(block_class: u32) -> u64 {
        1u64 << block_class
    }

    #[inline]
    fn find_bin_offset(block_class: u32) -> u64 {
        ALLOC_OFFSET + HEADER_SIZE + (block_class * 8u32) as u64
    }

    #[inline]
    fn find_bin_offset_in_slice(block_class: u32) -> usize {
        (block_class * 8u32) as usize + 4 // +4 to skip the checksum for the bin pointer region
    }

    /// Precondition:
    /// - File is aligned properly (BStack's length - 16 should be a multiple of 32)
    /// - Valid checksum for the bin pointer region (validated in new)
    /// - old_data goes from the beginning of the block to the end of the data
    ///   (not the end of the block)
    /// - old_ptr_to_free is either 0 or a valid block offset that can be added to the free list of its bin
    fn alloc_block(
        &self,
        len: u64,
        old_data: &mut [u8],
        old_ptr_to_free: u64,
    ) -> Result<u64, io::Error> {
        let block_class = Self::block_class_for(len);
        let block_size = Self::size_from_class(block_class);
        let bin_offset = Self::find_bin_offset(block_class);
        let mut head_buf = [0u8; 8];
        self.stack.get_into(bin_offset, &mut head_buf)?; // head_buf is reused
        let head = u64::from_le_bytes(head_buf);

        // Write block_class to old data
        old_data[4..8].copy_from_slice(&block_class.to_le_bytes());
        // Write data_len to old data, which is the length of the payload
        old_data[8..16].copy_from_slice(&len.to_le_bytes());
        // Write checksum for the data section
        write_checksum(old_data);

        if head == 0 {
            // TODO: try to split from existing larger blocks before extending the file

            // Case 1: empty bin, need to allocate a new block at the end of the file
            let mut new_slice = vec![0u8; block_size as usize];
            new_slice[..old_data.len()].copy_from_slice(old_data);

            // A single write
            self.stack.push(&new_slice)
        } else if !Self::is_valid_block_offset(head) {
            // Case 2: invalid block offset in the bin pointer, treat as corruption
            // since it cannot be a valid block
            Err(Error::stdio_corruption(format!(
                "invalid block offset {head} in bin pointer at offset {bin_offset}"
            )))
        } else {
            // Case 3: non-empty bin, pop the head block and return it
            // We will validate the block when we actually read it, so we don't need to validate it here
            self.stack
                .get_into(head + BLOCK_HEADER_SIZE, &mut head_buf)?; // head_buf is reused
            let next = u64::from_le_bytes(head_buf);

            // Write the old data to the popped block
            self.stack.set(head, old_data)?;

            // Compute the bin for the old block to free
            let old_class = Self::block_class_for(old_data.len() as u64 - BLOCK_HEADER_SIZE);
            let old_bin_offset = Self::find_bin_offset_in_slice(old_class);

            if old_ptr_to_free != 0 {
                // First mark the old block free
                // There is no consequence if we fail at this step since the old data is simply copied
                // Into the poped block, which is in the free list
                // Recover B: Correct list, block free marker (of the poped block) not present
                self.write_free_block(old_ptr_to_free, old_class)?;
            }

            // Load the entire bin pointer region into memory including the checksum above
            // Then, zero the entire bin pointer region. If this operation fail, and the free block
            // write was requested and succeed, we will have an orphaned block
            // Recover B: Correct list, block free marker (of the poped block) not present
            // Recover O: Orphaned block
            let mut buf = vec![0u8; (BIN_POINTERS_END - BIN_POINTERS_OFFSET + 4) as usize];
            self.stack.get_into(BIN_POINTERS_OFFSET - 4, &mut buf)?;
            self.stack.zero(BIN_POINTERS_OFFSET, BIN_POINTERS_END)?;

            // Write the next pointer to the bin pointer region
            let start = Self::find_bin_offset_in_slice(block_class);
            buf[start..start + 8].copy_from_slice(&next.to_le_bytes());

            if old_ptr_to_free != 0 {
                // Write the old block offset to the bin pointer region to free it
                buf[old_bin_offset..old_bin_offset + 8]
                    .copy_from_slice(&old_ptr_to_free.to_le_bytes());
            }

            // Update checksum for the bin pointer region
            write_checksum(&mut buf);

            // Commit all the new pointers
            // Recover A: Missing bin pointers
            self.stack.set(BIN_POINTERS_OFFSET - 4, &buf)?;

            // update checksum for the header
            // Recover X: Incorrect header checksum but correct pointer checksum
            rechecksum(&self.stack, ALLOC_OFFSET, BIN_POINTERS_OFFSET)?;

            Ok(head)
        }
    }

    /// Precondition:
    /// - File is aligned properly (BStack's length - 16 should be a multiple of 32)
    fn resize_block(&self, offset: u64, len: u64) -> Result<(), io::Error> {
        // Read the entire block (header + data) into memory,
        // since we need to update the block header and checksum
        let block_class = Self::block_class_for(len);
        let block_size = Self::size_from_class(block_class);
        let mut buf = self.stack.get(offset, offset + block_size)?;
        let current_data_len = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        // Check the current checksum and block class
        match verify_checksum_stdio(
            &buf[..(current_data_len + BLOCK_HEADER_SIZE) as usize],
            offset,
        ) {
            Ok(_) => {}
            Err(_) if self.repair_level >= RepairLevel::RecollectAndTrustBlocks => {}
            Err(e) => return Err(e),
        }

        // Figure out the current block class
        let current_block_class = u32::from_le_bytes(buf[4..8].try_into().unwrap());

        // Validate
        if block_class > current_block_class {
            Err(Error::stdio_invalid_slice())
        } else {
            // Write new length to the block header.
            buf[8..16].copy_from_slice(&len.to_le_bytes());

            if block_class != current_block_class {
                // Also update the block class
                buf[4..8].copy_from_slice(&block_size.trailing_zeros().to_le_bytes());

                // Add to free list
                // |B|5| 4 | Add 3 |  Addition 2  |            Addition 1           |
                //  ^ Resized block

                // Load the entire free pointer region into memory including the checksum above
                let mut free_list_buf =
                    vec![0u8; (BIN_POINTERS_END - BIN_POINTERS_OFFSET + 4) as usize];
                self.stack
                    .get_into(BIN_POINTERS_OFFSET - 4, &mut free_list_buf)?;
                self.stack.zero(BIN_POINTERS_OFFSET, BIN_POINTERS_END)?;

                for k in block_class..current_block_class {
                    let free_offset = offset + (1u64 << k);
                    self.write_free_block(free_offset, k)?;
                    let bin_slice_offset = Self::find_bin_offset_in_slice(k);
                    free_list_buf[bin_slice_offset..bin_slice_offset + 8]
                        .copy_from_slice(&free_offset.to_le_bytes());
                }
                write_checksum(&mut free_list_buf);
                self.stack.set(BIN_POINTERS_OFFSET - 4, &free_list_buf)?;
                rechecksum(&self.stack, ALLOC_OFFSET, BIN_POINTERS_OFFSET)?;
            }

            // Update checksum for the block
            write_checksum(&mut buf[..(len + BLOCK_HEADER_SIZE) as usize]);

            // Write the updated block back to the file
            self.stack.set(offset, &buf)
        }
    }

    /// This function is atomic
    fn write_free_block(&self, offset: u64, block_class: u32) -> Result<(), io::Error> {
        let block_size = Self::size_from_class(block_class);

        #[cfg(not(debug_assertions))]
        let mut buf = vec![0u8; block_size as usize];
        #[cfg(debug_assertions)]
        let mut buf = make_deadbeef_vec(block_size as usize);

        // Write the block class to the block header
        buf[4..8].copy_from_slice(&block_class.to_le_bytes());

        // Write "data_len" to be block_size to indicate the block is free
        buf[8..16].copy_from_slice(&block_size.to_le_bytes());

        // Write the next pointer for the free block into the buffer directly
        // which is the current head of the bin
        let bin_offset = Self::find_bin_offset(block_class);
        self.stack.get_into(
            bin_offset,
            &mut buf[BLOCK_HEADER_SIZE as usize..(BLOCK_HEADER_SIZE + 8) as usize],
        )?;

        // Recompute checksum for the free block
        write_checksum(&mut buf);

        self.stack.set(offset, &buf)?;
        Ok(())
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
        let zero_buf = &mut [0u8; BLOCK_HEADER_SIZE as usize];
        let offset = Self::alloc_block(self, len, zero_buf, 0)?;
        Ok(BStackSlice::new(self, offset + BLOCK_HEADER_SIZE, len))
    }

    fn realloc<'a>(
        &'a self,
        slice: BStackSlice<'a, Self>,
        new_len: u64,
    ) -> Result<BStackSlice<'a, Self>, std::io::Error> {
        let len = slice.len();
        let new_block_class = Self::block_class_for(new_len);
        let block_class = Self::block_class_for(len);
        let old_ptr = slice.start() - BLOCK_HEADER_SIZE;

        if !Self::is_valid_block_offset(old_ptr) {
            return Err(Error::stdio_invalid_slice());
        }

        if new_block_class <= block_class {
            self.resize_block(old_ptr, new_len)?;
            Ok(BStackSlice::new(self, slice.start(), new_len))
        } else {
            // Prepare data to copy to the new block
            let mut old_data_buf = self.stack.get(old_ptr, slice.start() + len)?;
            let new_offset = Self::alloc_block(self, new_len, &mut old_data_buf, old_ptr)?;
            Ok(BStackSlice::new(
                self,
                new_offset + BLOCK_HEADER_SIZE,
                new_len,
            ))
        }
    }

    fn dealloc(&self, slice: BStackSlice<'_, Self>) -> Result<(), std::io::Error> {
        let block_class = Self::block_class_for(slice.len());
        let old_ptr = slice.start() - BLOCK_HEADER_SIZE;
        let bin_offset = Self::find_bin_offset_in_slice(block_class);

        if !Self::is_valid_block_offset(old_ptr) {
            return Err(Error::stdio_invalid_slice());
        }

        // First mark the old block free
        // There is no consequence if we fail at this step since the old data is simply copied
        // Into the poped block, which is in the free list
        self.write_free_block(old_ptr, block_class)?;

        // Load the entire bin pointer region into memory including the checksum above
        // Then, zero the entire bin pointer region. If this operation fail, and the free block
        // write was requested and succeed, we will have an orphaned block
        // Recover O: Orphaned block
        let mut buf = vec![0u8; (BIN_POINTERS_END - BIN_POINTERS_OFFSET + 4) as usize];
        self.stack.get_into(BIN_POINTERS_OFFSET - 4, &mut buf)?;
        self.stack.zero(BIN_POINTERS_OFFSET, BIN_POINTERS_END)?;

        // Write the old block offset to the bin pointer region to free it
        buf[bin_offset..bin_offset + 8].copy_from_slice(&old_ptr.to_le_bytes());

        // Recompute checksum for the bin pointer region
        write_checksum(&mut buf);

        // Commit all the new pointers
        // Recover A: Missing bin pointers
        self.stack.set(BIN_POINTERS_OFFSET - 4, &buf)?;

        // update checksum for the header
        // Recover X: Incorrect header checksum but correct pointer checksum
        rechecksum(&self.stack, ALLOC_OFFSET, BIN_POINTERS_OFFSET)
    }
}
