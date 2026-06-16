use bstack::{BStack, BStackAllocator, BStackSlice};

use std::{io, num::NonZeroU64, sync::Mutex};

/// In debug builds, panics immediately to give you a stack trace and a core dump.
/// In release builds, returns an `Err(io::Error)` so the caller can handle it gracefully.
macro_rules! debug_panic_or_io_err {
    ($kind:ident, $fmt:literal $(, $arg:expr)*) => {{
        #[cfg(debug_assertions)]
        panic!($fmt $(, $arg)*);

        #[cfg(not(debug_assertions))]
        return Err(io::Error::new(
            io::ErrorKind::$kind,
            format!($fmt $(, $arg)*),
        ))?;
    }};
}

macro_rules! get_le {
    ($buf:expr; $t:ty) => {
        <$t>::from_le_bytes((&$buf).try_into().unwrap())
    };
}

/// The magic prefix for the allocator header, used to identify the file format
const ALCR_MAGIC_PREFIX: [u8; 4] = *b"ALCR";

/// The version number of the allocator format, encoded as a 4-byte integer.
const ALCR_VERSION: u32 = 0x00010000; // version 0.1

/// Mask to ignore patch version (0.1.x)
const VERSION_MASK: u32 = 0xFFFF0000;

/// Full magic for the allocator
///
/// This is generated at compile time by combining the magic prefix and version number.
/// It is stored in the file header and used to validate the file format and version
const ALBL_MAGIC: [u8; 8] = [
    ALCR_MAGIC_PREFIX[0],
    ALCR_MAGIC_PREFIX[1],
    ALCR_MAGIC_PREFIX[2],
    ALCR_MAGIC_PREFIX[3],
    (ALCR_VERSION & 0xFF) as u8,
    ((ALCR_VERSION >> 8) & 0xFF) as u8,
    ((ALCR_VERSION >> 16) & 0xFF) as u8,
    ((ALCR_VERSION >> 24) & 0xFF) as u8,
];

/// Aligns the given value up to the nearest multiple of 32 bytes.
///
/// Careful with zeros, which return 0.
#[inline]
fn align_32(value: u64) -> u64 {
    (value + 31) & !31
}

#[inline]
fn nonzero_into_u64(nonzero: Option<NonZeroU64>) -> u64 {
    nonzero.map_or(0, |n| n.get())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AllocClass {
    Small32 = 1,
    Small64,
    Small96,
    Small128,
    Small160,
    Small192,
    Small224,
    Small256,
    MediumUnsorted,
    // The class increment of medium is 2n + 32 due to the 32 byte block overhead
    /// Class 512
    Medium544,
    /// Class 1024
    Medium1120,
    /// Class 2048
    Medium2272,
    /// Class 4096
    Medium4576,
    /// Class 8192
    Medium9184,
    /// Class 16384
    Medium18400,
    /// Any size bigger than 18400 bytes
    /// When used as an option, this means the allocator will try to extend the file
    LargeOrExtend,
}

impl AllocClass {
    /// Gets the maximum data size for this allocation class, excluding the 32 byte block overhead.
    fn max_data_size(&self) -> Option<NonZeroU64> {
        match self {
            AllocClass::Small32 => NonZeroU64::new(32),
            AllocClass::Small64 => NonZeroU64::new(64),
            AllocClass::Small96 => NonZeroU64::new(96),
            AllocClass::Small128 => NonZeroU64::new(128),
            AllocClass::Small160 => NonZeroU64::new(160),
            AllocClass::Small192 => NonZeroU64::new(192),
            AllocClass::Small224 => NonZeroU64::new(224),
            AllocClass::Small256 => NonZeroU64::new(256),
            AllocClass::MediumUnsorted => None,
            AllocClass::Medium544 => NonZeroU64::new(544),
            AllocClass::Medium1120 => NonZeroU64::new(1120),
            AllocClass::Medium2272 => NonZeroU64::new(2272),
            AllocClass::Medium4576 => NonZeroU64::new(4576),
            AllocClass::Medium9184 => NonZeroU64::new(9184),
            AllocClass::Medium18400 => NonZeroU64::new(18400),
            AllocClass::LargeOrExtend => NonZeroU64::new(u64::MAX), // special case for large or extend
        }
    }

    /// Gets the index in the pointer table for this allocation class, if it has one.
    fn index_bin(&self) -> u64 {
        let num: u64 = (*self) as u64;
        ALLOC_PTR_TABLE_START + num * 8
    }

    /// Gets the allocation class for a given size, based on the defined allocation classes and their maximum data sizes.
    fn alloc_from_size(size: u64) -> AllocClass {
        if size <= 32 {
            AllocClass::Small32
        } else if size <= 64 {
            AllocClass::Small64
        } else if size <= 96 {
            AllocClass::Small96
        } else if size <= 128 {
            AllocClass::Small128
        } else if size <= 160 {
            AllocClass::Small160
        } else if size <= 192 {
            AllocClass::Small192
        } else if size <= 224 {
            AllocClass::Small224
        } else if size <= 256 {
            AllocClass::Small256
        } else if size <= 304 {
            AllocClass::MediumUnsorted
        } else if size <= 544 {
            AllocClass::Medium544
        } else if size <= 628 {
            AllocClass::MediumUnsorted
        } else if size <= 1120 {
            AllocClass::Medium1120
        } else if size <= 1276 {
            AllocClass::MediumUnsorted
        } else if size <= 2272 {
            AllocClass::Medium2272
        } else if size <= 2572 {
            AllocClass::MediumUnsorted
        } else if size <= 4576 {
            AllocClass::Medium4576
        } else if size <= 5164 {
            AllocClass::MediumUnsorted
        } else if size <= 9184 {
            AllocClass::Medium9184
        } else if size <= 10348 {
            AllocClass::MediumUnsorted
        } else if size <= 18400 {
            AllocClass::Medium18400
        } else {
            AllocClass::LargeOrExtend
        }
    }

    fn free_from_size(size: u64) -> Option<AllocClass> {
        debug_assert!(size.is_multiple_of(32));
        match size / 32 {
            0 => None,
            1 => Some(AllocClass::Small32),
            2 => Some(AllocClass::Small64),
            3 => Some(AllocClass::Small96),
            4 => Some(AllocClass::Small128),
            5 => Some(AllocClass::Small160),
            6 => Some(AllocClass::Small192),
            7 => Some(AllocClass::Small224),
            8 => Some(AllocClass::Small256),
            17 => Some(AllocClass::Medium544),
            35 => Some(AllocClass::Medium1120),
            71 => Some(AllocClass::Medium2272),
            143 => Some(AllocClass::Medium4576),
            287 => Some(AllocClass::Medium9184),
            575 => Some(AllocClass::Medium18400),
            576..=u64::MAX => Some(AllocClass::LargeOrExtend),
            _ => Some(AllocClass::MediumUnsorted),
        }
    }

    /// Returns true if this allocation class has a fixed size, meaning
    /// it can only be used for allocations of a specific size.
    fn is_exact_size(&self) -> bool {
        !Self::not_exact_size(self)
    }

    /// Returns true if this allocation class does not have a fixed size,
    /// meaning it can be used for multiple sizes of allocations.
    fn not_exact_size(&self) -> bool {
        matches!(self, AllocClass::MediumUnsorted | AllocClass::LargeOrExtend)
    }

    /// Returns true if this allocation class is one of the small classes
    /// (32 to 256 bytes).
    fn is_small(&self) -> bool {
        matches!(
            self,
            AllocClass::Small32
                | AllocClass::Small64
                | AllocClass::Small96
                | AllocClass::Small128
                | AllocClass::Small160
                | AllocClass::Small192
                | AllocClass::Small224
                | AllocClass::Small256
        )
    }

    /// Returns true if this allocation class is one of the medium classes
    /// excluding the unsorted class (544 to 18400 bytes).
    fn is_medium(&self) -> bool {
        matches!(
            self,
            AllocClass::Medium544
                | AllocClass::Medium1120
                | AllocClass::Medium2272
                | AllocClass::Medium4576
                | AllocClass::Medium9184
                | AllocClass::Medium18400
        )
    }
}

impl Into<u8> for AllocClass {
    fn into(self) -> u8 {
        self as u8 - 1
    }
}

impl Into<u8> for &AllocClass {
    fn into(self) -> u8 {
        (*self) as u8 - 1
    }
}

const ALLOC_MARKER_BE: [u8; 8] = 0xBAADF00DDEADBEEFu64.to_be_bytes();
const ALLOC_MARKER_LE: [u8; 8] = 0xBAADF00DDEADBEEFu64.to_le_bytes();
const ALLOC_MARKER_SMALL: [u8; 2] = 0xDEAD_u16.to_be_bytes();
const ALLOC_MARKER_MINI: u8 = 0xEF;
const ALLOC_HEADER_START: u64 = 32;
const ALLOC_HEADER_SIZE: u64 = 16;
const ALLOC_CHECKSUM_START: u64 = ALLOC_HEADER_START + ALLOC_HEADER_SIZE; // 48
const ALLOC_CHECKSUM_SIZE: u64 = 48;
const ALLOC_PTR_TABLE_START: u64 = ALLOC_CHECKSUM_START + ALLOC_CHECKSUM_SIZE; // 96
const ALLOC_PTR_TABLE_COUNT: u64 = 16;
const ALLOC_PTR_TABLE_SIZE: u64 = ALLOC_PTR_TABLE_COUNT * core::mem::size_of::<u64>() as u64; // 128 bytes
const ALLOC_DATA_START: u64 = ALLOC_PTR_TABLE_START + ALLOC_PTR_TABLE_SIZE; // 224
/// This is the highest bit of the size field in the block header, used to indicate whether the block is currently allocated or free.
const ALLOC_IN_USE_FLAG: u64 = 1 << 63;
/// This is the second highest bit of the size field in the block header, used to indicate whether the block is sorted.
const ALLOC_IS_SORTED_FLAG: u64 = 1 << 62;
/// This is the third highest bit of the size field in the block header, used to indicate whether the block can be flexibly reallocated
const ALLOC_CAN_REALLOC_FLAG: u64 = 1 << 61;
/// This is the fourth highest bit of the size field in the block header, used to indicate whether the block is dirty and needs to be
/// zeroed
const ALLOC_DIRTY_FLAG: u64 = 1 << 60;
// The nullpointer is 0

#[derive(Debug)]
pub struct CairnAlloc {
    stack: BStack,
    medium_unsorted_mutex: Mutex<()>,
    large_unsorted_mutex: Mutex<()>,
}

impl CairnAlloc {
    fn validate_header(buf: &[u8]) -> Result<(), io::Error> {
        // Check prefix
        if buf[..4] != ALCR_MAGIC_PREFIX {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "unsupported file format with magic prefix {:?}",
                    std::str::from_utf8(&buf[..4]).unwrap_or(
                        // Use hex if the prefix is not valid UTF-8
                        &format!("0x{:02X?}", &buf[..4])
                    )
                ),
            ));
        }
        // Parse version
        let version = get_le!(buf[4..]; u32);
        // Support anything 0.1.x, but reject incompatible versions
        if version & VERSION_MASK != ALCR_VERSION & VERSION_MASK {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "incompatible allocator version: found {}, expected {}",
                    version, ALCR_VERSION
                ),
            ));
        }
        Ok(())
    }

    fn make_unchecked(stack: BStack) -> Self {
        Self {
            stack,
            medium_unsorted_mutex: Mutex::new(()),
            large_unsorted_mutex: Mutex::new(()),
        }
    }

    /// Converts a requested size to the size stored on disk
    /// More specifically, the procedure is:
    /// 1. Align the size to a multiple of 32 bytes, since the allocator works with 32 byte blocks
    /// 2. Divide the size by 32 to get the number of blocks needed
    /// 3. Mask the result to fit in 58 bits
    ///
    /// 58 bit is enough since the maximum block size is 2^63 bytes, which is 2^58 blocks, thus
    /// we only need 58 bits to store the block count. The upper bits are used for flags
    fn size_to_disk_size(size: u64) -> u64 {
        ((size + 31) / 32) & 0x3FFFFFFFFFFFFFF
    }

    /// Converts a size stored on disk back to the actual size
    ///
    /// This method will never overflow u64
    fn disk_size_to_size(disk_size: u64) -> u64 {
        (disk_size & 0x3FFFFFFFFFFFFFF) * 32
    }

    pub fn new(stack: BStack) -> io::Result<Self> {
        let stack_len = stack.len()?;
        if stack_len == 0 {
            stack.extend(ALLOC_DATA_START)?;
            stack.set(ALLOC_HEADER_START, &ALBL_MAGIC)?;
            Ok(Self::make_unchecked(stack))
        } else if stack_len < 192 {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Stack too small for allocator: {} bytes", stack_len),
            ))
        } else if stack_len % 32 != 0 {
            // This is not repairable and is definitely caused by some kind of corruption,
            // since block tails are required to be complete.
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Stack length must be a multiple of 32 bytes: {} bytes",
                    stack_len
                ),
            ))
        } else {
            let buf = &mut [0u8; ALLOC_HEADER_SIZE as usize];
            stack.get_into(ALLOC_HEADER_START, buf)?;
            Self::validate_header(buf)?;
            // TODO: Maybe other validations
            Ok(Self::make_unchecked(stack))
        }
    }

    /// This function tries to link a free block at the given offset into the free list of the given class,
    /// assuming that offset is a valid free block of the given class with correct metadata written and the
    /// value at offset is offset itself.
    ///
    #[inline]
    fn try_link_free_block_unchecked(&self, class: AllocClass, offset: u64) -> io::Result<()> {
        let bin_index = class.index_bin();
        self.stack.cross_exchange(bin_index, offset, 8)?;
        Ok(())
    }

    // Write additional deadbeef markers after the allocated section of the block to help detect buffer
    // overflows.
    //
    // This is safe for a newly allocated block or any allocated block in general since if we overwrite
    // out of bounds the first 8 bytes of the tail section should be the BE marker
    fn write_additional_deadbeef(&self, offset: u64, len: u64) -> io::Result<()> {
        if len % 8 == 0 {
            self.stack.set(offset + len, &ALLOC_MARKER_BE)
        } else {
            let mut buf = [0u8; 16];
            // buf[0..2].copy_from_slice(&ALLOC_MARKER_SMALL); This line is never used
            buf[2..4].copy_from_slice(&ALLOC_MARKER_SMALL);
            buf[4..6].copy_from_slice(&ALLOC_MARKER_SMALL);
            buf[6..8].copy_from_slice(&ALLOC_MARKER_SMALL);
            buf[8..].copy_from_slice(&ALLOC_MARKER_BE);
            // Let say the end looks like this:
            // ooooooEFDEADDEAD BAADFOODDEADBEEF
            // The data part (oooooo) can be determined by len % 8, which we skip
            let skip = (len % 8) as usize; // This cast is safe because skip is always less than 8
            if len % 2 != 0 {
                // We overwrite the u8 immediately after "skip"
                buf[skip] = ALLOC_MARKER_MINI;
            }
            self.stack.set(offset + len, &buf[skip..])
        }
    }

    /// Validates the additional deadbeef markers after the allocated section of the block to help detect buffer overflows.
    fn validate_additional_deadbeef(&self, offset: u64, len: u64) -> io::Result<bool> {
        let skip = (len % 8) as usize;
        let mut buf = [0u8; 16];
        self.stack.get_into(offset + len, &mut buf[skip..])?;
        // buf[8..] should be the BE marker
        if buf[8..] != ALLOC_MARKER_BE {
            return Ok(false);
        }
        // buf[skip] should be the MINI marker
        if len % 2 != 0 && buf[0] != ALLOC_MARKER_MINI {
            return Ok(false);
        }
        // Unrolled loop
        if skip >= 2 && buf[2..4] != ALLOC_MARKER_SMALL {
            return Ok(false);
        }
        if skip >= 4 && buf[4..6] != ALLOC_MARKER_SMALL {
            return Ok(false);
        }
        if skip >= 6 && buf[6..8] != ALLOC_MARKER_SMALL {
            return Ok(false);
        }
        Ok(true)
    }

    /// Validates the free block at the given offset for the given class by checking the metadata in the block
    /// header and tail.
    ///
    /// Use [0..32) bytes of the shared buffer for reading the metadata. This function should
    /// be called after we successfully claim a free block. The function will also attempt to zero the block if
    /// it is marked as dirty, which means it should be cleaned before handed out. If the block is not marked
    /// as dirty, in debug mode, we will check if the block is actually clean by reading through the entire block,
    /// which can help detect potential use after free issues in the application code or random corruption.
    /// In release mode, we will trust the metadata and skip this check for performance reasons.
    ///
    /// ## Panics
    ///
    /// In debug mode, this function will panic if it detects any corruption in the block metadata, such as
    /// mismatched header and tail, invalid markers, or incorrect flags. In release mode, it will return an error
    /// instead.
    fn validate_free_block(
        &self,
        class: AllocClass,
        offset: u64,
        shared_buf: &mut [u8],
    ) -> io::Result<()> {
        let head_offset = offset - 16;
        let class_size = nonzero_into_u64(class.max_data_size());
        let tail_offset = offset + class_size;

        // Block validation. We can check the metadata in the block header and tail to make sure the block is not corrupted
        self.stack.get_into(head_offset, &mut shared_buf[0..16])?;
        self.stack.get_into(tail_offset, &mut shared_buf[16..32])?;
        let block_meta_head = get_le!(shared_buf[0..8]; u64);
        if shared_buf[16..24] != ALLOC_MARKER_LE {
            if shared_buf[16..24] == ALLOC_MARKER_BE {
                #[cfg(debug_assertions)]
                panic!(
                    "Corrupted block found in at offset {}, indicating an error in cairnalloc realloc or dealloc logic",
                    offset
                );
            } else {
                // It is highly likely that this is an application level buffer overflow issue
                debug_panic_or_io_err!(
                    InvalidData,
                    "Corrupted block found at offset {}, indicating a buffer overflow issue and an use after free \
                    in the application code while using the block or random corruption",
                    offset
                );
            }
        }
        #[cfg(debug_assertions)]
        if shared_buf[0..8] != shared_buf[24..32] {
            // The header and tail metadata do not match, should not happen
            #[cfg(debug_assertions)]
            panic!(
                "Corrupted block found in at offset {}: header and tail metadata do not match",
                offset
            );
            // Trust header otherwise
        }
        let current_len = Self::disk_size_to_size(block_meta_head);
        #[cfg(debug_assertions)]
        {
            if class.is_exact_size() {
                if current_len != class.max_data_size().unwrap().get() {
                    // The block size does not match the class, should not happen
                    panic!(
                        "Corrupted block found in at offset {}: block size {} does not match class {:?}",
                        offset, current_len, class
                    );
                }
                if block_meta_head & ALLOC_IS_SORTED_FLAG == 0 {
                    // Exact size block should always be sorted, should not happen
                    panic!(
                        "Corrupted block found in at offset {}: exact size block is not marked as sorted",
                        offset
                    );
                }
            } else if block_meta_head & ALLOC_IS_SORTED_FLAG != 0 {
                // Non-exact size block should not be sorted, should not happen
                panic!(
                    "Corrupted block found in at offset {}: non-exact size block is marked as sorted",
                    offset
                );
            }
        }
        if block_meta_head & ALLOC_IN_USE_FLAG == 0 || shared_buf[0..8] != [0u8; 8] {
            // Used block in free list or corrupted block, should not happen
            debug_panic_or_io_err!(
                InvalidData,
                "Corrupted block found at offset {}: in-use flag is not set or block marker is not zero",
                offset
            );
        }
        if block_meta_head & ALLOC_DIRTY_FLAG != 0 {
            self.stack.zero(offset, class_size)?;
        } else {
            #[cfg(debug_assertions)]
            {
                let mut fault = 0u64;
                let mut cursor = offset;
                self.stack.get_batched_gen(|| {
                    // SAFETY: Slice `shared_buf` lives for the duration of the get_batched_gen call
                    let res = (cursor, unsafe { escape_slice(&mut shared_buf[..32]) });
                    if cursor == offset {
                        cursor += 32;
                        Some(res)
                    } else {
                        if cursor + 32 == offset {
                            // The first 16 bytes may contain the pointer to the next and prev free block
                            // but the rest should be zero
                            if shared_buf[16..32] != [0u8; 24] {
                                fault = cursor
                                    + 16
                                    + shared_buf[16..32].iter().position(|&b| b != 0).unwrap()
                                        as u64;
                            }
                        } else if shared_buf[..32] != [0u8; 32] {
                            fault =
                                cursor + shared_buf.iter().position(|&b| b != 0).unwrap() as u64;
                            return None;
                        }
                        cursor += 32;
                        if cursor >= offset + class_size {
                            None
                        } else {
                            Some(res)
                        }
                    }
                })?;
                if fault != 0 {
                    panic!(
                        "Corrupted block found in at offset {}: block is not marked as dirty but contains non-zero data \
                        at stack offset {}, indicating a potential use after free issue in the application code, a random 
                        corruption, or an error in cairnalloc logic",
                        offset, fault
                    );
                }
            }
        }
        Ok(())
    }

    /// Reads the head and next pointers of a free list
    fn alloc_list_read(&self, bin_index: u64) -> io::Result<(u64, u64)> {
        enum ReadState {
            Head,
            Current,
            Next,
            Done,
        }
        let buf = &mut [0u8; 8];
        let mut current_head = 0u64;
        let mut next_head = 0u64;
        let mut read_state = ReadState::Head;
        self.stack.get_batched_gen(|| {
            return match read_state {
                ReadState::Head => {
                    read_state = ReadState::Current;
                    // SAFETY: Slice `buf` lives for the duration of the get_batched_gen call
                    Some((bin_index, unsafe { escape_slice(buf) }))
                }
                ReadState::Current => {
                    current_head = u64::from_le_bytes(*buf);
                    if current_head == 0 {
                        read_state = ReadState::Done;
                        None
                    } else {
                        read_state = ReadState::Next;
                        // SAFETY: Slice `buf` lives for the duration of the get_batched_gen call
                        Some((current_head, unsafe { escape_slice(buf) }))
                    }
                }
                ReadState::Next => {
                    next_head = u64::from_le_bytes(*buf);
                    read_state = ReadState::Done;
                    if next_head == 0 {
                        None
                    } else {
                        // SAFETY: Slice `buf` lives for the duration of the get_batched_gen call
                        Some((next_head, unsafe { escape_slice(buf) }))
                    }
                }
                ReadState::Done => None,
            };
        })?;
        Ok((current_head, next_head))
    }

    /// Split a block from a mutex-protected unsorted list
    ///
    /// Call to this method must be protected by the corresponding unsorted list mutex, and the caller must ensure
    /// that the block at the given offset is valid and big enough for splitting. This method will attempt to split
    /// the block into an allocated block of the requested size and a remaining free block, and write the
    /// corresponding metadata to both blocks. If the remaining free block is big enough, it will also try to link
    /// it into the appropriate free list atomically or protected under a mutex.
    ///
    /// Providing incorrect class may lead to deadlock.
    ///
    /// Anticipate shared_buf to be zero in [32, 64) range
    fn split_block(
        &self,
        class: AllocClass,
        offset: u64,
        current_size: u64,
        requested_size: u64,
        shared_buf: &mut [u8],
    ) -> io::Result<()> {
        // First align requested size
        let aligned_size = align_32(requested_size);
        let free_block_size = current_size - aligned_size - 32;
        let free_class =
            AllocClass::free_from_size(free_block_size)
            .expect("Free block should be big enough for at least 32 bytes of data and 32 bytes of metadata");

        let fake_meta = // First write as if the entire block is in use
            &(Self::size_to_disk_size(current_size) | ALLOC_IN_USE_FLAG | ALLOC_DIRTY_FLAG)
            .to_le_bytes();
        let real_meta = // Then we will fix the metadata after we successfully claim the block
            &(Self::size_to_disk_size(aligned_size) | ALLOC_IN_USE_FLAG).to_le_bytes();
        let free_block_meta = &(if free_class.is_exact_size() {
            0u64 | ALLOC_IS_SORTED_FLAG
        } else {
            0u64
        })
        .to_le_bytes();
        let free_offset = offset + 16 + aligned_size;
        shared_buf[0..8].copy_from_slice(fake_meta);
        shared_buf[8..16].copy_from_slice(&current_size.to_le_bytes());
        // Do not override any pointer structures
        self.stack.set(offset - 16, &shared_buf[0..16])?;

        // Write block tail metadata and the entire outer block
        shared_buf[0..8].copy_from_slice(&ALLOC_MARKER_BE);
        shared_buf[8..16].copy_from_slice(real_meta);
        shared_buf[16..24].copy_from_slice(&free_offset.to_le_bytes());
        shared_buf[24..32].copy_from_slice(&[0u8; 8]);
        // shared_buf[32..48] should be zero
        shared_buf[48..52].copy_from_slice(free_block_meta);
        // shared_buf[52..64] should be zero to indicate no data in the free block
        shared_buf[64..72].copy_from_slice(&ALLOC_MARKER_BE);
        shared_buf[72..80].copy_from_slice(free_block_meta);
        self.stack.set(offset + aligned_size, &shared_buf[0..80])?;
        // TODO: The above section is incorrect

        // Write the correct metadata to the header
        shared_buf[0..8].copy_from_slice(real_meta);
        shared_buf[8..16].copy_from_slice(&requested_size.to_le_bytes());
        self.stack.set(offset - 16, &shared_buf[0..16])?;

        // Link the free block. Duplicating call to link in both branches because I'm
        // too lazy to manually drop mutex and the scope correctly protects it
        // the free_class == class here solely exists to avoid reentry of the mutex
        // which is not reentrant and will cause deadlock.
        if free_class == class || free_class.is_exact_size() {
            self.try_link_free_block_unchecked(free_class, free_offset)
        } else {
            let _guard = match class {
                AllocClass::MediumUnsorted => self.medium_unsorted_mutex.lock().unwrap(),
                AllocClass::LargeOrExtend => self.large_unsorted_mutex.lock().unwrap(),
                _ => unreachable!(),
            };
            self.try_link_free_block_unchecked(free_class, free_offset)
        }
    }
}

unsafe fn escape_slice<T>(slice: &mut [T]) -> &'static mut [T] {
    unsafe { core::mem::transmute(slice) }
}

impl BStackAllocator for CairnAlloc {
    type Error = io::Error;
    type Allocated<'a> = BStackSlice<'a, Self>;
    fn stack(&self) -> &BStack {
        &self.stack
    }

    fn into_stack(self) -> BStack {
        self.stack
    }

    fn alloc(&self, len: u64) -> Result<BStackSlice<'_, Self>, io::Error> {
        if len == 0 {
            return Ok(BStackSlice::empty(self));
        }
        let class = AllocClass::alloc_from_size(len);
        let bin_index = class.index_bin();
        let class_max_size = nonzero_into_u64(class.max_data_size());
        // Shared buffer for unknown purposes
        let shared_buf = &mut [0u8; 80];
        let ptr_read_buf: &mut [u8; 8] = &mut shared_buf[64..72].try_into().unwrap();
        let wptr_read_buf: &mut [u8; 48] = &mut shared_buf[48..80].try_into().unwrap();
        if class.is_exact_size() {
            let (current_head, next_head) = self.alloc_list_read(bin_index)?;
            // TODO: The CAS pattern may contain a ABA problem. For details, see algos/ATOMICLIST.md of bstack
            // https://raw.githubusercontent.com/williamwutq/bstack/refs/heads/master/algos/ATOMICLIST.md
            if current_head != 0
                && self
                    .stack
                    .cas(bin_index, current_head.to_le_bytes(), &ptr_read_buf)?
            {
                // Update next block's prev pointer
                if next_head != 0 {
                    self.stack.zero(next_head, 8)?;
                }
                // After CAS succeeds, we know that other threads cannot touch the block we already allocated
                // So now we can freely issue calls
                self.validate_free_block(class, current_head, shared_buf)?;

                // Write block head metadata: the first 8 bytes are the size with flags, the next 8 bytes are the used size
                // If this operation fails, the block is orphaned
                let meta = // The block is definitely sorted, not reallocable, and in use
                    &(Self::size_to_disk_size(len) | ALLOC_IS_SORTED_FLAG | ALLOC_IN_USE_FLAG)
                        .to_le_bytes();
                shared_buf[0..8].copy_from_slice(meta);
                shared_buf[8..16].copy_from_slice(&len.to_le_bytes());
                shared_buf[16..32].copy_from_slice(&[0u8; 16]);
                self.stack.set(current_head - 16, &shared_buf[0..32])?;

                // Write block tail metadata: the first 8 bytes are the be marker for allocation, the next 8 bytes are
                // mirrored identical metadata. Since header is always trusted over tail, if this operation fails, nothing
                // large will be affected and the block can be detected as corrupted and recovered later.
                shared_buf[0..8].copy_from_slice(&ALLOC_MARKER_BE);
                shared_buf[8..16].copy_from_slice(meta);
                self.stack
                    .set(current_head + class_max_size, &shared_buf[0..16])?;

                self.write_additional_deadbeef(current_head, len)?;

                // SAFETY: We have allocated this block with enough space
                return Ok(unsafe { BStackSlice::from_raw_parts(self, current_head, len) });
            }
            // Fall through to other pathes if the CAS fails, which means another thread allocated the block
            // we were trying to take. We can retry but the other paths reduce contention more
            if AllocClass::alloc_from_size(len + 64).is_small() {
                let larger_class = AllocClass::alloc_from_size(len + 64);
                let larger_class_max_size = nonzero_into_u64(larger_class.max_data_size());
                let second_attempt_bin_index = larger_class.index_bin();
                ptr_read_buf.copy_from_slice(&second_attempt_bin_index.to_le_bytes());
                let (current_head, next_head) = self.alloc_list_read(second_attempt_bin_index)?;
                // TODO: The CAS pattern may contain a ABA problem. See previous comment for details
                if current_head != 0
                    && self.stack.cas(
                        second_attempt_bin_index,
                        current_head.to_le_bytes(),
                        &ptr_read_buf,
                    )?
                {
                    // Update next block's prev pointer
                    if next_head != 0 {
                        self.stack.zero(next_head, 8)?;
                    }
                    self.validate_free_block(larger_class, current_head, shared_buf)?;

                    let fake_meta = // First write as if the entire block is in use
                        &(Self::size_to_disk_size(larger_class_max_size) | ALLOC_IS_SORTED_FLAG | ALLOC_IN_USE_FLAG | ALLOC_DIRTY_FLAG)
                        .to_le_bytes();
                    let real_meta = // Then we will fix the metadata after we successfully claim the block
                        &(Self::size_to_disk_size(len) | ALLOC_IS_SORTED_FLAG | ALLOC_IN_USE_FLAG).to_le_bytes();
                    let free_block_meta = // This is the metadata we will write to the remaining free block after we split
                        &(0u64 | ALLOC_IS_SORTED_FLAG).to_le_bytes();
                    let free_offset = current_head + 16 + class_max_size;
                    shared_buf[0..8].copy_from_slice(fake_meta);
                    shared_buf[8..16].copy_from_slice(&larger_class_max_size.to_le_bytes());
                    shared_buf[16..32].copy_from_slice(&[0u8; 16]);
                    self.stack.set(current_head - 16, &shared_buf[0..32])?;

                    // Write block tail metadata and the entire outer block
                    shared_buf[0..8].copy_from_slice(&ALLOC_MARKER_BE);
                    shared_buf[8..16].copy_from_slice(real_meta);
                    shared_buf[16..24].copy_from_slice(&free_offset.to_le_bytes());
                    // shared_buf[24..48] should be zero
                    shared_buf[48..52].copy_from_slice(free_block_meta);
                    // shared_buf[52..64] should be zero to indicate no data in the free block
                    shared_buf[64..72].copy_from_slice(&ALLOC_MARKER_BE);
                    shared_buf[72..80].copy_from_slice(free_block_meta);
                    self.stack
                        .set(current_head + class_max_size, &shared_buf[0..80])?;

                    // Write the correct metadata to the header
                    shared_buf[0..8].copy_from_slice(real_meta);
                    shared_buf[8..16].copy_from_slice(&len.to_le_bytes());
                    self.stack.set(current_head - 16, &shared_buf[0..16])?;

                    // Link the free block
                    self.try_link_free_block_unchecked(AllocClass::Small32, free_offset)?;

                    self.write_additional_deadbeef(current_head, len)?;

                    return Ok(unsafe { BStackSlice::from_raw_parts(self, current_head, len) });
                }
            }
            // Attempt to pull from medium unsorted bin, fall through
        }

        let mut read_blk_s = 0i8;
        let mut cursor = 0u64;
        let mut prev_head = 0u64;
        let mut current_head = 0u64;
        let mut next_head = 0u64;
        let actual_size = align_32(len + 32);
        if class == AllocClass::LargeOrExtend {
            // Tranversing the entire list and try to find a good fit
            // This is a first-fit algorithm since all large blocks feels similar
            let lum_guard = self.large_unsorted_mutex.lock().unwrap();
            self.stack.get_batched_gen(|| {
                if read_blk_s == 0 {
                    read_blk_s = 1;
                    // SAFETY: Slice `ptr_read_buf` lives for the duration of the get_batched_gen call
                    Some((bin_index, unsafe { escape_slice(ptr_read_buf) }))
                } else if cursor == 0 {
                    None
                } else if read_blk_s == 1 {
                    cursor = u64::from_le_bytes(*ptr_read_buf);
                    read_blk_s = 2;
                    // SAFETY: Slice `wptr_read_buf` lives for the duration of the get_batched_gen call
                    Some((cursor - 16, unsafe { escape_slice(wptr_read_buf) }))
                } else if wptr_read_buf[8..16] != [0u8; 8] {
                    // Should not happen
                    // Current error handling is to just ignore the block and continue searching
                    // because this is not a recovery logic here
                    Some((cursor - 16, unsafe { escape_slice(wptr_read_buf) }))
                } else {
                    current_head = cursor;
                    cursor = get_le!(wptr_read_buf[16..24]; u64);
                    let should_be_prev_head = get_le!(wptr_read_buf[24..32]; u64);
                    debug_assert_eq!(
                        should_be_prev_head, prev_head,
                        "Corrupted prev pointer in large unsorted list at offset {}: expected {}, found {}",
                        current_head, prev_head, should_be_prev_head
                    );
                    let block_meta = get_le!(wptr_read_buf[0..8]; u64);
                    let current_len = Self::disk_size_to_size(block_meta);
                    #[cfg(debug_assertions)]
                    if block_meta & ALLOC_IS_SORTED_FLAG != 0 {
                        // Something is not right, sorted blocks should not be in the unsorted list
                        panic!("Corrupted block found in large unsorted list at offset {}: sorted flag is set", cursor);
                    }
                    if current_len >= len && block_meta & ALLOC_IN_USE_FLAG == 0 {
                        next_head = cursor;
                        // We found a big enough block, quit reading more blocks
                        return None;
                    }
                    prev_head = current_head;
                    // SAFETY: see previous one
                    Some((cursor - 16, unsafe { escape_slice(wptr_read_buf) }))
                }
            })?;
            return if current_head != 0 {
                // Link prev to next and next to prev, effectively removing the block from the list
                self.stack.set(
                    if prev_head != 0 { prev_head } else { bin_index },
                    &next_head.to_le_bytes(),
                )?;
                if next_head != 0 {
                    self.stack.set(next_head + 8, &prev_head.to_le_bytes())?;
                }
                self.validate_free_block(class, current_head, shared_buf)?;
                let block_len = Self::disk_size_to_size(get_le!(wptr_read_buf[0..8]; u64));
                if block_len - len < 64 {
                    // This block is not worth splitting, we will just use the entire block
                    let meta = get_le!(wptr_read_buf[0..8]; u64) | ALLOC_IN_USE_FLAG;
                    shared_buf[0..8].copy_from_slice(&meta.to_le_bytes());
                    shared_buf[8..16].copy_from_slice(&len.to_le_bytes());
                    shared_buf[16..24].copy_from_slice(&[0u8; 8]);
                    self.stack.set(current_head - 16, &shared_buf[0..24])?;

                    shared_buf[0..8].copy_from_slice(&ALLOC_MARKER_BE);
                    shared_buf[8..16].copy_from_slice(&meta.to_le_bytes());
                    self.stack.set(
                        current_head + nonzero_into_u64(class.max_data_size()),
                        &shared_buf[0..16],
                    )?;
                } else {
                    self.split_block(class, current_head, block_len, len, shared_buf)?;
                }
                self.write_additional_deadbeef(current_head, len)?;
                Ok(unsafe { BStackSlice::from_raw_parts(self, current_head, len) })
                // drop(lum_guard) happens automatically here due to scope
            } else {
                drop(lum_guard);
                let ptr = self.stack.extend(actual_size)?; // 32 bytes for metadata
                let meta = // The block is not sorted, not reallocable, and in use
                    &(Self::size_to_disk_size(len) | ALLOC_IN_USE_FLAG).to_le_bytes();
                shared_buf[0..8].copy_from_slice(meta);
                shared_buf[8..16].copy_from_slice(&len.to_le_bytes());
                self.stack.set(ptr, &shared_buf[0..16])?;
                shared_buf[0..8].copy_from_slice(&ALLOC_MARKER_BE);
                shared_buf[8..16].copy_from_slice(meta);
                self.stack.set(ptr + actual_size - 16, &shared_buf[0..16])?;
                self.write_additional_deadbeef(ptr, len)?;
                // SAFETY: We have allocated this block with enough space
                Ok(unsafe { BStackSlice::from_raw_parts(self, ptr + 16, len) })
            };
        }

        current_head = AllocClass::MediumUnsorted.index_bin();
        let mut best_block_offset = 0u64;
        let mut prev_of_best_block = 0u64;
        let mut next_of_best_block = 0u64;
        let mut best_block_size = 18400u64; // The maximum size for medium unsorted class
        // Tranversing the entire list and try to find a good fit
        // This is a best-fit algorithm to reduce fragmentation in the medium range
        // Since generally the size of this linked list is not large, the contention should be acceptable
        let mum_guard = self.medium_unsorted_mutex.lock().unwrap();
        self.stack.get_batched_gen(|| {
            if read_blk_s == 0 {
                read_blk_s += 1;
                // SAFETY: Slice `ptr_read_buf` lives for the duration of the get_batched_gen call
                Some((bin_index, unsafe { escape_slice(ptr_read_buf) }))
            } else if cursor == 0 {
                None
            } else if read_blk_s == 1 {
                cursor = u64::from_le_bytes(*ptr_read_buf);
                read_blk_s += 1;
                // SAFETY: Slice `wptr_read_buf` lives for the duration of the get_batched_gen call
                Some((cursor - 16, unsafe { escape_slice(wptr_read_buf) }))
            } else {
                current_head = cursor;
                cursor = get_le!(wptr_read_buf[16..24]; u64);
                next_head = cursor;
                let should_be_prev_head = get_le!(wptr_read_buf[24..32]; u64);
                debug_assert_eq!(
                    should_be_prev_head, prev_head,
                    "Corrupted prev pointer in medium unsorted list at offset {}: expected {}, found {}",
                    current_head, prev_head, should_be_prev_head
                );
                let current_size = Self::disk_size_to_size(get_le!(wptr_read_buf[0..8]; u64));
                if current_size >= len {
                    let d = current_size - align_32(len);
                    if d < 16 || (d >= 64 && d <= 256) {
                        // This is a good enough block
                        best_block_offset = current_head;
                        best_block_size = current_size;
                        prev_of_best_block = prev_head;
                        next_of_best_block = next_head;
                        return None;
                    }
                    // We found a big enough block
                    if cursor != best_block_offset && current_size < best_block_size {
                        best_block_offset = current_head;
                        best_block_size = current_size;
                        prev_of_best_block = prev_head;
                        next_of_best_block = next_head;
                    }
                }
                prev_head = current_head;
                // SAFETY: see previous one
                Some((cursor - 16, unsafe { escape_slice(wptr_read_buf) }))
            }
        })?;

        if best_block_offset != 0 {
            // Link prev to next and next to prev, effectively removing the block from the list
            self.stack.set(
                if prev_of_best_block != 0 {
                    prev_of_best_block
                } else {
                    bin_index
                },
                &next_of_best_block.to_le_bytes(),
            )?;
            if next_of_best_block != 0 {
                self.stack
                    .set(next_of_best_block + 8, &prev_of_best_block.to_le_bytes())?;
            }
            // Block validation
            self.validate_free_block(AllocClass::MediumUnsorted, best_block_offset, shared_buf)?;
            let block_len = Self::disk_size_to_size(get_le!(wptr_read_buf[0..8]; u64));
            // We found something
            if block_len - len < 64 {
                // This block is not worth splitting, we will just use the entire block
                let meta = get_le!(wptr_read_buf[0..8]; u64) | ALLOC_IN_USE_FLAG;
                shared_buf[0..8].copy_from_slice(&meta.to_le_bytes());
                shared_buf[8..16].copy_from_slice(&len.to_le_bytes());
                shared_buf[16..24].copy_from_slice(&[0u8; 8]);
                self.stack.set(best_block_offset - 16, &shared_buf[0..24])?;

                shared_buf[0..8].copy_from_slice(&ALLOC_MARKER_BE);
                shared_buf[8..16].copy_from_slice(&meta.to_le_bytes());
                self.stack
                    .set(best_block_offset - 16 + block_len, &shared_buf[0..16])?;
            } else {
                self.split_block(
                    AllocClass::MediumUnsorted,
                    best_block_offset,
                    block_len,
                    len,
                    shared_buf,
                )?;
            }
            self.write_additional_deadbeef(best_block_offset, len)?;
            Ok(unsafe { BStackSlice::from_raw_parts(self, best_block_offset, len) })
            // drop(mum_guard) happens automatically here due to scope
        } else {
            drop(mum_guard);
            let ptr = self.stack.extend(actual_size)?; // 32 bytes for metadata
            let meta = // The block is not sorted, not reallocable, and in use
                    &(Self::size_to_disk_size(len) | ALLOC_IN_USE_FLAG).to_le_bytes();
            shared_buf[0..8].copy_from_slice(meta);
            shared_buf[8..16].copy_from_slice(&len.to_le_bytes());
            self.stack.set(ptr, &shared_buf[0..16])?;
            shared_buf[0..8].copy_from_slice(&ALLOC_MARKER_BE);
            shared_buf[8..16].copy_from_slice(meta);
            self.stack.set(ptr + actual_size - 16, &shared_buf[0..16])?;
            self.write_additional_deadbeef(ptr, len)?;
            // SAFETY: We have allocated this block with enough space
            return Ok(unsafe { BStackSlice::from_raw_parts(self, ptr + 16, len) });
        }
    }

    fn realloc<'a>(
        &'a self,
        slice: BStackSlice<'a, Self>,
        new_len: u64,
    ) -> io::Result<BStackSlice<'a, Self>> {
        if slice.is_empty() {
            return self.alloc(new_len);
        }
        if new_len == 0 {
            self.dealloc(slice)?;
            return Ok(BStackSlice::empty(self));
        }
        todo!()
    }

    fn dealloc(&self, slice: BStackSlice<'_, Self>) -> io::Result<()> {
        if slice.is_empty() {
            return Ok(());
        }
        // Shared buffer for unknown purposes
        let current_offset = slice.start();
        if current_offset < ALLOC_DATA_START + 16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Invalid deallocation: offset {} is out of bounds",
                    current_offset
                ),
            ));
        }
        // From this point on, current_offset - 32 is safe
        let shared_buf = &mut [0u8; 32];
        enum DeallocState {
            ReadHead,
            ReadTail,
            Validate,
            ValidateOnlyCurrentTail,
            ValidateNextBlockTail,
            ChecksOk,
            ErrInvalidDeallocationLen(u64),
            ErrDoubleFree,
            ErrBlockCorrupted(u64),
            ErrSuspectBufferOverflow(u64),
            ErrWrongDeadbeefInFreedBlock(u64),
            ErrWrongDeadbeefInUsedBlock(u64),
        }
        let state = &mut DeallocState::ReadHead;
        let mut current_block_meta = 0u64;
        let mut prev_block_meta = 0u64;
        let mut next_block_meta = 0u64;
        let stack_len = self.stack.len()?;
        // We read the head meta of the current block and the tail meta of the previous block first
        // Then, according to size data, we read the tail meta of the current block and the head meta of the next block
        self.stack.get_batched_gen(|| {
            loop {
                match state {
                    DeallocState::ReadHead => {
                        // SAFETY: Slice `shared_buf` lives for the duration of the get_batched_gen call
                        let res = (current_offset - 32, unsafe { escape_slice(shared_buf) });
                        *state = DeallocState::ReadTail;
                        return Some(res);
                    }
                    DeallocState::ReadTail => {
                        let prev_block_deadbeef: [u8; 8] = shared_buf[0..8].try_into().unwrap();
                        prev_block_meta = get_le!(shared_buf[8..16]; u64);
                        current_block_meta = get_le!(shared_buf[16..24]; u64);
                        let current_block_in_use_len = get_le!(shared_buf[24..32]; u64);
                        if current_block_in_use_len != slice.len() {
                            // Invalid deallocation
                            *state =
                                DeallocState::ErrInvalidDeallocationLen(current_block_in_use_len);
                            continue;
                        }
                        if current_offset - 16 > ALLOC_DATA_START {
                            // Not the first block
                            let prev_block_size = Self::disk_size_to_size(prev_block_meta);
                            let a = current_offset - 32; // This is unchecked because we have done basic checks
                            match a.checked_sub(prev_block_size) {
                                Some(prev_offset) if prev_offset < ALLOC_DATA_START => {
                                    if prev_block_deadbeef != ALLOC_MARKER_BE || prev_block_deadbeef != ALLOC_MARKER_LE {
                                        // It is highly likely that this is an application level buffer overflow issue
                                        *state = DeallocState::ErrSuspectBufferOverflow(prev_offset);
                                        continue;
                                    }
                                    let prev_block_in_use = prev_block_meta & ALLOC_IN_USE_FLAG != 0;
                                    if prev_block_in_use {
                                        if prev_block_deadbeef == ALLOC_MARKER_LE {
                                            *state = DeallocState::ErrWrongDeadbeefInUsedBlock(prev_offset);
                                            continue;
                                        }
                                    } else {
                                        if prev_block_deadbeef == ALLOC_MARKER_BE {
                                            *state =
                                            DeallocState::ErrWrongDeadbeefInFreedBlock(prev_offset);
                                            continue;
                                        }
                                    }
                                },
                                _ => {
                                    // Invalid previous block offset, do n ot use it
                                    prev_block_meta = 0;
                                }
                            }
                        } else {
                            // Do not use a previous block that does not exist
                            // Thus, previous block meta is empty
                            prev_block_meta = 0;
                        }
                        let current_block_size = Self::disk_size_to_size(current_block_meta);
                        if current_block_size < slice.len() {
                            // Invalid deallocation, the block is too small for the slice
                            *state = DeallocState::ErrBlockCorrupted(current_block_size);
                            continue;
                        }
                        if current_block_meta & ALLOC_IN_USE_FLAG == 0 {
                            // The block is already free, which is a double free issue in the application code
                            *state = DeallocState::ErrDoubleFree;
                            continue;
                        }
                        // current_block_size has a max of 2^63, so adding 32 will not cause overflow.
                        if let Some(current_tail_plus_32) = current_offset.checked_add(current_block_size + 32)
                            && current_tail_plus_32 <= stack_len
                        {
                            *state = DeallocState::Validate;
                            return Some((current_tail_plus_32 - 32, unsafe { escape_slice(shared_buf) }));
                        } else if let Some(current_tail_plus_16) = current_offset.checked_add(current_block_size + 16)
                            && current_tail_plus_16 <= stack_len
                        {
                            // We can at least read the tail meta of the current block, which is important for validation
                            *state = DeallocState::ValidateOnlyCurrentTail;
                            return Some((current_tail_plus_16 - 16, unsafe { escape_slice(&mut shared_buf[..16]) }));
                        } else {
                            // Either the block exceeds u64 or the size of the stack, both of which are invalid
                            *state = DeallocState::ErrBlockCorrupted(current_block_size);
                        }
                    }
                    DeallocState::ValidateOnlyCurrentTail => {
                        let current_block_deadbeef: [u8; 8] = shared_buf[0..8].try_into().unwrap();
                        let current_block_meta_tail = get_le!(shared_buf[8..16]; u64);
                        if current_block_deadbeef != ALLOC_MARKER_BE {
                            if current_block_deadbeef == ALLOC_MARKER_LE {
                                *state = DeallocState::ErrWrongDeadbeefInUsedBlock(current_offset);
                            } else {
                                *state = DeallocState::ErrSuspectBufferOverflow(current_offset);
                            }
                            continue;
                        }
                        #[cfg(debug_assertions)]
                        if current_block_meta_tail != current_block_meta {
                            panic!(
                                "Corrupted block found in at offset {}: header and tail metadata do not match",
                                current_offset
                            );
                        } // Otherwise trust the head
                        // We cannot read the next block, but we can still validate the current block and deallocate it
                        // At this point, next_block_meta is still zero, which is correct
                        *state = DeallocState::ChecksOk;
                        return None;
                    }
                    DeallocState::Validate => {
                        let current_block_deadbeef: [u8; 8] = shared_buf[0..8].try_into().unwrap();
                        let current_block_meta_tail = get_le!(shared_buf[8..16]; u64);
                        next_block_meta = get_le!(shared_buf[16..24]; u64);
                        if current_block_deadbeef != ALLOC_MARKER_BE {
                            if current_block_deadbeef == ALLOC_MARKER_LE {
                                *state = DeallocState::ErrWrongDeadbeefInUsedBlock(current_offset);
                            } else {
                                *state = DeallocState::ErrSuspectBufferOverflow(current_offset);
                            }
                            continue;
                        }
                        #[cfg(debug_assertions)]
                        if current_block_meta_tail != current_block_meta {
                            panic!(
                                "Corrupted block found in at offset {}: header and tail metadata do not match",
                                current_offset
                            );
                        } // Otherwise trust the head
                        // Validate that this size do not exceed length
                        let a = current_offset + Self::disk_size_to_size(current_block_meta);
                        // The above is unchecked because current_block_meta has not changed and we have added the same value before
                        // in ReadTail, which means adding offset and size to get tail will not overflow
                        let b = 32 + Self::disk_size_to_size(next_block_meta);
                        // The above is unchecked because next_block_size <= 2^63, which means adding 32 will not cause overflow
                        return match a.checked_add(b) {
                            Some(next_block_end) if next_block_end <= stack_len => {
                                *state = DeallocState::ValidateNextBlockTail;
                                Some((next_block_end, unsafe { escape_slice(&mut shared_buf[0..16]) }))
                            }
                            _ => {
                                // Invalid next block. We just don't use it
                                next_block_meta = 0;
                                None
                            }
                        };
                    }
                    DeallocState::ValidateNextBlockTail => {
                        let next_offset = current_offset + Self::disk_size_to_size(current_block_meta) + 32;
                        // The above is unchecked because current_block_meta has not changed and we have added the same value before,
                        // and to reach here we must have go down the path of Validate, which requires checked add with additional 32
                        // to also pass, which means adding offset and size to get next block tail will not overflow
                        let next_block_deadbeef: [u8; 8] = shared_buf[0..8].try_into().unwrap();
                        let next_block_meta_tail = get_le!(shared_buf[8..16]; u64);
                        if next_block_meta & ALLOC_IN_USE_FLAG != 0 {
                            if next_block_deadbeef == ALLOC_MARKER_LE {
                                *state = DeallocState::ErrWrongDeadbeefInUsedBlock(next_offset);
                                continue;
                            }
                        } else {
                            if next_block_deadbeef == ALLOC_MARKER_BE {
                                *state =
                                DeallocState::ErrWrongDeadbeefInFreedBlock(next_offset);
                                continue;
                            }
                        }
                        #[cfg(debug_assertions)]
                        if next_block_meta_tail != next_block_meta {
                            panic!(
                                "Corrupted block found in at offset {}: header and tail metadata do not match",
                                next_offset
                            );
                        } // Otherwise trust the head
                        *state = DeallocState::ChecksOk;
                        return None;
                    }
                    // The Error variants
                    _ => return None,
                }
            }
        })?;
        // Immediately check whether the previous routine was quit cleanly
        match state {
            DeallocState::ErrInvalidDeallocationLen(current_len) => {
                debug_panic_or_io_err!(
                    InvalidInput,
                    "Invalid deallocation at offset {}: block size {} does not match slice length {}, \
                    indicating a potential violation of the slice origin requirement of dealloc or a memory \
                    management issue in the application code",
                    current_offset,
                    current_len,
                    slice.len()
                );
            }
            DeallocState::ErrBlockCorrupted(current_size) => {
                debug_panic_or_io_err!(
                    InvalidData,
                    "Corrupted block found at offset {}: block size {} or slice length {} is not valid, which is \
                    potentially caused by random corruption or a bug in cairnalloc logic",
                    current_offset,
                    current_size,
                    slice.len()
                );
            }
            DeallocState::ErrDoubleFree => {
                debug_panic_or_io_err!(
                    InvalidInput,
                    "Double free detected at offset {}: indicating a double free issue in the application code",
                    current_offset
                );
            }
            DeallocState::ErrWrongDeadbeefInFreedBlock(offset) => {
                debug_panic_or_io_err!(
                    InvalidData,
                    "Corrupted block found at offset {}, indicating a buffer overflow issue and an use after free \
                    in the application code while using the block or random corruption",
                    offset
                );
            }
            DeallocState::ErrWrongDeadbeefInUsedBlock(offset) => {
                debug_panic_or_io_err!(
                    InvalidData,
                    "Corrupted block found at offset {}, indicating a buffer overflow issue, a double free in the \
                    application code, or a random corruption while using the block and almost certainly a bug in cairnalloc logic",
                    offset
                );
            }
            DeallocState::ErrSuspectBufferOverflow(offset) => {
                debug_panic_or_io_err!(
                    InvalidInput,
                    "Corrupted block found at offset {}, indicating a buffer overflow issue \
                    in the application code while using the block or random corruption",
                    offset
                );
            }
            _ => {}
        }
        // An in use block is not usable
        if prev_block_meta & ALLOC_IN_USE_FLAG != 0 {
            prev_block_meta = 0;
        }
        if next_block_meta & ALLOC_IN_USE_FLAG != 0 {
            next_block_meta = 0;
        }
        // We validate the tail deadbeef
        if !self.validate_additional_deadbeef(slice.start(), slice.len())? {
            debug_panic_or_io_err!(
                InvalidData,
                "Corrupted block found at offset {}, indicating a buffer overflow issue \
                in the application code while using the block or random corruption",
                current_offset
            );
        }

        // Compute sizes. Note that if the meta is zero, disk_size_to_size still works and returns zero, which is correct
        let current_block_size = Self::disk_size_to_size(current_block_meta);
        let current_block_end = current_offset + current_block_size;
        let prev_block_size = Self::disk_size_to_size(prev_block_meta);
        let next_block_size = Self::disk_size_to_size(next_block_meta);

        // If the checks are all good, we can proceed to deallocate the block and coalesce with neighbors if possible
        // First, we perform zeroing of the entire data section
        #[cfg(debug_assertions)]
        {
            // Validate that, start from len.next_multiple_of_8() + 8 to the end of the block, all bytes are zero
            let mut fault = 0u64;
            let cursor_start = slice.end().next_multiple_of(8) + 8;
            let mut cursor = cursor_start;
            self.stack.get_batched_gen(|| {
                // Check
                if cursor != cursor_start && shared_buf[..32] != [0u8; 32] {
                    fault = cursor + shared_buf.iter().position(|&b| b != 0).unwrap() as u64;
                    return None;
                }
                if cursor + 32 <= current_block_end {
                    // SAFETY: Slice `shared_buf` lives for the duration of the get_batched_gen call
                    let res = (cursor, unsafe { escape_slice(shared_buf) });
                    cursor += 32;
                    Some(res)
                } else {
                    let remaining = (current_block_end - cursor) as usize; // This is safe because remaining is smaller than 32
                    if remaining > 0 {
                        // SAFETY: Slice `shared_buf` lives for the duration of the get_batched_gen call,
                        // and we only read the valid remaining bytes
                        cursor = current_block_end;
                        Some((cursor, unsafe {
                            escape_slice(&mut shared_buf[..remaining])
                        }))
                    } else {
                        None
                    }
                }
            })?;
            if fault != 0 {
                panic!(
                    "Corrupted block found at offset {} since expecting zeroed bytes at stack offset {}, indicating \
                    a buffer overflow issue in the application code while using the block or random corruption",
                    current_offset, fault
                );
            }
        }
        // We avoid zeroing everything because the user should not have touched the memory after the end of the slice
        // This is an optimization and is backed by the fact that out of bounds writes should be undefined behavior
        // In fact, we cover to the next multiple of 8 plus 8 bytes to remove the deadbeef patterns, however,
        // we do not want to corrupt the deadbeef that is part of the tail metadata, so min is applied.
        #[cfg(not(debug_assertions))]
        self.stack.zero(
            slice.start(),
            (slice.len().next_multiple_of(8) + 8).min(current_block_end),
        )?;

        // Speculative unlinking, only for usable prev block that are in sorted list
        if prev_block_size != 0 && prev_block_meta & ALLOC_IS_SORTED_FLAG != 0 {
            // Read the prev and next of prev block
            self.stack
                .get_into(prev_block_size, &mut shared_buf[0..16])?;
            let next_of_prev_block = get_le!(shared_buf[0..8]; u64);
            let prev_of_prev_block = get_le!(shared_buf[8..16]; u64);
            self.stack.set(
                if prev_of_prev_block != 0 {
                    prev_of_prev_block
                } else {
                    AllocClass::alloc_from_size(prev_block_size).index_bin()
                },
                &next_of_prev_block.to_le_bytes(),
            )?;
            if next_of_prev_block != 0 {
                self.stack
                    .set(next_of_prev_block + 8, &prev_of_prev_block.to_le_bytes())?;
            }
        }
        // Speculative unlinking, only for usable next block that are in sorted list
        if next_block_size != 0 && next_block_meta & ALLOC_IS_SORTED_FLAG != 0 {
            // Read the prev and next of next block
            self.stack
                .get_into(prev_block_size, &mut shared_buf[0..16])?;
            let next_of_next_block = get_le!(shared_buf[0..8]; u64);
            let prev_of_next_block = get_le!(shared_buf[8..16]; u64);
            self.stack.set(
                if prev_of_next_block != 0 {
                    prev_of_next_block
                } else {
                    AllocClass::alloc_from_size(next_block_size).index_bin()
                },
                &next_of_next_block.to_le_bytes(),
            )?;
            if next_of_next_block != 0 {
                self.stack
                    .set(next_of_next_block + 8, &prev_of_next_block.to_le_bytes())?;
            }
        }
        // At this point, both next and free are unlinked if they are usable and in sorted list

        // For coalescing, we check size first, not meta
        if prev_block_size != 0 && next_block_size != 0 {
            // Try coalesce with both sides
        } else if prev_block_size != 0 {
            // Try coalesce with previous block
        } else if next_block_size != 0 {
            // Try coalesce with next block
        }

        todo!("The actual logic")
    }
}
