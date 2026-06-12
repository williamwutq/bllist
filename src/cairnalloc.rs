use bstack::{BStack, BStackAllocator, BStackSlice, FirstFitBStackAllocator};

use std::{
    io::{self, Write},
    num::NonZeroU64,
    sync::Mutex,
};

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
        let version = u32::from_le_bytes(buf[4..].try_into().unwrap());
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
            // TODO: This might be repairable
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
        let block_meta_head = u64::from_le_bytes(shared_buf[0..8].try_into().unwrap());
        if shared_buf[16..24] != ALLOC_MARKER_LE {
            if shared_buf[16..24] == ALLOC_MARKER_BE {
                #[cfg(debug_assertions)]
                panic!(
                    "Corrupted block found in at offset {}, indicating an error in cairnalloc realloc or dealloc logic",
                    offset
                );
            } else {
                // It is highly likely that this is an application level buffer overflow issue
                #[cfg(debug_assertions)]
                panic! {
                    "Corrupted block found in at offset {}, indicating a buffer overflow issue and an use after free \
                    in the application code while using the block or random corruption",
                    offset
                }
                #[cfg(not(debug_assertions))]
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Corrupted block found in at offset {}, indicating a buffer overflow issue and an use after free \
                        in the application code while using the block or random corruption",
                        current_head
                    ),
                ))?
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
            #[cfg(debug_assertions)]
            panic!(
                "Corrupted block found in at offset {}: in-use flag is not set or block marker is not zero",
                offset
            );
            #[cfg(not(debug_assertions))]
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Corrupted block found at offset {}: in-use flag is not set or block marker is not zero",
                    offset
                ),
            ))?
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
                            // The first 8 bytes may contain the pointer to the next free block,
                            // but the rest should be zero
                            if shared_buf[8..32] != [0u8; 24] {
                                fault = cursor
                                    + 8
                                    + shared_buf[8..32].iter().position(|&b| b != 0).unwrap()
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
        let wptr_read_buf: &mut [u8; 24] = &mut shared_buf[48..72].try_into().unwrap();
        let mut current_head = 0u64;
        if class.is_exact_size() {
            ptr_read_buf.copy_from_slice(&bin_index.to_le_bytes());
            let mut read_first_block = false;
            self.stack.get_batched_gen(|| {
                current_head = u64::from_le_bytes(*ptr_read_buf);
                if current_head == bin_index {
                    // SAFETY: Slice `ptr_read_buf` lives for the duration of the get_batched_gen call
                    Some((current_head, unsafe { escape_slice(ptr_read_buf) }))
                } else if current_head == 0 || read_first_block {
                    None
                } else {
                    read_first_block = true;
                    // SAFETY: Slice `ptr_read_buf` lives for the duration of the get_batched_gen call
                    Some((current_head, unsafe { escape_slice(ptr_read_buf) }))
                }
            })?;
            // TODO: The CAS pattern may contain a ABA problem. For details, see algos/ATOMICLIST.md of bstack
            // https://raw.githubusercontent.com/williamwutq/bstack/refs/heads/master/algos/ATOMICLIST.md
            if current_head != 0
                && self
                    .stack
                    .cas(bin_index, current_head.to_le_bytes(), &ptr_read_buf)?
            {
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
                shared_buf[16..24].copy_from_slice(&[0u8; 8]);
                self.stack.set(current_head - 16, &shared_buf[0..24])?;

                // Write block tail metadata: the first 8 bytes are the be marker for allocation, the next 8 bytes are
                // mirrored identical metadata. Since header is always trusted over tail, if this operation fails, nothing
                // large will be affected and the block can be detected as corrupted and recovered later.
                shared_buf[0..8].copy_from_slice(&ALLOC_MARKER_BE);
                shared_buf[8..16].copy_from_slice(meta);
                self.stack
                    .set(current_head + class_max_size, &shared_buf[0..16])?;

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
                let mut read_first_block = false;
                self.stack.get_batched_gen(|| {
                    current_head = u64::from_le_bytes(*ptr_read_buf);
                    if current_head == second_attempt_bin_index {
                        // SAFETY: Slice `ptr_read_buf` lives for the duration of the get_batched_gen call
                        Some((current_head, unsafe { escape_slice(ptr_read_buf) }))
                    } else if current_head == 0 || read_first_block {
                        None
                    } else {
                        read_first_block = true;
                        // SAFETY: Slice `ptr_read_buf` lives for the duration of the get_batched_gen call
                        Some((current_head, unsafe { escape_slice(ptr_read_buf) }))
                    }
                })?;
                // TODO: The CAS pattern may contain a ABA problem. See previous comment for details
                if current_head != 0
                    && self.stack.cas(
                        second_attempt_bin_index,
                        current_head.to_le_bytes(),
                        &ptr_read_buf,
                    )?
                {
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
                    shared_buf[16..24].copy_from_slice(&[0u8; 8]);
                    self.stack.set(current_head - 16, &shared_buf[0..24])?;

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
                    return Ok(unsafe { BStackSlice::from_raw_parts(self, current_head, len) });
                }
            }
            // Attempt to pull from medium unsorted bin, fall through
        }

        let mut read_blk_s = 0i8;
        let mut prev_addr = u64::MAX;
        let actual_size = align_32(len + 32);
        if class == AllocClass::LargeOrExtend {
            // Tranversing the entire list and try to find a good fit
            // This is a first-fit algorithm since all large blocks feels similar
            let lum_guard = self.large_unsorted_mutex.lock().unwrap();
            self.stack.get_batched_gen(|| {
                if read_blk_s == 0 {
                    read_blk_s += 1;
                    // SAFETY: Slice `ptr_read_buf` lives for the duration of the get_batched_gen call
                    Some((bin_index, unsafe { escape_slice(ptr_read_buf) }))
                } else if current_head == 0 {
                    None
                } else if read_blk_s == 1 {
                    current_head = u64::from_le_bytes(*ptr_read_buf);
                    read_blk_s += 1;
                    // SAFETY: Slice `wptr_read_buf` lives for the duration of the get_batched_gen call
                    Some((current_head - 16, unsafe { escape_slice(wptr_read_buf) }))
                } else if wptr_read_buf[8..16] != [0u8; 8] {
                    // Should not happen
                    // Current error handling is to just ignore the block and continue searching
                    // because this is not a recovery logic here
                    Some((current_head - 16, unsafe { escape_slice(wptr_read_buf) }))
                } else {
                    prev_addr = current_head;
                    current_head = u64::from_le_bytes(wptr_read_buf[16..24].try_into().unwrap());
                    let block_meta = u64::from_le_bytes(wptr_read_buf[0..8].try_into().unwrap());
                    let current_len = Self::disk_size_to_size(block_meta);
                    #[cfg(debug_assertions)]
                    if block_meta & ALLOC_IS_SORTED_FLAG != 0 {
                        // Something is not right, sorted blocks should not be in the unsorted list
                        panic!("Corrupted block found in large unsorted list at offset {}: sorted flag is set", current_head);
                    }
                    if current_len >= len && block_meta & ALLOC_IN_USE_FLAG == 0 {
                        // We found a big enough block, quit reading more blocks
                        return None;
                    }
                    // SAFETY: see previous one
                    Some((current_head - 16, unsafe { escape_slice(wptr_read_buf) }))
                }
            })?;
            return if current_head != 0 {
                self.validate_free_block(class, current_head, shared_buf)?;
                let block_len = Self::disk_size_to_size(u64::from_le_bytes(
                    wptr_read_buf[0..8].try_into().unwrap(),
                ));
                if block_len - len < 64 {
                    // This block is not worth splitting, we will just use the entire block
                    let meta = u64::from_le_bytes(wptr_read_buf[0..8].try_into().unwrap())
                        | ALLOC_IN_USE_FLAG;
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
                    todo!("TODO splitting")
                }
                Ok(unsafe { BStackSlice::from_raw_parts(self, current_head, len) })
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
                // SAFETY: We have allocated this block with enough space
                Ok(unsafe { BStackSlice::from_raw_parts(self, ptr + 16, len) })
            };
        }

        current_head = AllocClass::MediumUnsorted.index_bin();
        prev_addr = current_head;
        let mut best_block_offset = 0u64;
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
            } else if current_head == 0 {
                None
            } else if read_blk_s == 1 {
                current_head = u64::from_le_bytes(*ptr_read_buf);
                read_blk_s += 1;
                // SAFETY: Slice `wptr_read_buf` lives for the duration of the get_batched_gen call
                Some((current_head - 16, unsafe { escape_slice(wptr_read_buf) }))
            } else {
                current_head = u64::from_le_bytes(wptr_read_buf[16..24].try_into().unwrap());
                let current_size = Self::disk_size_to_size(u64::from_le_bytes(
                    wptr_read_buf[0..8].try_into().unwrap(),
                ));
                if current_size >= len {
                    let d = current_size - align_32(len);
                    if d < 16 || (d >= 64 && d <= 256) {
                        // This is a good enough block
                        best_block_offset = current_head;
                        best_block_size = current_size;
                        return None;
                    }
                    // We found a big enough block
                    if current_head != best_block_offset && current_size < best_block_size {
                        best_block_offset = current_head;
                        best_block_size = current_size;
                    }
                }
                // SAFETY: see previous one
                Some((current_head - 16, unsafe { escape_slice(wptr_read_buf) }))
            }
        })?;

        if current_head != 0 {
            // TODO Block validation
            // We found something
            todo!(
                "Medium allocation with best block at offset {} with size {}",
                best_block_offset,
                best_block_size
            )
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
        todo!()
    }
}
