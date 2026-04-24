# Changelog

All notable changes to `bllist` will be documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

- **`async` feature flag** — opt-in Tokio integration; add
  `bllist = { version = "0.2", features = ["async"] }` to enable.
- **`AsyncFixedBlockList<PAYLOAD_CAPACITY>`** — async wrapper around
  `FixedBlockList`.  Every method (`open`, `alloc`, `free`, `write`, `read`,
  `set_next`, `get_next`, `root`, `push_front`, `pop_front`) runs on Tokio's
  blocking-thread pool via `tokio::task::spawn_blocking`.  The type is `Clone`
  (cheap `Arc` increment) so multiple tasks can share one file handle without
  reopening it.
- **`AsyncDynamicBlockList`** — async wrapper around `DynamicBlockList` with
  the same design.  Adds async versions of `alloc(size)`, `free`, `write`,
  `read`, `set_next`, `get_next`, `root`, `capacity`, `data_len`, `data_end`,
  `push_front`, and `pop_front`.
- Data inputs accept `impl AsRef<[u8]> + Send + 'static` (e.g. `Vec<u8>`,
  `Box<[u8]>`, `&'static [u8]`) — no extra copy is made when owned data is
  provided.
- `inner()` method on both async types returns a `&`-reference to the
  underlying synchronous list, enabling direct BStack streaming reads from
  async contexts.
- 22 new unit tests (using `#[tokio::test]`) covering open, alloc/free, write/read,
  set/get_next, push/pop LIFO, mixed sizes, clone sharing, and persistence
  across reopen — for both async wrapper types.

### Dependencies

- [`tokio`](https://crates.io/crates/tokio) `1` with `features = ["rt"]`
  (optional; only compiled when `features = ["async"]` is set).

---

## [0.2.0] - 2026-04-24

### Changed

- **`DynamicBlockList` on-disk format bumped to version 2** (breaking — existing
  `.blld` version-1 files are rejected with `Error::Corruption` on open).
  The `capacity` field in every block header is replaced by `block_size`: the
  **total** on-disk size of the block (header + payload), always a power of two
  and at least 32 bytes.  The payload capacity is therefore `block_size − 20`
  rather than the value stored verbatim.
- **Bin semantics changed**: bin *k* now holds free blocks whose total on-disk
  size equals 2^*k*.  Bins 0–4 are never populated; the minimum usable bin is
  5 (32-byte blocks, 12-byte payload).  Previously bin *k* held blocks whose
  *payload capacity* equalled 2^*k*.
- **`capacity_for(size)` replaced by `block_size_for(size)`**: returns the
  smallest power-of-two total block size ≥ `size + 20`, with a minimum of 32.
- **`capacity(block)` semantics unchanged** (returns payload capacity in bytes)
  but internally computes `block_size − 20` instead of reading a stored value.

### Added

- **`MIN_BIN`** (`= 5`) — public constant for the smallest usable bin index.
- **`MAX_SPLIT`** (`= 3`) — public constant controlling the maximum number of
  bin levels searched above the target before the file is extended.
- **Splitting on allocation**: when the target bin is empty, `alloc` searches
  bins *k+1* through *k+`MAX_SPLIT`* for a free block.  If one is found at
  bin *m*, it is split by halving repeatedly — the upper half is placed in
  bin *m−1*, *m−2*, … until the lower half reaches the target bin.  The lower
  half's `block_size` field is updated first on each split step so the
  sequential scan remains consistent if a crash occurs mid-split.
- **Coalescing on open** (`recover_orphans`): after collecting all non-active
  blocks via sequential scan, adjacent free blocks whose combined size is a
  power of two are merged into a single block (one pass).  This handles runs of
  any length — e.g. three adjacent blocks of 256 + 512 + 256 = 1024 bytes merge
  into a single bin-10 block.  All bin free-lists are rebuilt from scratch using
  a two-phase header write (zero all bin heads first, then populate) so a crash
  mid-coalesce leaves only orphans that are safely reclaimed on the next open.
- **`DynBlockRef::data_start(self) -> u64`** — pure, infallible method on the
  ref itself; returns the logical byte offset of the first payload byte
  (`self.0 + 20`).  No file access or validation.
- **`DynamicBlockList::data_start(block) -> Result<u64, Error>`** — validates
  the block offset then returns the same value as `block.data_start()`.
  Consistent with the rest of the metadata API (`capacity`, `data_len`).
- **`DynamicBlockList::data_end(block) -> Result<u64, Error>`** — validates the
  offset, reads `data_len` from the file, and returns
  `block.data_start() + data_len`.  Equals `data_start` for an empty block.

---

## [0.1.0] - 2026-04-21

### Added

- `DynamicBlockList` — crash-safe singly-linked list of **variable-size** blocks
  backed by a single BStack file.
- `DynBlockRef(u64)` — `Copy` handle encoding a dynamic block's logical byte offset.
- **Block layout** (dynamic): 4-byte CRC32 + 8-byte next pointer + 4-byte capacity
  + 4-byte data_len + payload of `capacity` bytes; CRC covers
  `next + capacity + data_len + full payload`.
- **File layout** (dynamic): valid BStack file; bllist-dynamic header (272 bytes:
  `"BLLD"` magic, version `u32`, root `u64`, 32 × bin-head `u64`) at logical
  offset 0, followed by variably-sized blocks packed contiguously.
- **Bin allocator**: 32 power-of-two bins (bin *k* → capacity 2^k, covering 1 to
  2^31 bytes). `alloc(size)` rounds `size` up to the next power of two and serves
  from the matching bin; freed blocks return to their bin.
- **Crash recovery** on `open` (dynamic): sequential scan using each block's
  `capacity` field to step through the file; orphaned blocks are reclaimed into
  the appropriate bin.
- **Cross-type protection**: `DynamicBlockList::open` rejects `"BLLS"` files;
  `FixedBlockList::open` rejects `"BLLD"` files.
- **Public API** (`DynamicBlockList`):
  - `open(path)` — open or create; validates header; performs crash recovery
  - `alloc(size)` — pop from matching bin or grow file; capacity = next power of 2
  - `free(block)` — zero, set `data_len = 0`, link into bin
  - `write(block, data)` — write payload, set `data_len`, recompute CRC
  - `read(block)` → `Vec<u8>` — read exactly `data_len` bytes, CRC-verify
  - `read_into(block, buf)` — zero-copy variant; buf must be ≥ `data_len`
  - `set_next(block, next)` — update next pointer, preserve payload
  - `get_next(block)` → `Option<DynBlockRef>` — fast structural traversal (no CRC)
  - `root()` → `Option<DynBlockRef>` — current head of the active list
  - `capacity(block)` → `usize` — allocated payload capacity (power of two)
  - `data_len(block)` → `usize` — bytes written by last `write`
  - `push_front(data)` — alloc + write + link as new root
  - `pop_front()` → `Option<Vec<u8>>` — unlink head + read + free
  - `pop_front_into(buf)` — zero-copy variant of `pop_front`
  - `capacity_for(size)` — `const fn`: next power of two ≥ `size` (minimum 1)
- 25 unit tests for `DynamicBlockList`.
- `FixedBlockList<const PAYLOAD_CAPACITY: usize>` — crash-safe singly-linked
  list of fixed-size blocks backed by a single BStack file.
- `BlockRef(u64)` — `Copy` handle encoding a block's logical byte offset.
- **Block layout**: 4-byte CRC32 checksum + 8-byte next pointer + payload field
  of `PAYLOAD_CAPACITY` bytes; checksum covers `next + full payload`.
- **File layout**: valid BStack file; bllist header (24 bytes: `"BLLS"` magic,
  version `u32`, root `u64`, free-list-head `u64`) at logical offset 0, followed
  by contiguously-packed blocks starting at offset 24.
- **Crash recovery** on `open`: orphaned blocks (allocated but unreachable from
  active or free list) are detected by exhaustive slot enumeration and silently
  reclaimed into the free list.
- **Public API**:
  - `open(path)` — open or create; validates header; performs crash recovery
  - `alloc()` — pop from free list or grow file
  - `free(block)` — zero and link into free list
  - `write(block, data)` — write payload, preserve next, recompute CRC
  - `read(block)` → `Vec<u8>` — read and CRC-verify payload
  - `read_into(block, buf)` — zero-copy variant; reads directly into caller buffer
  - `set_next(block, next)` — update next pointer, preserve payload
  - `get_next(block)` → `Option<BlockRef>` — fast structural traversal (no CRC)
  - `root()` → `Option<BlockRef>` — current head of the active list
  - `push_front(data)` — alloc + write + link as new root
  - `pop_front()` → `Option<Vec<u8>>` — unlink head + read + free
  - `pop_front_into(buf)` — zero-copy variant of `pop_front`
  - `payload_capacity()` — `PAYLOAD_CAPACITY`
- **Error type** (`Error` enum): `Io`, `ChecksumMismatch`, `Corruption`,
  `InvalidBlock`, `BlockTooSmall`, `DataTooLarge`.
- Compile-time rejection of `PAYLOAD_CAPACITY = 0` via `const` assertion.
- `FixedBlockList` is `Send + Sync`; header mutations are serialised through an
  internal `Mutex<()>`; block-only operations (`write`, `read`, `set_next`, …)
  do not acquire the mutex.
- 20 unit tests and 2 doc tests.


### Dependencies

- [`bstack`](https://crates.io/crates/bstack) `>=0.1.3` with `features = ["set"]`
- [`crc32fast`](https://crates.io/crates/crc32fast) `1.5`

