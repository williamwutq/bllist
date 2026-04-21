# Changelog

All notable changes to `bllist` will be documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

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

### Changed

- **Breaking** — `FixedBlockList<const BLOCK_SIZE: usize>` renamed to
  `FixedBlockList<const PAYLOAD_CAPACITY: usize>`. The const generic now
  expresses the number of **payload bytes** per block directly, rather than
  the total on-disk block size. Callers must update their type instantiations:
  `FixedBlockList::<64>` → `FixedBlockList::<52>` (for the same 64-byte
  on-disk block). On-disk format is unchanged.
- `PAYLOAD_CAPACITY` associated const removed; the const generic itself is
  the payload capacity. `payload_capacity()` still works.
- Compile-time assertion updated: `PAYLOAD_CAPACITY > 0` (was `BLOCK_SIZE > 12`).

---

## [0.1.0] — 2026-04-20

### Added

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

[Unreleased]: https://github.com/williamwutq/bllist/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/williamwutq/bllist/releases/tag/v0.1.0
