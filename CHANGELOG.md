# Changelog

All notable changes to `bllist` will be documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

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
  - `payload_capacity()` / `PAYLOAD_CAPACITY` — `BLOCK_SIZE − 12`
- **Error type** (`Error` enum): `Io`, `ChecksumMismatch`, `Corruption`,
  `InvalidBlock`, `BlockTooSmall`, `DataTooLarge`.
- Compile-time rejection of `BLOCK_SIZE ≤ 12` via `const` assertion.
- `FixedBlockList` is `Send + Sync`; header mutations are serialised through an
  internal `Mutex<()>`; block-only operations (`write`, `read`, `set_next`, …)
  do not acquire the mutex.
- 20 unit tests and 2 doc tests.

### Dependencies

- [`bstack`](https://crates.io/crates/bstack) `>=0.1.3` with `features = ["set"]`
- [`crc32fast`](https://crates.io/crates/crc32fast) `1.5`

[Unreleased]: https://github.com/williamwutq/bllist/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/williamwutq/bllist/releases/tag/v0.1.0
