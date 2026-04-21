# bllist

Durable, crash-safe, checksummed block-based linked list allocators stored in a single file.

`bllist` builds on [`bstack`](https://crates.io/crates/bstack) to provide persistent linked lists backed by fixed-size or variable-size blocks. Every block carries a CRC32 checksum; writes flush durably to disk before returning; and the file survives unclean shutdowns through automatic orphan recovery on the next open.

[![Crates.io](https://img.shields.io/crates/v/bllist)](https://crates.io/crates/bllist)
[![Docs.rs](https://img.shields.io/docsrs/bllist)](https://docs.rs/bllist)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

---

## Features

- **Two allocator types** — `FixedBlockList` for uniform records, `DynamicBlockList` for variable-size records with bin-based reuse
- **CRC32 integrity** — every block is checksummed; corruption is detected on read
- **Crash safety** — all mutations are durable before returning; orphaned blocks are recovered on the next `open`
- **Bin-based free list** — freed blocks are returned to the power-of-two bin matching their capacity and reused immediately
- **Zero-copy reads** — `read_into` and `pop_front_into` fill a caller-supplied buffer directly from the file
- **Thread-safe** — `Send + Sync`; concurrent reads are efficient via `pread` on Unix/Windows
- **Valid BStack files** — both list types produce valid `bstack` files; the BStack header and crash-recovery semantics are inherited for free
- **Cross-type protection** — `FixedBlockList` and `DynamicBlockList` use different file magics (`"BLLS"` vs `"BLLD"`) and cannot open each other's files

---

## Quick start

Add to `Cargo.toml`:

```toml
[dependencies]
bllist = "0.1"
```

### Fixed-size blocks

```rust
use bllist::FixedBlockList;

fn main() -> Result<(), bllist::Error> {
    // 52 bytes of payload per block (64 bytes total on disk).
    let list = FixedBlockList::<52>::open("data.blls")?;

    list.push_front(b"hello")?;
    list.push_front(b"world")?;

    while let Some(data) = list.pop_front()? {
        println!("{}", String::from_utf8_lossy(&data));
    }
    // prints "world", then "hello"

    Ok(())
}
```

### Variable-size blocks

```rust
use bllist::DynamicBlockList;

fn main() -> Result<(), bllist::Error> {
    // Payload capacity is rounded up to the next power of two per block.
    let list = DynamicBlockList::open("data.blld")?;

    list.push_front(b"short")?;
    list.push_front(b"a somewhat longer record")?;

    while let Some(data) = list.pop_front()? {
        println!("{}", String::from_utf8_lossy(&data));
    }

    Ok(())
}
```

---

## API overview

### `FixedBlockList<PAYLOAD_CAPACITY>`

`PAYLOAD_CAPACITY` is the number of **payload bytes** per block. Each block occupies `PAYLOAD_CAPACITY + 12` bytes on disk. `PAYLOAD_CAPACITY` must be `> 0`; a value of `0` is rejected at compile time.

| Method | Description |
|--------|-------------|
| `open(path)` | Open or create; performs crash recovery |
| `push_front(data)` | Allocate, write, and prepend to the list |
| `pop_front()` → `Option<Vec<u8>>` | Unlink head, read payload, free block |
| `pop_front_into(buf)` → `bool` | Zero-copy pop into caller buffer |
| `alloc()` → `BlockRef` | Allocate a raw block (from free list or new) |
| `free(block)` | Return a block to the free list |
| `write(block, data)` | Write payload, preserve next pointer |
| `read(block)` → `Vec<u8>` | Read and checksum-verify payload |
| `read_into(block, buf)` | Zero-copy read into caller buffer |
| `set_next(block, next)` | Update next pointer, preserve payload |
| `get_next(block)` → `Option<BlockRef>` | Read next pointer (no CRC check) |
| `root()` → `Option<BlockRef>` | Current head of the active list |
| `payload_capacity()` | `PAYLOAD_CAPACITY` |

### `BlockRef`

A `Copy` handle encoding a block's logical byte offset in the BStack file. Treat it like a typed index; never forge offsets manually.

### `DynamicBlockList`

Blocks may hold any payload up to 2^31 bytes. Each allocation is rounded up to the next power of two (minimum 1) and served from one of 32 power-of-two bins. Freed blocks return to their bin and are reused by the next allocation of the same size.

| Method | Description |
|--------|-------------|
| `open(path)` | Open or create; performs crash recovery |
| `push_front(data)` | Allocate, write, and prepend to the list |
| `pop_front()` → `Option<Vec<u8>>` | Unlink head, read payload, free block |
| `pop_front_into(buf)` → `bool` | Zero-copy pop into caller buffer |
| `alloc(size)` → `DynBlockRef` | Allocate a block with capacity ≥ `size` |
| `free(block)` | Return a block to its bin |
| `write(block, data)` | Write payload, update `data_len` |
| `read(block)` → `Vec<u8>` | Read `data_len` bytes, checksum-verify |
| `read_into(block, buf)` | Zero-copy read into caller buffer |
| `set_next(block, next)` | Update next pointer, preserve payload |
| `get_next(block)` → `Option<DynBlockRef>` | Read next pointer (no CRC check) |
| `root()` → `Option<DynBlockRef>` | Current head of the active list |
| `capacity(block)` → `usize` | Allocated payload capacity (power of two) |
| `data_len(block)` → `usize` | Bytes last written to this block |
| `capacity_for(size)` → `usize` | Next power of two ≥ `size` |

### `DynBlockRef`

A `Copy` handle encoding a dynamic block's logical byte offset. Analogous to `BlockRef` but for `DynamicBlockList`.

---

## File layouts

### `FixedBlockList` files (`"BLLS"`)

```
┌──────────────────────────┬───────────────────────────────────────────┐
│  BStack header (16 B)    │  bllist header (24 B, logical offset 0)   │
│  "BSTK" magic + clen     │  "BLLS" + version + root + free_head      │
├──────────────────────────┴───────────────────────────────────────────┤
│  Block 0  (PAYLOAD_CAPACITY+12 bytes, logical offset 24)             │
│  checksum(4) │ next(8) │ payload(PAYLOAD_CAPACITY)                   │
├──────────────────────────────────────────────────────────────────────┤
│  Block 1  (PAYLOAD_CAPACITY+12 bytes, logical offset 24+PC+12)  …    │
└──────────────────────────────────────────────────────────────────────┘
```

- The **bllist header** stores the root block offset and single free-list-head offset.
- The **block checksum** covers bytes `[4..PAYLOAD_CAPACITY+12]` (next pointer + full payload field). Payload bytes beyond the last `write` are guaranteed to be zero.
- The **free list** is an embedded singly-linked list using the `next` field of freed blocks.

### `DynamicBlockList` files (`"BLLD"`)

```
┌──────────────────────────┬───────────────────────────────────────────────┐
│  BStack header (16 B)    │  bllist-dynamic header (272 B, logical off 0) │
│  "BSTK" magic + clen     │  "BLLD" + version + root + bin_heads[32]      │
├──────────────────────────┴───────────────────────────────────────────────┤
│  Block (variable size)                                                    │
│  checksum(4) │ next(8) │ capacity(4) │ data_len(4) │ payload(capacity B) │
├──────────────────────────────────────────────────────────────────────────┤
│  Block …                                                                  │
└──────────────────────────────────────────────────────────────────────────┘
```

- The **bllist-dynamic header** stores the root offset and 32 bin free-list heads (one per power-of-two capacity 2^0–2^31).
- The **block checksum** covers bytes `[4..20+capacity]` (next + capacity + data_len + full payload field).
- `data_len` records how many bytes were written; bytes beyond it are guaranteed to be zero.

---

## Crash safety details

`bllist` is designed around two principles:

1. **Durable writes** — every `stack.set()` / `stack.push()` call issues `fsync` (or `F_FULLFSYNC` on macOS) before returning.
2. **CRC-detected partial writes** — the checksum over the block header and payload detects any block that was partially overwritten before a crash.

On `open`, the file is scanned for *orphaned* blocks (allocated but not reachable from either the active list or any free list). They are silently reclaimed.

| Crash point | Effect | Recovery |
|---|---|---|
| During `alloc` (file grow) | Block exists but is in no list | Reclaimed as orphan on next `open` |
| After `alloc`, before `push_front` links it | Block written but root not updated | Reclaimed as orphan on next `open` |
| After `pop_front` advances root, before `free` | Block exists but in no list | Reclaimed as orphan on next `open` |

No data that was fully committed (root updated) is ever lost.

---

## Choosing the right type

| | `FixedBlockList` | `DynamicBlockList` |
|---|---|---|
| Record size | Always the same | Varies |
| On-disk overhead per block | 12 bytes | 20 bytes |
| Free list | Single flat list | 32 power-of-two bins |
| Orphan scan | O(n) slot enumeration | O(n) sequential scan |
| File magic | `"BLLS"` | `"BLLD"` |

### Choosing `PAYLOAD_CAPACITY` for `FixedBlockList`

- Minimum: `1`
- For small records: `52` (64 bytes on disk), `116` (128 bytes on disk)
- For larger records: set to your typical payload size directly
- `PAYLOAD_CAPACITY = 0` is rejected at compile time

---

## Direct file access — use with extreme caution

Both list types produce valid BStack files, so you can open them with
`bstack::BStack::open` or inspect the raw bytes with any file tool.
**Writing to the file outside of `bllist` is strongly discouraged.**
`bllist` does not re-validate structural invariants on every operation, so
direct writes can silently corrupt the list in ways that are not caught until
much later — or not caught at all.

Specific dangers:

| Operation                          | Risk |
|------------------------------------|------|
| `BStack::push`                     | Appends raw bytes that are not a complete, aligned block; breaks slot enumeration and orphan recovery |
| `BStack::pop`                      | May truncate a block mid-stream or destroy the list header |
| `BStack::set` at header offsets    | Overwrites root or free-list / bin-head pointers |
| `BStack::set` inside a block       | Invalidates the block's CRC; `read` will return a checksum error |
| Raw file writes (`write(2)`, etc.) | Bypasses the advisory lock entirely; any of the above, plus potential torn writes |

**The exclusive advisory lock** (`flock` on Unix, `LockFileEx` on Windows)
held by a live list prevents a second process from opening the same file
through BStack simultaneously. It does **not** prevent raw file
descriptor access, so a process that opens the file without going through
BStack can bypass the lock and cause corruption.

**Safe read-only inspection** is possible: open the file with
`bstack::BStack::open` and use only `get`, `peek`, and `len`. These calls do
not write to the file and will not disturb the list state. Mutating calls
(`push`, `pop`, `set`) must not be used.
