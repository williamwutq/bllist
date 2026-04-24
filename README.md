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
- **Power-of-two block sizes** — dynamic blocks have a total on-disk footprint (header + payload) that is always a power of two, enabling splitting and coalescing
- **Splitting** — when a free block is larger than needed (within `MAX_SPLIT` = 3 levels), it is halved and the spare half returned to its bin, reducing wasted space
- **Coalescing on open** — adjacent free blocks whose combined size is a power of two are merged into a single larger block, fighting long-term fragmentation
- **Zero-copy reads** — `read_into` and `pop_front_into` fill a caller-supplied buffer directly from the file
- **Async I/O** — `AsyncFixedBlockList` and `AsyncDynamicBlockList` wrappers offload every blocking call to Tokio's thread pool; enable with `features = ["async"]`
- **Thread-safe** — `Send + Sync`; concurrent reads are efficient via `pread` on Unix/Windows
- **Valid BStack files** — both list types produce valid `bstack` files; the BStack header and crash-recovery semantics are inherited for free
- **Cross-type protection** — `FixedBlockList` and `DynamicBlockList` use different file magics (`"BLLS"` vs `"BLLD"`) and cannot open each other's files

---

## Quick start

Add to `Cargo.toml`:

```toml
[dependencies]
bllist = "0.2"

# Enable async wrappers (requires a Tokio runtime):
# bllist = { version = "0.2", features = ["async"] }
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
    // The total on-disk block size (header + payload) is a power of two.
    // A 5-byte push occupies 32 bytes on disk (5+20=25 → 32, bin 5).
    let list = DynamicBlockList::open("data.blld")?;

    list.push_front(b"short")?;
    list.push_front(b"a somewhat longer record")?;

    while let Some(data) = list.pop_front()? {
        println!("{}", String::from_utf8_lossy(&data));
    }

    Ok(())
}
```

### Async (Tokio)

Enable the `async` feature and use `AsyncFixedBlockList` / `AsyncDynamicBlockList`:

```rust
use bllist::AsyncDynamicBlockList;

#[tokio::main]
async fn main() -> Result<(), bllist::Error> {
    let list = AsyncDynamicBlockList::open("data.blld").await?;

    list.push_front(b"short record").await?;
    list.push_front(b"a somewhat longer record").await?;

    while let Some(data) = list.pop_front().await? {
        println!("{}", String::from_utf8_lossy(&data));
    }

    Ok(())
}
```

The wrappers are `Clone` (cheap `Arc` increment) and share the same underlying
file handle, so you can hand copies to multiple tasks without reopening the file.

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

Blocks may hold any payload up to 2^31 − 20 bytes.  The **total on-disk footprint** of a block (20-byte header + payload) is always a power of two, with a minimum of 32 bytes (bin 5, 12-byte payload).  Allocation first checks the exact power-of-two bin, then searches up to `MAX_SPLIT` = 3 bins higher and splits if found, and finally extends the file.  On open, adjacent free blocks whose combined size is a power of two are coalesced.

| Method | Description |
|--------|-------------|
| `open(path)` | Open or create; validates header; coalesces free blocks and recovers orphans |
| `push_front(data)` | Allocate, write, and prepend to the list |
| `pop_front()` → `Option<Vec<u8>>` | Unlink head, read payload, free block |
| `pop_front_into(buf)` → `bool` | Zero-copy pop into caller buffer |
| `alloc(size)` → `DynBlockRef` | Allocate block; splits from larger bin or extends file |
| `free(block)` | Return a block to its bin |
| `write(block, data)` | Write payload, update `data_len` |
| `read(block)` → `Vec<u8>` | Read `data_len` bytes, checksum-verify |
| `read_into(block, buf)` | Zero-copy read into caller buffer |
| `set_next(block, next)` | Update next pointer, preserve payload |
| `get_next(block)` → `Option<DynBlockRef>` | Read next pointer (no CRC check) |
| `root()` → `Option<DynBlockRef>` | Current head of the active list |
| `capacity(block)` → `usize` | Payload capacity = `block_size − 20` |
| `data_len(block)` → `usize` | Bytes last written to this block |
| `data_start(block)` → `u64` | Logical offset of first payload byte (validates offset) |
| `data_end(block)` → `u64` | Logical offset past the last written byte (reads `data_len`) |
| `bstack()` → `&BStack` | Underlying file handle for raw read-only streaming |
| `block_size_for(size)` → `usize` | Smallest power-of-two total size ≥ `size + 20` (min 32) |

### `DynBlockRef`

A `Copy` handle encoding a dynamic block's logical byte offset. Analogous to `BlockRef` but for `DynamicBlockList`.

| Method | Description |
|--------|-------------|
| `data_start()` → `u64` | Logical offset of the first payload byte (`self.0 + 20`); pure, no I/O |

### `AsyncFixedBlockList<PAYLOAD_CAPACITY>` *(feature `async`)*

An async, `Clone`-able wrapper around `FixedBlockList`. Each method runs on
Tokio's blocking-thread pool via `spawn_blocking`.  Data inputs accept any
`impl AsRef<[u8]> + Send + 'static` (e.g. `Vec<u8>`, `Box<[u8]>`, `&'static [u8]`).

| Method | Description |
|--------|-------------|
| `open(path).await` | Open or create on a blocking thread |
| `push_front(data).await` | Allocate, write, prepend to list |
| `pop_front().await` → `Option<Vec<u8>>` | Unlink head, read payload, free block |
| `alloc().await` → `BlockRef` | Allocate a raw block |
| `free(block).await` | Return block to free list |
| `write(block, data).await` | Write payload, preserve next pointer |
| `read(block).await` → `Vec<u8>` | Read and checksum-verify payload |
| `set_next(block, next).await` | Update next pointer |
| `get_next(block).await` → `Option<BlockRef>` | Read next pointer (no CRC check) |
| `root().await` → `Option<BlockRef>` | Current head of the active list |
| `payload_capacity()` | `PAYLOAD_CAPACITY` (no I/O) |
| `inner()` → `&FixedBlockList<N>` | Underlying sync handle for streaming reads |

### `AsyncDynamicBlockList` *(feature `async`)*

An async, `Clone`-able wrapper around `DynamicBlockList`. Same `spawn_blocking`
approach as `AsyncFixedBlockList`.

| Method | Description |
|--------|-------------|
| `open(path).await` | Open or create on a blocking thread |
| `push_front(data).await` | Allocate, write, prepend to list |
| `pop_front().await` → `Option<Vec<u8>>` | Unlink head, read payload, free block |
| `alloc(size).await` → `DynBlockRef` | Allocate block; splits or extends file |
| `free(block).await` | Return block to its bin |
| `write(block, data).await` | Write payload, update `data_len` |
| `read(block).await` → `Vec<u8>` | Read `data_len` bytes, checksum-verify |
| `set_next(block, next).await` | Update next pointer |
| `get_next(block).await` → `Option<DynBlockRef>` | Read next pointer (no CRC check) |
| `root().await` → `Option<DynBlockRef>` | Current head of the active list |
| `capacity(block).await` → `usize` | Payload capacity = `block_size − 20` |
| `data_len(block).await` → `usize` | Bytes last written to this block |
| `data_end(block).await` → `u64` | Logical offset past the last written byte |
| `block_size_for(size)` | Smallest power-of-two total size ≥ `size + 20` (no I/O) |
| `inner()` → `&DynamicBlockList` | Underlying sync handle for streaming reads |

---

## Streaming reads

`read()` and `read_into()` verify the CRC on every call and either allocate a
`Vec<u8>` or copy into a caller buffer.  For large payloads — or when you need
to hand a byte range to another layer (e.g. `sendfile`, a scatter-gather
buffer, or an async runtime) — `DynamicBlockList` exposes three building
blocks that let you issue a single raw read:

| Building block | I/O cost | Returns |
|---|---|---|
| `block.data_start()` | none (pure arithmetic) | start of payload as `u64` |
| `list.data_end(block)?` | 1 × 4-byte read (`data_len`) | one-past-end of written data as `u64` |
| `list.bstack().get_into(start, buf)?` | 1 × `pread` of your chosen length | fills `buf` from the file |

```rust
use bllist::DynamicBlockList;

let list = DynamicBlockList::open("data.blld")?;
// … obtain `block` from push_front / pop_front / root traversal …

// Compute the byte range with no file I/O.
let start: u64 = block.data_start();
let end:   u64 = list.data_end(block)?;  // one 4-byte read

// Single pread into a caller-owned buffer — no CRC, no Vec allocation.
let mut buf = vec![0u8; (end - start) as usize];
list.bstack().get_into(start, &mut buf)?;

// Or fill a sub-range of an existing buffer:
list.bstack().get_into(start, &mut frame[offset..])?;
```

> **Only read-only BStack operations are safe**: `get`, `get_into`, `peek`,
> `len`.  Never call `push`, `pop`, or `set` on the handle returned by
> `bstack()` — doing so can silently corrupt the list structure.  Use `read()`
> or `read_into()` when CRC verification matters.

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
│  Block (total size = 2^k bytes, k ≥ 5)                                   │
│  checksum(4) │ next(8) │ block_size(4) │ data_len(4) │ payload(bs-20 B)  │
├──────────────────────────────────────────────────────────────────────────┤
│  Block …                                                                 │
└──────────────────────────────────────────────────────────────────────────┘
```

- The **bllist-dynamic header** stores the root offset and 32 bin free-list heads. Bin *k* holds free blocks whose **total on-disk size** equals 2^*k* bytes (bins 0–4 are always empty; minimum is bin 5 = 32 bytes).
- The **`block_size` field** stores the total on-disk size of the block (header + payload), always a power of two (≥ 32). Payload capacity = `block_size − 20`.
- The **block checksum** covers bytes `[4..block_size]` (next + block_size + data_len + full payload field, including zero-padded tail).
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
| Block size on disk | `PAYLOAD_CAPACITY + 12` | Power of two ≥ `payload + 20` (min 32) |
| Free list | Single flat list | 32 power-of-two bins |
| Splitting | No | Up to `MAX_SPLIT` = 3 levels above target bin |
| Coalescing on open | No | Adjacent free blocks merged when sum is power of two |
| Orphan scan | O(n) slot enumeration | O(n) sequential scan + rebuild |
| File magic | `"BLLS"` | `"BLLD"` |
| On-disk format version | 1 | 2 |

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
