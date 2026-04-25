# bllist

Durable, crash-safe, checksummed block-based linked list allocators stored in a single file.

`bllist` builds on [`bstack`](https://crates.io/crates/bstack) to provide persistent linked lists backed by fixed-size or variable-size blocks. Every block carries a CRC32 checksum; writes flush durably to disk before returning; and the file survives unclean shutdowns through automatic orphan recovery on the next open.

[![Crates.io](https://img.shields.io/crates/v/bllist)](https://crates.io/crates/bllist)
[![Docs.rs](https://img.shields.io/docsrs/bllist)](https://docs.rs/bllist)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

---

## Features

- **Four allocator types** — singly-linked (`FixedBlockList`, `DynamicBlockList`) and doubly-linked (`FixedDblList`, `DynamicDblList`) variants for fixed and variable-size records
- **Bidirectional iteration** — `FixedDblIter` and `DynDblIter` implement `DoubleEndedIterator`, enabling forward, reverse (`.rev()`), and interleaved front/back traversal; the singly-linked variants provide forward-only `FixedIter` / `DynIter`
- **O(1) push/pop at both ends** — doubly-linked types store a tail pointer and support `push_back` / `pop_back` in addition to `push_front` / `pop_front`
- **CRC32 integrity** — every block is checksummed; corruption is detected on read
- **Crash safety** — all mutations are durable before returning; orphaned blocks are recovered on the next `open`; doubly-linked lists also rebuild the tail pointer on recovery
- **Power-of-two block sizes** — dynamic blocks have a total on-disk footprint that is always a power of two, enabling splitting and coalescing
- **Splitting** — when a free block is larger than needed (within `MAX_SPLIT` = 3 levels), it is halved and the spare half returned to its bin, reducing wasted space
- **Coalescing on open** — adjacent free blocks whose combined size is a power of two are merged into a single larger block, fighting long-term fragmentation
- **Tail-block shrink** — freeing the last block in the file pops it from the BStack instead of adding it to the free list, keeping the file compact in sequential push/pop workloads
- **Zero-copy reads** — `read_into`, `pop_front_into`, and `pop_back_into` fill a caller-supplied buffer directly from the file
- **Async I/O** — `AsyncFixedBlockList` and `AsyncDynamicBlockList` wrappers offload every blocking call to Tokio's thread pool; enable with `features = ["async"]`
- **Thread-safe** — `Send + Sync`; concurrent reads are efficient via `pread` on Unix/Windows
- **Valid BStack files** — all list types produce valid `bstack` files; the BStack header and crash-recovery semantics are inherited for free
- **Cross-type protection** — all four list types use distinct file magics (`"BLLS"`, `"BLLD"`, `"BLDF"`, `"BLDD"`) and cannot open each other's files

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

### Doubly-linked fixed-size blocks

```rust
use bllist::FixedDblList;

fn main() -> Result<(), bllist::Error> {
    // 44 bytes of payload per block (64 bytes total on disk).
    let list = FixedDblList::<44>::open("data.bldf")?;

    // Queue: push to back, pop from front (FIFO).
    list.push_back(b"task-1")?;
    list.push_back(b"task-2")?;
    list.push_back(b"task-3")?;

    while let Some(data) = list.pop_front()? {
        println!("{}", String::from_utf8_lossy(&data));
    }
    // prints "task-1", "task-2", "task-3"

    // Or iterate in reverse using DoubleEndedIterator:
    list.push_back(b"a")?;
    list.push_back(b"b")?;
    list.push_back(b"c")?;
    for item in list.iter()?.rev() {
        println!("{}", String::from_utf8_lossy(&item?));
    }
    // prints "c", "b", "a"

    Ok(())
}
```

### Doubly-linked variable-size blocks

```rust
use bllist::DynamicDblList;

fn main() -> Result<(), bllist::Error> {
    // block_size_for(size) = smallest power-of-two ≥ size+28 (28-byte header).
    let list = DynamicDblList::open("data.bldd")?;

    list.push_back(b"short")?;
    list.push_back(b"a somewhat longer record")?;

    // Bidirectional iteration:
    let mut it = list.iter()?;
    println!("{}", String::from_utf8_lossy(&it.next().unwrap()?));      // "short"
    println!("{}", String::from_utf8_lossy(&it.next_back().unwrap()?)); // "a somewhat longer record"

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

| Method                                 | Description                                  |
|----------------------------------------|----------------------------------------------|
| `open(path)`                           | Open or create; performs crash recovery      |
| `push_front(data)`                     | Allocate, write, and prepend to the list     |
| `pop_front()` → `Option<Vec<u8>>`      | Unlink head, read payload, free block        |
| `pop_front_into(buf)` → `bool`         | Zero-copy pop into caller buffer             |
| `alloc()` → `BlockRef`                 | Allocate a raw block (from free list or new) |
| `free(block)`                          | Return a block to the free list              |
| `write(block, data)`                   | Write payload, preserve next pointer         |
| `read(block)` → `Vec<u8>`              | Read and checksum-verify payload             |
| `read_into(block, buf)`                | Zero-copy read into caller buffer            |
| `set_next(block, next)`                | Update next pointer, preserve payload        |
| `get_next(block)` → `Option<BlockRef>` | Read next pointer (no CRC check)             |
| `root()` → `Option<BlockRef>`          | Current head of the active list              |
| `iter()` → `FixedIter<'_>`             | Forward iterator from head to tail           |
| `payload_capacity()`                   | `PAYLOAD_CAPACITY`                           |

### `BlockRef`

A `Copy` handle encoding a block's logical byte offset in the BStack file. Treat it like a typed index; never forge offsets manually.

### `DynamicBlockList`

Blocks may hold any payload up to 2^31 − 20 bytes.  The **total on-disk footprint** of a block (20-byte header + payload) is always a power of two, with a minimum of 32 bytes (bin 5, 12-byte payload).  Allocation first checks the exact power-of-two bin, then searches up to `MAX_SPLIT` = 3 bins higher and splits if found, and finally extends the file.  On open, adjacent free blocks whose combined size is a power of two are coalesced.

| Method                                    | Description                                                                  |
|-------------------------------------------|------------------------------------------------------------------------------|
| `open(path)`                              | Open or create; validates header; coalesces free blocks and recovers orphans |
| `push_front(data)`                        | Allocate, write, and prepend to the list                                     |
| `pop_front()` → `Option<Vec<u8>>`         | Unlink head, read payload, free block                                        |
| `pop_front_into(buf)` → `bool`            | Zero-copy pop into caller buffer                                             |
| `alloc(size)` → `DynBlockRef`             | Allocate block; splits from larger bin or extends file                       |
| `free(block)`                             | Return a block to its bin                                                    |
| `write(block, data)`                      | Write payload, update `data_len`                                             |
| `read(block)` → `Vec<u8>`                 | Read `data_len` bytes, checksum-verify                                       |
| `read_into(block, buf)`                   | Zero-copy read into caller buffer                                            |
| `set_next(block, next)`                   | Update next pointer, preserve payload                                        |
| `get_next(block)` → `Option<DynBlockRef>` | Read next pointer (no CRC check)                                             |
| `root()` → `Option<DynBlockRef>`          | Current head of the active list                                              |
| `capacity(block)` → `usize`               | Payload capacity = `block_size − 20`                                         |
| `data_len(block)` → `usize`               | Bytes last written to this block                                             |
| `data_start(block)` → `u64`               | Logical offset of first payload byte (validates offset)                      |
| `data_end(block)` → `u64`                 | Logical offset past the last written byte (reads `data_len`)                 |
| `bstack()` → `&BStack`                    | Underlying file handle for raw read-only streaming                           |
| `iter()` → `DynIter<'_>`                  | Forward iterator from head to tail                                           |
| `block_size_for(size)` → `usize`          | Smallest power-of-two total size ≥ `size + 20` (min 32)                      |

### `DynBlockRef`

A `Copy` handle encoding a dynamic block's logical byte offset. Analogous to `BlockRef` but for `DynamicBlockList`.

| Method                 | Description                                                            |
|------------------------|------------------------------------------------------------------------|
| `data_start()` → `u64` | Logical offset of the first payload byte (`self.0 + 20`); pure, no I/O |

### `FixedIter<'a, PAYLOAD_CAPACITY>`

A forward iterator over the active list of a `FixedBlockList`.  Obtained via
`list.iter()?`.  Each item is `Result<Vec<u8>, Error>`; the `Vec` is always
`PAYLOAD_CAPACITY` bytes long (zero-padded past the last write).  CRC is
verified on every step; the iterator stops after the first error.

### `DynIter<'a>`

A forward iterator over the active list of a `DynamicBlockList`.  Obtained
via `list.iter()?`.  Each item is `Result<Vec<u8>, Error>` containing exactly
the bytes last written to that block.  CRC is verified on every step; the
iterator stops after the first error.

---

### `FixedDblList<PAYLOAD_CAPACITY>`

`PAYLOAD_CAPACITY` must be `> 0`.  Each block occupies `PAYLOAD_CAPACITY + 20`
bytes on disk (4-byte CRC + 8-byte `prev` + 8-byte `next` + payload).  The
file header (32 bytes) stores `root`, `tail`, and `free_head`.

| Method                                    | Description                                        |
|-------------------------------------------|----------------------------------------------------|
| `open(path)`                              | Open or create; recovers orphans and rebuilds tail |
| `push_front(data)`                        | Allocate, write, and prepend to the list           |
| `push_back(data)`                         | Allocate, write, and append to the list            |
| `pop_front()` → `Option<Vec<u8>>`         | Unlink head, read payload, free block              |
| `pop_back()` → `Option<Vec<u8>>`          | Unlink tail, read payload, free block              |
| `pop_front_into(buf)` → `bool`            | Zero-copy pop from head into caller buffer         |
| `pop_back_into(buf)` → `bool`             | Zero-copy pop from tail into caller buffer         |
| `alloc()` → `BlockDblRef`                 | Allocate a raw block                               |
| `free(block)`                             | Return a block to the free list                    |
| `write(block, data)`                      | Write payload, preserve prev/next                  |
| `read(block)` → `Vec<u8>`                 | Read and checksum-verify payload                   |
| `read_into(block, buf)`                   | Zero-copy read into caller buffer                  |
| `set_next(block, next)`                   | Update next pointer, preserve payload and prev     |
| `set_prev(block, prev)`                   | Update prev pointer, preserve payload and next     |
| `get_next(block)` → `Option<BlockDblRef>` | Read next pointer (no CRC check)                   |
| `get_prev(block)` → `Option<BlockDblRef>` | Read prev pointer (no CRC check)                   |
| `root()` → `Option<BlockDblRef>`          | Current head of the active list                    |
| `tail()` → `Option<BlockDblRef>`          | Current tail of the active list                    |
| `iter()` → `FixedDblIter<'_>`             | Double-ended iterator (forward and backward)       |
| `payload_capacity()`                      | `PAYLOAD_CAPACITY`                                 |

### `BlockDblRef`

A `Copy` handle encoding a `FixedDblList` block's logical byte offset.  Same
`Display` / `LowerHex` / `UpperHex` / `From` traits as `BlockRef`.

### `FixedDblIter<'a, PAYLOAD_CAPACITY>`

A double-ended iterator over the active list of a `FixedDblList`.  Obtained
via `list.iter()?`.  Implements both `Iterator` (forward, head→tail) and
`DoubleEndedIterator` (backward, tail→head).  When both cursors converge on the
same block, it is yielded exactly once.  CRC is verified on every item; the
iterator terminates on the first error from either end.

---

### `DynamicDblList`

Same bin-based allocator as `DynamicBlockList` with `prev` pointers added.
The block header is 28 bytes (CRC + prev + next + block\_size + data\_len);
`block_size_for(size)` returns the smallest power-of-two ≥ `size + 28` (min 32,
payload capacity = 4 bytes for bin 5).

| Method                                       | Description                                                |
|----------------------------------------------|------------------------------------------------------------|
| `open(path)`                                 | Open or create; coalesces, recovers orphans, rebuilds tail |
| `push_front(data)`                           | Allocate, write, and prepend                               |
| `push_back(data)`                            | Allocate, write, and append                                |
| `pop_front()` → `Option<Vec<u8>>`            | Unlink head, read payload, free block                      |
| `pop_back()` → `Option<Vec<u8>>`             | Unlink tail, read payload, free block                      |
| `pop_front_into(buf)` → `bool`               | Zero-copy pop from head                                    |
| `pop_back_into(buf)` → `bool`                | Zero-copy pop from tail                                    |
| `alloc(size)` → `DynBlockDblRef`             | Allocate block; splits or extends file                     |
| `free(block)`                                | Return block to its bin                                    |
| `write(block, data)`                         | Write payload, update `data_len`                           |
| `read(block)` → `Vec<u8>`                    | Read `data_len` bytes, checksum-verify                     |
| `read_into(block, buf)`                      | Zero-copy read into caller buffer                          |
| `set_next(block, next)`                      | Update next pointer, preserve all other fields             |
| `set_prev(block, prev)`                      | Update prev pointer, preserve all other fields             |
| `get_next(block)` → `Option<DynBlockDblRef>` | Read next pointer (no CRC check)                           |
| `get_prev(block)` → `Option<DynBlockDblRef>` | Read prev pointer (no CRC check)                           |
| `root()` → `Option<DynBlockDblRef>`          | Current head of the active list                            |
| `tail()` → `Option<DynBlockDblRef>`          | Current tail of the active list                            |
| `capacity(block)` → `usize`                  | Payload capacity = `block_size − 28`                       |
| `data_len(block)` → `usize`                  | Bytes last written to this block                           |
| `data_start(block)` → `u64`                  | Logical offset of first payload byte (validates offset)    |
| `data_end(block)` → `u64`                    | Logical offset past the last written byte                  |
| `bstack()` → `&BStack`                       | Underlying file handle for raw read-only streaming         |
| `iter()` → `DynDblIter<'_>`                  | Double-ended iterator (forward and backward)               |
| `block_size_for(size)` → `usize`             | Smallest power-of-two total size ≥ `size + 28` (min 32)    |

### `DynBlockDblRef`

A `Copy` handle encoding a `DynamicDblList` block's logical byte offset.

| Method                 | Description                                                            |
|------------------------|------------------------------------------------------------------------|
| `data_start()` → `u64` | Logical offset of the first payload byte (`self.0 + 28`); pure, no I/O |

### `DynDblIter<'a>`

A double-ended iterator over the active list of a `DynamicDblList`.  Obtained
via `list.iter()?`.  Implements `Iterator` and `DoubleEndedIterator` with the
same convergence semantics as `FixedDblIter`.

### `AsyncFixedBlockList<PAYLOAD_CAPACITY>` *(feature `async`)*

An async, `Clone`-able wrapper around `FixedBlockList`. Each method runs on
Tokio's blocking-thread pool via `spawn_blocking`.  Data inputs accept any
`impl AsRef<[u8]> + Send + 'static` (e.g. `Vec<u8>`, `Box<[u8]>`, `&'static [u8]`).

> **No async iterator** — `spawn_blocking` requires `'static` closures, so a
> streaming async iterator cannot hold a borrowed `&'a` reference to the inner
> list across await points.  Use `list.inner().iter()?` to iterate
> synchronously, or collect into a `Vec` inside one `spawn_blocking` block.

| Method                                       | Description                                |
|----------------------------------------------|--------------------------------------------|
| `open(path).await`                           | Open or create on a blocking thread        |
| `push_front(data).await`                     | Allocate, write, prepend to list           |
| `pop_front().await` → `Option<Vec<u8>>`      | Unlink head, read payload, free block      |
| `alloc().await` → `BlockRef`                 | Allocate a raw block                       |
| `free(block).await`                          | Return block to free list                  |
| `write(block, data).await`                   | Write payload, preserve next pointer       |
| `read(block).await` → `Vec<u8>`              | Read and checksum-verify payload           |
| `set_next(block, next).await`                | Update next pointer                        |
| `get_next(block).await` → `Option<BlockRef>` | Read next pointer (no CRC check)           |
| `root().await` → `Option<BlockRef>`          | Current head of the active list            |
| `payload_capacity()`                         | `PAYLOAD_CAPACITY` (no I/O)                |
| `inner()` → `&FixedBlockList<N>`             | Underlying sync handle for streaming reads |

### `AsyncDynamicBlockList` *(feature `async`)*

An async, `Clone`-able wrapper around `DynamicBlockList`. Same `spawn_blocking`
approach as `AsyncFixedBlockList`.

| Method                                          | Description                                             |
|-------------------------------------------------|---------------------------------------------------------|
| `open(path).await`                              | Open or create on a blocking thread                     |
| `push_front(data).await`                        | Allocate, write, prepend to list                        |
| `pop_front().await` → `Option<Vec<u8>>`         | Unlink head, read payload, free block                   |
| `alloc(size).await` → `DynBlockRef`             | Allocate block; splits or extends file                  |
| `free(block).await`                             | Return block to its bin                                 |
| `write(block, data).await`                      | Write payload, update `data_len`                        |
| `read(block).await` → `Vec<u8>`                 | Read `data_len` bytes, checksum-verify                  |
| `set_next(block, next).await`                   | Update next pointer                                     |
| `get_next(block).await` → `Option<DynBlockRef>` | Read next pointer (no CRC check)                        |
| `root().await` → `Option<DynBlockRef>`          | Current head of the active list                         |
| `capacity(block).await` → `usize`               | Payload capacity = `block_size − 20`                    |
| `data_len(block).await` → `usize`               | Bytes last written to this block                        |
| `data_end(block).await` → `u64`                 | Logical offset past the last written byte               |
| `block_size_for(size)`                          | Smallest power-of-two total size ≥ `size + 20` (no I/O) |
| `inner()` → `&DynamicBlockList`                 | Underlying sync handle for streaming reads              |

> **No async iterator** — `spawn_blocking` requires `'static` closures, so a
> streaming async iterator cannot hold a borrowed `&'a` reference to the inner
> list across await points.  Use `list.inner().iter()?` to iterate
> synchronously, or collect into a `Vec` inside one `spawn_blocking` block.

---

## Traversal and iteration

All four list types expose an iterator via `.iter()?`.  The singly-linked types
return a **forward-only** iterator; the doubly-linked types return a
**double-ended** iterator.

```rust
use bllist::FixedBlockList;

let list = FixedBlockList::<52>::open("data.blls")?;
for item in list.iter()? {
    let payload = item?;          // Vec<u8>, always 52 bytes
    println!("{}", String::from_utf8_lossy(&payload));
}
```

```rust
use bllist::DynamicBlockList;

let list = DynamicBlockList::open("data.blld")?;
for item in list.iter()? {
    let payload = item?;          // Vec<u8>, exactly data_len bytes
    println!("{}", String::from_utf8_lossy(&payload));
}
```

```rust
use bllist::FixedDblList;

let list = FixedDblList::<44>::open("data.bldf")?;

// Forward (head → tail):
for item in list.iter()? {
    println!("{}", String::from_utf8_lossy(&item?));
}

// Backward (tail → head) via DoubleEndedIterator:
for item in list.iter()?.rev() {
    println!("{}", String::from_utf8_lossy(&item?));
}

// Alternating from both ends simultaneously:
let mut it = list.iter()?;
while let Some(front) = it.next() {
    println!("front: {}", String::from_utf8_lossy(&front?));
    if let Some(back) = it.next_back() {
        println!("back:  {}", String::from_utf8_lossy(&back?));
    }
}
```

`iter()` reads the current `root` (and `tail` for doubly-linked lists) once to
seed the iterator; each subsequent `next()` / `next_back()` call issues one
file read and CRC verification.  The iterator holds a shared `&` reference to
the list, preventing mutation while iteration is in progress.

For the doubly-linked iterators, both cursors advance toward each other.  When
they converge on the same block, that block is yielded exactly once (from
whichever side calls next), then both cursors are set to `None`.  The iterator
terminates on the first error from either end.

`DoubleEndedIterator` is **not** implemented for `FixedIter` and `DynIter` —
the singly-linked list types do not store a tail pointer, so backward traversal
would require collecting all elements first.  Use `FixedDblList` or
`DynamicDblList` when bidirectional traversal is needed.

---

## Streaming reads

`read()` and `read_into()` verify the CRC on every call and either allocate a
`Vec<u8>` or copy into a caller buffer.  For large payloads — or when you need
to hand a byte range to another layer (e.g. `sendfile`, a scatter-gather
buffer, or an async runtime) — `DynamicBlockList` exposes three building
blocks that let you issue a single raw read:

| Building block                        | I/O cost                          | Returns                               |
|---------------------------------------|-----------------------------------|---------------------------------------|
| `block.data_start()`                  | none (pure arithmetic)            | start of payload as `u64`             |
| `list.data_end(block)?`               | 1 × 4-byte read (`data_len`)      | one-past-end of written data as `u64` |
| `list.bstack().get_into(start, buf)?` | 1 × `pread` of your chosen length | fills `buf` from the file             |

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

### `FixedDblList` files (`"BLDF"`)

```
┌──────────────────────────┬──────────────────────────────────────────────────────────┐
│  BStack header (16 B)    │  bllist-dbl-fixed header (32 B, logical offset 0)        │
│  "BSTK" magic + clen     │  "BLDF" + version + root + tail + free_head              │
├──────────────────────────┴──────────────────────────────────────────────────────────┤
│  Block 0  (PAYLOAD_CAPACITY+20 bytes, logical offset 32)                            │
│  checksum(4) │ prev(8) │ next(8) │ payload(PAYLOAD_CAPACITY)                        │
├─────────────────────────────────────────────────────────────────────────────────────┤
│  Block 1  …                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

- The **header** stores `root`, `tail`, and `free_head`; `tail` enables O(1) `push_back` and `pop_back`.
- The **block checksum** covers bytes `[4..PAYLOAD_CAPACITY+20]` (prev + next + full payload).
- The **free list** is singly-linked via the `next` field of freed blocks (`prev` is zeroed on free).

### `DynamicDblList` files (`"BLDD"`)

```
┌──────────────────────────┬──────────────────────────────────────────────────────────────┐
│  BStack header (16 B)    │  bllist-dbl-dynamic header (280 B, logical off 0)            │
│  "BSTK" magic + clen     │  "BLDD" + version + root + tail + bin_heads[32]              │
├──────────────────────────┴──────────────────────────────────────────────────────────────┤
│  Block (total size = 2^k bytes, k ≥ 5)                                                  │
│  checksum(4) │ prev(8) │ next(8) │ block_size(4) │ data_len(4) │ payload(bs-28 B)      │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│  Block …                                                                                │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

- The **header** stores `root`, `tail`, and 32 bin free-list heads.
- Block layout adds `prev` before `next`; payload capacity = `block_size − 28`. Minimum block is 32 bytes (bin 5, 4-byte payload).
- `block_size_for(size)` returns the smallest power-of-two ≥ `size + 28` (min 32).
- Free blocks are singly-linked via the `next` field (`prev` zeroed); bin allocator, splitting, and coalescing are identical to `DynamicBlockList`.

---

## Crash safety details

`bllist` is designed around two principles:

1. **Durable writes** — every `stack.set()` / `stack.push()` call issues `fsync` (or `F_FULLFSYNC` on macOS) before returning.
2. **CRC-detected partial writes** — the checksum over the block header and payload detects any block that was partially overwritten before a crash.

On `open`, the file is scanned for *orphaned* blocks (allocated but not reachable from either the active list or any free list). They are silently reclaimed.

| Crash point                                    | Effect                             | Recovery                           |
|------------------------------------------------|------------------------------------|------------------------------------|
| During `alloc` (file grow)                     | Block exists but is in no list     | Reclaimed as orphan on next `open` |
| After `alloc`, before `push_front` links it    | Block written but root not updated | Reclaimed as orphan on next `open` |
| After `pop_front` advances root, before `free` | Block exists but in no list        | Reclaimed as orphan on next `open` |

No data that was fully committed (root updated) is ever lost.

---

## Choosing the right type

|                            | `FixedBlockList`        | `DynamicBlockList`                     | `FixedDblList`          | `DynamicDblList`                       |
|----------------------------|-------------------------|----------------------------------------|-------------------------|----------------------------------------|
| Record size                | Always the same         | Varies                                 | Always the same         | Varies                                 |
| Links                      | Singly-linked           | Singly-linked                          | Doubly-linked           | Doubly-linked                          |
| On-disk overhead per block | 12 bytes                | 20 bytes                               | 20 bytes                | 28 bytes                               |
| Block size on disk         | `PAYLOAD_CAPACITY + 12` | Power of two ≥ `payload + 20` (min 32) | `PAYLOAD_CAPACITY + 20` | Power of two ≥ `payload + 28` (min 32) |
| push/pop at both ends      | No                      | No                                     | Yes                     | Yes                                    |
| Bidirectional iteration    | No                      | No                                     | Yes                     | Yes                                    |
| Free list                  | Single flat list        | 32 power-of-two bins                   | Single flat list        | 32 power-of-two bins                   |
| Splitting                  | No                      | Up to `MAX_SPLIT` = 3 levels           | No                      | Up to `MAX_SPLIT` = 3 levels           |
| Coalescing on open         | No                      | Yes (adjacent same-power-of-two runs)  | No                      | Yes (adjacent same-power-of-two runs)  |
| Tail-block shrink on free  | Yes                     | Yes                                    | Yes                     | Yes                                    |
| Tail rebuilt on open       | N/A                     | N/A                                    | Yes                     | Yes                                    |
| Orphan scan                | O(n) slot enumeration   | O(n) sequential scan + rebuild         | O(n) slot enumeration   | O(n) sequential scan + rebuild         |
| File magic                 | `"BLLS"`                | `"BLLD"`                               | `"BLDF"`                | `"BLDD"`                               |
| On-disk format version     | 1                       | 2                                      | 1                       | 1                                      |

**Choose singly-linked** (`FixedBlockList` / `DynamicBlockList`) when you only
need a stack (push/pop from one end) or a forward-only iterator — they have
lower per-block overhead.

**Choose doubly-linked** (`FixedDblList` / `DynamicDblList`) when you need a
queue (push to one end, pop from the other), bidirectional iteration, or
efficient access to the tail.

### Choosing `PAYLOAD_CAPACITY` for fixed-size lists

- Minimum: `1`
- For `FixedBlockList` (`+12` overhead): `52` (64 bytes on disk), `116` (128 bytes on disk)
- For `FixedDblList` (`+20` overhead): `44` (64 bytes on disk), `108` (128 bytes on disk)
- `PAYLOAD_CAPACITY = 0` is rejected at compile time for both types

---

## Direct file access — use with extreme caution

All list types produce valid BStack files, so you can open them with
`bstack::BStack::open` or inspect the raw bytes with any file tool.
**Writing to the file outside of `bllist` is strongly discouraged.**
`bllist` does not re-validate structural invariants on every operation, so
direct writes can silently corrupt the list in ways that are not caught until
much later — or not caught at all.

Specific dangers:

| Operation                          | Risk                                                                                                  |
|------------------------------------|-------------------------------------------------------------------------------------------------------|
| `BStack::push`                     | Appends raw bytes that are not a complete, aligned block; breaks slot enumeration and orphan recovery |
| `BStack::pop`                      | May truncate a block mid-stream or destroy the list header                                            |
| `BStack::set` at header offsets    | Overwrites root or free-list / bin-head pointers                                                      |
| `BStack::set` inside a block       | Invalidates the block's CRC; `read` will return a checksum error                                      |
| Raw file writes (`write(2)`, etc.) | Bypasses the advisory lock entirely; any of the above, plus potential torn writes                     |

**The exclusive advisory lock** (`flock` on Unix, `LockFileEx` on Windows)
held by a live list prevents a second process from opening the same file
through BStack simultaneously. It does **not** prevent raw file
descriptor access, so a process that opens the file without going through
BStack can bypass the lock and cause corruption.

**Safe read-only inspection** is possible: open the file with
`bstack::BStack::open` and use only `get`, `peek`, and `len`. These calls do
not write to the file and will not disturb the list state. Mutating calls
(`push`, `pop`, `set`) must not be used.
