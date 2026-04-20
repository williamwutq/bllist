# bllist

Durable, crash-safe, checksummed block-based linked list allocators stored in a single file.

`bllist` builds on [`bstack`](https://crates.io/crates/bstack) to provide persistent linked lists backed by fixed-size blocks. Every block carries a CRC32 checksum; writes flush durably to disk before returning; and the file survives unclean shutdowns through automatic orphan recovery on the next open.

[![Crates.io](https://img.shields.io/crates/v/bllist)](https://crates.io/crates/bllist)
[![Docs.rs](https://img.shields.io/docsrs/bllist)](https://docs.rs/bllist)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

---

## Features

- **Fixed-size blocks** — constant overhead, no fragmentation, `O(1)` alloc and free
- **CRC32 integrity** — every block is checksummed; corruption is detected on read
- **Crash safety** — all mutations are durable before returning; orphaned blocks are recovered on the next `open`
- **Free list** — freed blocks are reused immediately; the file only grows when the free list is empty
- **Zero-copy reads** — `read_into` and `pop_front_into` fill a caller-supplied buffer directly from the file
- **Thread-safe** — `Send + Sync`; concurrent reads are efficient via `pread` on Unix/Windows
- **Valid BStack file** — a `bllist` file is a valid `bstack` file; the BStack header and crash-recovery semantics are inherited for free

---

## Quick start

Add to `Cargo.toml`:

```toml
[dependencies]
bllist = "0.1"
```

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

---

## File layout

A `bllist` file is a valid BStack file:

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

- The **bllist header** stores the root block offset and free-list-head offset.
- The **block checksum** covers bytes `[4..PAYLOAD_CAPACITY+12]` (next pointer + full payload field). Payload bytes beyond the last `write` are guaranteed to be zero.
- The **free list** is an embedded linked list using the `next` field of freed blocks.

---

## Crash safety details

`bllist` is designed around two principles:

1. **Durable writes** — every `stack.set()` / `stack.push()` call issues `fsync` (or `F_FULLFSYNC` on macOS) before returning.
2. **CRC-detected partial writes** — the checksum over `next + payload` detects any block that was partially overwritten before a crash.

On `open`, the file is scanned for *orphaned* blocks (slots not reachable from the active list or the free list). These are blocks that were allocated but not yet linked when the process died. They are silently reclaimed into the free list.

| Crash point | Effect | Recovery |
|---|---|---|
| During `alloc` (file grow) | Block exists but is in no list | Reclaimed as orphan on next `open` |
| After `alloc`, before `push_front` links it | Block written but root not updated | Reclaimed as orphan on next `open` |
| After `pop_front` advances root, before `free` | Block exists but in no list | Reclaimed as orphan on next `open` |

No data that was fully committed (root updated) is ever lost.

---

## Choosing `PAYLOAD_CAPACITY`

The const generic parameter is the exact number of bytes you want to store per block.

- Minimum: `1`
- For small records: `52` (64 bytes on disk), `116` (128 bytes on disk)
- For larger records: set to your typical payload size directly
- `PAYLOAD_CAPACITY = 0` is rejected at compile time

---

## Direct file access — use with extreme caution

A `bllist` file is a valid BStack file, so you can open it with
`bstack::BStack::open` or inspect the raw bytes with any file tool.
**Writing to the file outside of `bllist` is strongly discouraged.**
`bllist` does not re-validate structural invariants on every operation, so
direct writes can silently corrupt the list in ways that are not caught until
much later — or not caught at all.

Specific dangers:

| Operation                          | Risk |
|------------------------------------|------|
| `BStack::push`                     | Appends raw bytes that are not a complete, aligned block; breaks slot enumeration and orphan recovery |
| `BStack::pop`                      | May truncate a block mid-stream or destroy the bllist header |
| `BStack::set` at offsets 0–23      | Overwrites the bllist header; can corrupt the root or free-list-head pointer |
| `BStack::set` inside a block       | Invalidates the block's CRC; `read` will return a checksum error |
| Raw file writes (`write(2)`, etc.) | Bypasses the advisory lock entirely; any of the above, plus potential torn writes |

**The exclusive advisory lock** (`flock` on Unix, `LockFileEx` on Windows)
held by a live `FixedBlockList` prevents a second process from opening the
same file through BStack simultaneously. It does **not** prevent raw file
descriptor access, so a process that opens the file without going through
BStack can bypass the lock and cause corruption.

**Safe read-only inspection** is possible: open the file with
`bstack::BStack::open` and use only `get`, `peek`, and `len`. These calls do
not write to the file and will not disturb the bllist state. Mutating calls
(`push`, `pop`, `set`) must not be used.

---

## License

MIT — see [LICENSE](LICENSE).
