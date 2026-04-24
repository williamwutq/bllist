//! # bllist
//!
//! `bllist` provides durable, crash-safe, checksummed block-based linked-list
//! allocators built on top of a single [`bstack`] file.
//!
//! Two allocator types are available:
//!
//! | Type | Block size | File magic | Use when |
//! |------|-----------|------------|----------|
//! | [`FixedBlockList<PAYLOAD_CAPACITY>`](FixedBlockList) | constant | `"BLLS"` | All records are the same size |
//! | [`DynamicBlockList`] | variable (power-of-two bins) | `"BLLD"` | Records vary in size |
//!
//! The two types use **different file formats** and cannot open each other's
//! files. Both inherit BStack's exclusive advisory lock, durable fsync writes,
//! and crash-recovery guarantees.
//!
//! ## Quick start — fixed-size blocks
//!
//! ```no_run
//! use bllist::FixedBlockList;
//!
//! // 52 bytes of payload per block (64 bytes total on disk).
//! let list = FixedBlockList::<52>::open("data.blls")?;
//!
//! list.push_front(b"hello")?;
//! list.push_front(b"world")?;
//!
//! while let Some(data) = list.pop_front()? {
//!     println!("{}", String::from_utf8_lossy(&data));
//! }
//! // prints "world", then "hello"
//! # Ok::<(), bllist::Error>(())
//! ```
//!
//! ## Quick start — variable-size blocks
//!
//! ```no_run
//! use bllist::DynamicBlockList;
//!
//! // The total on-disk block size (header + payload) is a power of two.
//! // A 5-byte push occupies 32 bytes on disk (5+20=25 → 32, bin 5).
//! let list = DynamicBlockList::open("data.blld")?;
//!
//! list.push_front(b"short")?;
//! list.push_front(b"a somewhat longer record")?;
//!
//! while let Some(data) = list.pop_front()? {
//!     println!("{}", String::from_utf8_lossy(&data));
//! }
//! # Ok::<(), bllist::Error>(())
//! ```
//!
//! ## File layouts
//!
//! ### `FixedBlockList` (`"BLLS"`)
//!
//! ```text
//! ┌──────────────────────────┬───────────────────────────────────────────┐
//! │  BStack header (16 B)    │  bllist header (24 B at logical offset 0) │
//! │  "BSTK" magic + clen     │  "BLLS" + version + root + free_head      │
//! ├──────────────────────────┴───────────────────────────────────────────┤
//! │  Block 0  (PAYLOAD_CAPACITY+12 bytes, logical offset 24)             │
//! │  checksum(4) │ next(8) │ payload(PAYLOAD_CAPACITY)                   │
//! ├──────────────────────────────────────────────────────────────────────┤
//! │  Block 1  …                                                          │
//! └──────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ### `DynamicBlockList` (`"BLLD"`)
//!
//! ```text
//! ┌──────────────────────────┬───────────────────────────────────────────────┐
//! │  BStack header (16 B)    │  bllist-dynamic header (272 B, logical off 0) │
//! │  "BSTK" magic + clen     │  "BLLD" + version + root + bin_heads[32]      │
//! ├──────────────────────────┴───────────────────────────────────────────────┤
//! │  Block (total size = 2^k bytes, k ≥ 5)                                   │
//! │  checksum(4) │ next(8) │ block_size(4) │ data_len(4) │ payload(bs-20 B)  │
//! ├──────────────────────────────────────────────────────────────────────────┤
//! │  Block …                                                                 │
//! └──────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! Bin *k* holds free blocks whose **total** on-disk size equals 2^*k* bytes.
//! The minimum block is bin 5 (32 bytes total, 12-byte payload).  Large free
//! blocks may be split to satisfy smaller requests; adjacent free blocks may
//! be coalesced on open.
//!
//! The BStack header is managed transparently by the [`bstack`] crate;
//! callers only see logical offsets starting at 0.
//!
//! ## Crash safety
//!
//! Every mutation flushes durably (via `fsync` / `F_FULLFSYNC`) before
//! returning. If the process is killed mid-operation the worst case is one
//! *orphaned* block that is silently reclaimed the next time the file is
//! opened.
//!
//! ## Traversal and iteration
//!
//! Both list types expose a forward iterator via `.iter()?`:
//!
//! ```no_run
//! use bllist::FixedBlockList;
//!
//! let list = FixedBlockList::<52>::open("data.blls")?;
//! for item in list.iter()? {
//!     let payload = item?;         // Vec<u8>, always PAYLOAD_CAPACITY bytes
//!     println!("{}", String::from_utf8_lossy(&payload));
//! }
//! # Ok::<(), bllist::Error>(())
//! ```
//!
//! ```no_run
//! use bllist::DynamicBlockList;
//!
//! let list = DynamicBlockList::open("data.blld")?;
//! for item in list.iter()? {
//!     let payload = item?;         // Vec<u8>, exactly data_len bytes
//!     println!("{}", String::from_utf8_lossy(&payload));
//! }
//! # Ok::<(), bllist::Error>(())
//! ```
//!
//! `iter()` reads the current root once to seed the iterator; each subsequent
//! [`next`](Iterator::next) call issues one file read with CRC verification.
//! The iterator holds a `&` reference to the list, preventing mutation during
//! traversal.
//!
//! [`DoubleEndedIterator`] is **not** implemented — both list types are
//! singly-linked, so backward traversal is not possible without first
//! collecting all elements.
//!
//! ### Why the async wrappers have no iterator
//!
//! [`AsyncFixedBlockList`] and [`AsyncDynamicBlockList`] do not provide their
//! own iterator types.  `tokio::task::spawn_blocking` requires `'static`
//! closures, which means a streaming async iterator cannot hold a borrowed
//! `&'a` reference to the inner list across `.await` points.  Use
//! `list.inner().iter()?` to iterate synchronously inside a blocking context,
//! or collect into a `Vec` inside a single `spawn_blocking` call.
//!
//! ## Async I/O *(feature `async`)*
//!
//! Enable with `features = ["async"]` in your `Cargo.toml`.  This adds two
//! [`Clone`]-able wrapper types — [`AsyncFixedBlockList`] and
//! [`AsyncDynamicBlockList`] — that run every operation on Tokio's blocking-
//! thread pool via [`tokio::task::spawn_blocking`]:
//!
//! ```no_run
//! # #[cfg(feature = "async")]
//! # async fn example() -> Result<(), bllist::Error> {
//! use bllist::AsyncDynamicBlockList;
//!
//! let list = AsyncDynamicBlockList::open("data.blld").await?;
//!
//! list.push_front(b"hello").await?;
//! list.push_front(b"world").await?;
//!
//! while let Some(data) = list.pop_front().await? {
//!     println!("{}", String::from_utf8_lossy(&data));
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Both wrapper types are `Clone`, so a single `open` call is enough even
//! when multiple tasks need concurrent access:
//!
//! ```no_run
//! # #[cfg(feature = "async")]
//! # async fn example() -> Result<(), bllist::Error> {
//! use bllist::AsyncFixedBlockList;
//! use std::sync::Arc;
//!
//! let list = AsyncFixedBlockList::<52>::open("data.blls").await?;
//! let list2 = list.clone(); // cheap Arc clone; shares the same file handle
//!
//! let h = tokio::spawn(async move {
//!     list2.push_front(b"from task").await
//! });
//! list.push_front(b"from main").await?;
//! h.await.unwrap()?;
//! # Ok(())
//! # }
//! ```
//!
//! Data inputs accept `impl AsRef<[u8]> + Send + 'static`, so both
//! `Vec<u8>` (owned, no extra copy) and `&'static [u8]` (static byte strings)
//! work directly.
//!
//! The underlying synchronous list is always accessible via
//! [`AsyncFixedBlockList::inner`] / [`AsyncDynamicBlockList::inner`] for
//! operations that do not need async (e.g. pure-computation helpers or
//! streaming raw reads through [`DynamicBlockList::bstack`]).
//!
//! ## Streaming reads (`DynamicBlockList`)
//!
//! [`read`](DynamicBlockList::read) and [`read_into`](DynamicBlockList::read_into)
//! always verify the CRC and copy or allocate on every call.  For large
//! payloads — or when you need to pass a byte range to another layer (e.g.
//! `sendfile`, a scatter-gather buffer, or an async runtime) — you can compute
//! the exact file offsets and issue a single raw read through the underlying
//! [`BStack`](bstack::BStack):
//!
//! ```no_run
//! use bllist::DynamicBlockList;
//!
//! let list = DynamicBlockList::open("data.blld")?;
//! # let block = list.alloc(0)?;
//!
//! // data_start is pure — no I/O, no Result.
//! let start: u64 = block.data_start();
//! // data_end reads the 4-byte data_len field.
//! let end: u64 = list.data_end(block)?;
//!
//! // One pread directly into a caller-owned buffer; no CRC, no allocation.
//! let mut buf = vec![0u8; (end - start) as usize];
//! list.bstack().get_into(start, &mut buf)?;
//!
//! // Or stream a sub-range into an existing buffer:
//! # let mut frame = vec![0u8; 64];
//! # let frame_offset = 0usize;
//! list.bstack().get_into(start, &mut frame[frame_offset..])?;
//! # Ok::<(), bllist::Error>(())
//! ```
//!
//! **Only read-only BStack operations are safe** (`get`, `get_into`, `peek`,
//! `len`).  Never call `push`, `pop`, or `set` on the handle returned by
//! [`bstack()`](DynamicBlockList::bstack) — doing so can silently corrupt the
//! list structure.  Use [`read`](DynamicBlockList::read) or
//! [`read_into`](DynamicBlockList::read_into) when CRC verification matters.
//!
//! ## Direct file access — use with extreme caution
//!
//! Both list types produce valid BStack files, so you can open them with
//! [`bstack::BStack::open`] or inspect raw bytes with any file tool.
//! **Writing to the file outside of `bllist` is strongly discouraged.**
//! `bllist` does not re-validate structural invariants on every operation, so
//! direct writes can silently corrupt the list in ways that are not caught
//! until much later — or not caught at all.
//!
//! | Direct BStack operation              | Risk |
//! |--------------------------------------|------|
//! | `BStack::push`                       | Appends raw bytes that are not a complete, aligned block; corrupts slot enumeration and orphan recovery |
//! | `BStack::pop`                        | May truncate a block mid-stream or destroy the list header |
//! | `BStack::set` at header offsets      | Overwrites root or free-list / bin-head pointers |
//! | `BStack::set` inside a block         | Invalidates the block's CRC; `read` returns [`Error::ChecksumMismatch`] |
//! | Raw file writes (`write(2)`, etc.)   | Bypasses the advisory lock entirely; any of the above, plus torn writes |
//!
//! The exclusive advisory lock ([`flock`] on Unix, `LockFileEx` on Windows)
//! held by a live list prevents a second process from opening the same file
//! through BStack simultaneously. It does **not** prevent raw file-descriptor
//! access.
//!
//! **Safe read-only inspection** is possible: open the file with
//! [`bstack::BStack::open`] and use only `get`, `peek`, and `len`. These
//! calls do not write to the file and will not disturb the list state.
//! Mutating calls (`push`, `pop`, `set`) must not be used.
//!
//! [`flock`]: https://man7.org/linux/man-pages/man2/flock.2.html

pub mod dynamic;
pub mod error;
pub mod fixed;

#[cfg(feature = "async")]
pub mod async_dynamic;
#[cfg(feature = "async")]
pub mod async_fixed;

pub use dynamic::{DynBlockRef, DynIter, DynamicBlockList};
pub use error::Error;
pub use fixed::{BlockRef, FixedBlockList, FixedIter};

#[cfg(feature = "async")]
pub use async_dynamic::AsyncDynamicBlockList;
#[cfg(feature = "async")]
pub use async_fixed::AsyncFixedBlockList;
