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
//! // Blocks are sized to the next power of two automatically.
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
//! │  Block 1  …                                                           │
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
//! │  Block (variable size)                                                    │
//! │  checksum(4) │ next(8) │ capacity(4) │ data_len(4) │ payload(capacity B) │
//! ├──────────────────────────────────────────────────────────────────────────┤
//! │  Block …                                                                  │
//! └──────────────────────────────────────────────────────────────────────────┘
//! ```
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

pub use dynamic::{DynBlockRef, DynamicBlockList};
pub use error::Error;
pub use fixed::{BlockRef, FixedBlockList};
