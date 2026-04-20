//! # bllist
//!
//! `bllist` provides durable, crash-safe, checksummed block-based linked-list
//! allocators built on top of a single [`bstack`] file.
//!
//! Each entry in the list is a fixed-size *block* stored directly on disk.
//! Blocks are linked by 64-bit logical file offsets. A CRC32 checksum guards
//! every block against partial writes and silent corruption. The underlying
//! file is a valid BStack file, so all of BStack's crash-recovery guarantees
//! apply as well.
//!
//! ## Quick start
//!
//! ```no_run
//! use bllist::FixedBlockList;
//!
//! // Open (or create) a list backed by "data.blls".
//! // 52 bytes of payload per block (64 bytes total on disk).
//! let list = FixedBlockList::<52>::open("data.blls")?;
//!
//! // Push items onto the front.
//! list.push_front(b"hello")?;
//! list.push_front(b"world")?;
//!
//! // Pop in LIFO order.
//! while let Some(data) = list.pop_front()? {
//!     println!("{}", String::from_utf8_lossy(&data));
//! }
//! // prints "world", then "hello"
//! # Ok::<(), bllist::Error>(())
//! ```
//!
//! ## File layout
//!
//! ```text
//! ┌──────────────────────────┬───────────────────────────────────────────┐
//! │  BStack header (16 B)    │  bllist header (24 B at logical offset 0) │
//! │  "BSTK" magic + clen     │  "BLLS" + version + root + free_head      │
//! ├──────────────────────────┴───────────────────────────────────────────┤
//! │  Block 0  (PAYLOAD_CAPACITY+12 bytes, logical offset 24)             │
//! │  checksum(4) │ next(8) │ payload(PAYLOAD_CAPACITY)                   │
//! ├──────────────────────────────────────────────────────────────────────┤
//! │  Block 1  (PAYLOAD_CAPACITY+12 bytes, logical offset 24+PC+12)  …    │
//! └──────────────────────────────────────────────────────────────────────┘
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

pub mod error;
pub mod fixed;

pub use error::Error;
pub use fixed::{BlockRef, FixedBlockList};
