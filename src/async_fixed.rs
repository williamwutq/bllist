//! Async wrapper around [`FixedBlockList`].
//!
//! Enabled with the `async` feature flag.  Each method offloads the blocking
//! file I/O onto Tokio's blocking-thread pool via
//! [`tokio::task::spawn_blocking`] and returns a future that resolves once the
//! I/O completes.

use std::io;
use std::path::Path;
use std::sync::Arc;

use crate::{BlockRef, Error, FixedBlockList};

/// Async wrapper around [`FixedBlockList`].
///
/// Every method runs the underlying blocking operation on Tokio's blocking
/// thread pool via [`tokio::task::spawn_blocking`].  The wrapper is
/// [`Clone`]; cloning it is cheap (increments an [`Arc`] reference count).
///
/// # Opening
///
/// ```no_run
/// # #[cfg(feature = "async")]
/// # async fn example() -> Result<(), bllist::Error> {
/// use bllist::AsyncFixedBlockList;
///
/// // 52 bytes of payload per block (64 bytes total on disk).
/// let list = AsyncFixedBlockList::<52>::open("data.blls").await?;
///
/// list.push_front(b"hello").await?;
/// list.push_front(b"world").await?;
///
/// while let Some(data) = list.pop_front().await? {
///     println!("{}", String::from_utf8_lossy(&data));
/// }
/// # Ok(())
/// # }
/// ```
///
/// # Thread safety
///
/// `AsyncFixedBlockList` is `Send + Sync`.  Concurrent calls are safe; header
/// mutations are serialised through the inner `Mutex` of [`FixedBlockList`].
///
/// # Crash safety
///
/// The same guarantees as [`FixedBlockList`] apply: every mutation flushes
/// durably before returning.  If the blocking thread is killed mid-operation
/// the worst case is one orphaned block, reclaimed on the next
/// [`open`](Self::open).
///
/// # Panics
///
/// If the spawned blocking task panics, the async method returns
/// [`Error::Io`] wrapping the panic message.
#[derive(Clone)]
pub struct AsyncFixedBlockList<const PAYLOAD_CAPACITY: usize>(
    Arc<FixedBlockList<PAYLOAD_CAPACITY>>,
);

impl<const PAYLOAD_CAPACITY: usize> AsyncFixedBlockList<PAYLOAD_CAPACITY> {
    // ── constructor ───────────────────────────────────────────────────────────

    /// Open or create the file at `path` as an [`AsyncFixedBlockList`].
    ///
    /// Runs [`FixedBlockList::open`] on a Tokio blocking thread.  See that
    /// method for full open semantics, crash recovery, and possible errors.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref().to_path_buf();
        tokio::task::spawn_blocking(move || FixedBlockList::open(&path))
            .await
            .map_err(|e| Error::Io(io::Error::other(e.to_string())))?
            .map(|list| Self(Arc::new(list)))
    }

    // ── accessors ─────────────────────────────────────────────────────────────

    /// Returns the number of payload bytes available in each block.
    ///
    /// Equivalent to [`FixedBlockList::payload_capacity`]; no I/O performed.
    pub const fn payload_capacity() -> usize {
        PAYLOAD_CAPACITY
    }

    /// Return a reference to the underlying synchronous [`FixedBlockList`].
    ///
    /// Useful for direct BStack streaming reads (see
    /// [`FixedBlockList`](crate::FixedBlockList) docs).  Do **not** call
    /// mutating methods on the inner list while an async operation is in
    /// flight.
    pub fn inner(&self) -> &FixedBlockList<PAYLOAD_CAPACITY> {
        &self.0
    }

    // ── allocation ────────────────────────────────────────────────────────────

    /// Async version of [`FixedBlockList::alloc`].
    pub async fn alloc(&self) -> Result<BlockRef, Error> {
        let inner = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || inner.alloc())
            .await
            .map_err(|e| Error::Io(io::Error::other(e.to_string())))?
    }

    /// Async version of [`FixedBlockList::free`].
    pub async fn free(&self, block: BlockRef) -> Result<(), Error> {
        let inner = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || inner.free(block))
            .await
            .map_err(|e| Error::Io(io::Error::other(e.to_string())))?
    }

    // ── payload I/O ───────────────────────────────────────────────────────────

    /// Async version of [`FixedBlockList::write`].
    ///
    /// `data` must be `Send + 'static` (e.g. `Vec<u8>`, `Box<[u8]>`, or a
    /// `&'static [u8]`).  No extra copy is made when the caller already
    /// provides owned data.
    pub async fn write(
        &self,
        block: BlockRef,
        data: impl AsRef<[u8]> + Send + 'static,
    ) -> Result<(), Error> {
        let inner = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || inner.write(block, data.as_ref()))
            .await
            .map_err(|e| Error::Io(io::Error::other(e.to_string())))?
    }

    /// Async version of [`FixedBlockList::read`].
    pub async fn read(&self, block: BlockRef) -> Result<Vec<u8>, Error> {
        let inner = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || inner.read(block))
            .await
            .map_err(|e| Error::Io(io::Error::other(e.to_string())))?
    }

    // ── structural pointer operations ─────────────────────────────────────────

    /// Async version of [`FixedBlockList::set_next`].
    pub async fn set_next(&self, block: BlockRef, next: Option<BlockRef>) -> Result<(), Error> {
        let inner = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || inner.set_next(block, next))
            .await
            .map_err(|e| Error::Io(io::Error::other(e.to_string())))?
    }

    /// Async version of [`FixedBlockList::get_next`].
    pub async fn get_next(&self, block: BlockRef) -> Result<Option<BlockRef>, Error> {
        let inner = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || inner.get_next(block))
            .await
            .map_err(|e| Error::Io(io::Error::other(e.to_string())))?
    }

    // ── list head ─────────────────────────────────────────────────────────────

    /// Async version of [`FixedBlockList::root`].
    pub async fn root(&self) -> Result<Option<BlockRef>, Error> {
        let inner = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || inner.root())
            .await
            .map_err(|e| Error::Io(io::Error::other(e.to_string())))?
    }

    // ── convenience list operations ───────────────────────────────────────────

    /// Async version of [`FixedBlockList::push_front`].
    ///
    /// `data` must be `Send + 'static` (e.g. `Vec<u8>`, `Box<[u8]>`, or a
    /// `&'static [u8]`).
    pub async fn push_front(
        &self,
        data: impl AsRef<[u8]> + Send + 'static,
    ) -> Result<BlockRef, Error> {
        let inner = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || inner.push_front(data.as_ref()))
            .await
            .map_err(|e| Error::Io(io::Error::other(e.to_string())))?
    }

    /// Async version of [`FixedBlockList::pop_front`].
    pub async fn pop_front(&self) -> Result<Option<Vec<u8>>, Error> {
        let inner = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || inner.pop_front())
            .await
            .map_err(|e| Error::Io(io::Error::other(e.to_string())))?
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    type List = AsyncFixedBlockList<52>;

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn tmp(label: &str) -> std::path::PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "bllist_async_{}_{}_{}.blls",
            std::process::id(),
            label,
            n
        ));
        p
    }

    #[tokio::test]
    async fn fresh_open_empty() {
        let path = tmp("fresh");
        let list = List::open(&path).await.unwrap();
        assert_eq!(list.root().await.unwrap(), None);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn payload_capacity_const() {
        assert_eq!(List::payload_capacity(), 52);
    }

    #[tokio::test]
    async fn alloc_free_reuse() {
        let path = tmp("alloc");
        let list = List::open(&path).await.unwrap();

        let b0 = list.alloc().await.unwrap();
        let b1 = list.alloc().await.unwrap();
        list.free(b1).await.unwrap();
        let b2 = list.alloc().await.unwrap();
        assert_eq!(b2, b1);

        let _ = (b0, b2);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn write_read_roundtrip() {
        let path = tmp("rw");
        let list = List::open(&path).await.unwrap();
        let block = list.alloc().await.unwrap();

        list.write(block, b"hello async!".to_vec()).await.unwrap();
        let out = list.read(block).await.unwrap();
        assert_eq!(&out[..12], b"hello async!");
        assert!(out[12..].iter().all(|&b| b == 0));

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn set_get_next() {
        let path = tmp("next");
        let list = List::open(&path).await.unwrap();
        let b0 = list.alloc().await.unwrap();
        let b1 = list.alloc().await.unwrap();

        assert_eq!(list.get_next(b0).await.unwrap(), None);
        list.set_next(b0, Some(b1)).await.unwrap();
        assert_eq!(list.get_next(b0).await.unwrap(), Some(b1));
        list.set_next(b0, None).await.unwrap();
        assert_eq!(list.get_next(b0).await.unwrap(), None);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn push_pop_lifo() {
        let path = tmp("lifo");
        let list = List::open(&path).await.unwrap();

        list.push_front(b"first".to_vec()).await.unwrap();
        list.push_front(b"second".to_vec()).await.unwrap();
        list.push_front(b"third".to_vec()).await.unwrap();

        let d1 = list.pop_front().await.unwrap().unwrap();
        assert_eq!(&d1[..5], b"third");
        let d2 = list.pop_front().await.unwrap().unwrap();
        assert_eq!(&d2[..6], b"second");
        let d3 = list.pop_front().await.unwrap().unwrap();
        assert_eq!(&d3[..5], b"first");
        assert_eq!(list.pop_front().await.unwrap(), None);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn push_static_bytes() {
        let path = tmp("static");
        let list = List::open(&path).await.unwrap();
        list.push_front(b"static slice").await.unwrap();
        let out = list.pop_front().await.unwrap().unwrap();
        assert_eq!(&out[..12], b"static slice");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn pop_empty() {
        let path = tmp("pop_empty");
        let list = List::open(&path).await.unwrap();
        assert_eq!(list.pop_front().await.unwrap(), None);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn clone_shared_state() {
        let path = tmp("clone");
        let list = List::open(&path).await.unwrap();
        let list2 = list.clone();

        list.push_front(b"shared".to_vec()).await.unwrap();
        let out = list2.pop_front().await.unwrap().unwrap();
        assert_eq!(&out[..6], b"shared");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn reopen_persists_data() {
        let path = tmp("reopen");
        {
            let list = List::open(&path).await.unwrap();
            list.push_front(b"persisted".to_vec()).await.unwrap();
        }
        {
            let list = List::open(&path).await.unwrap();
            let data = list.pop_front().await.unwrap().unwrap();
            assert_eq!(&data[..9], b"persisted");
            assert_eq!(list.pop_front().await.unwrap(), None);
        }
        let _ = std::fs::remove_file(&path);
    }
}
