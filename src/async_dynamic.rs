//! Async wrapper around [`DynamicBlockList`].
//!
//! Enabled with the `async` feature flag.  Each method offloads the blocking
//! file I/O onto Tokio's blocking-thread pool via
//! [`tokio::task::spawn_blocking`] and returns a future that resolves once the
//! I/O completes.

use std::io;
use std::path::Path;
use std::sync::Arc;

use crate::dynamic::DynamicBlockList;
use crate::{DynBlockRef, Error};

/// Async wrapper around [`DynamicBlockList`].
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
/// use bllist::AsyncDynamicBlockList;
///
/// // Block sizes are power-of-two; a 5-byte push → 32-byte block (bin 5).
/// let list = AsyncDynamicBlockList::open("data.blld").await?;
///
/// list.push_front(b"short").await?;
/// list.push_front(b"a somewhat longer record").await?;
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
/// `AsyncDynamicBlockList` is `Send + Sync`.  Concurrent calls are safe;
/// header mutations are serialised through the inner `Mutex` of
/// [`DynamicBlockList`].
///
/// # Crash safety
///
/// The same guarantees as [`DynamicBlockList`] apply: every mutation flushes
/// durably before returning.  If the blocking thread is killed mid-operation
/// the worst case is one orphaned block, reclaimed on the next
/// [`open`](Self::open).
///
/// # Panics
///
/// If the spawned blocking task panics, the async method returns
/// [`Error::Io`] wrapping the panic message.
#[derive(Clone)]
pub struct AsyncDynamicBlockList(Arc<DynamicBlockList>);

impl AsyncDynamicBlockList {
    // ── constructor ───────────────────────────────────────────────────────────

    /// Open or create the file at `path` as an [`AsyncDynamicBlockList`].
    ///
    /// Runs [`DynamicBlockList::open`] on a Tokio blocking thread.  See that
    /// method for full open semantics, crash recovery, and possible errors.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref().to_path_buf();
        tokio::task::spawn_blocking(move || DynamicBlockList::open(&path))
            .await
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e.to_string())))?
            .map(|list| Self(Arc::new(list)))
    }

    // ── accessors ─────────────────────────────────────────────────────────────

    /// Return the smallest power-of-two total block size that holds `size`
    /// payload bytes.
    ///
    /// Delegates to [`DynamicBlockList::block_size_for`]; no I/O performed.
    pub const fn block_size_for(size: usize) -> usize {
        DynamicBlockList::block_size_for(size)
    }

    /// Return a reference to the underlying synchronous [`DynamicBlockList`].
    ///
    /// Useful for direct BStack streaming reads (see
    /// [`DynamicBlockList::bstack`]).  Do **not** call mutating methods on
    /// the inner list while an async operation is in flight.
    pub fn inner(&self) -> &DynamicBlockList {
        &self.0
    }

    // ── allocation ────────────────────────────────────────────────────────────

    /// Async version of [`DynamicBlockList::alloc`].
    pub async fn alloc(&self, size: usize) -> Result<DynBlockRef, Error> {
        let inner = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || inner.alloc(size))
            .await
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e.to_string())))?
    }

    /// Async version of [`DynamicBlockList::free`].
    pub async fn free(&self, block: DynBlockRef) -> Result<(), Error> {
        let inner = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || inner.free(block))
            .await
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e.to_string())))?
    }

    // ── payload I/O ───────────────────────────────────────────────────────────

    /// Async version of [`DynamicBlockList::write`].
    ///
    /// `data` must be `Send + 'static` (e.g. `Vec<u8>`, `Box<[u8]>`, or a
    /// `&'static [u8]`).  No extra copy is made when the caller provides
    /// owned data.
    pub async fn write(
        &self,
        block: DynBlockRef,
        data: impl AsRef<[u8]> + Send + 'static,
    ) -> Result<(), Error> {
        let inner = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || inner.write(block, data.as_ref()))
            .await
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e.to_string())))?
    }

    /// Async version of [`DynamicBlockList::read`].
    pub async fn read(&self, block: DynBlockRef) -> Result<Vec<u8>, Error> {
        let inner = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || inner.read(block))
            .await
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e.to_string())))?
    }

    // ── structural pointer operations ─────────────────────────────────────────

    /// Async version of [`DynamicBlockList::set_next`].
    pub async fn set_next(
        &self,
        block: DynBlockRef,
        next: Option<DynBlockRef>,
    ) -> Result<(), Error> {
        let inner = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || inner.set_next(block, next))
            .await
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e.to_string())))?
    }

    /// Async version of [`DynamicBlockList::get_next`].
    pub async fn get_next(&self, block: DynBlockRef) -> Result<Option<DynBlockRef>, Error> {
        let inner = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || inner.get_next(block))
            .await
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e.to_string())))?
    }

    // ── list head ─────────────────────────────────────────────────────────────

    /// Async version of [`DynamicBlockList::root`].
    pub async fn root(&self) -> Result<Option<DynBlockRef>, Error> {
        let inner = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || inner.root())
            .await
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e.to_string())))?
    }

    // ── block metadata ────────────────────────────────────────────────────────

    /// Async version of [`DynamicBlockList::capacity`].
    pub async fn capacity(&self, block: DynBlockRef) -> Result<usize, Error> {
        let inner = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || inner.capacity(block))
            .await
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e.to_string())))?
    }

    /// Async version of [`DynamicBlockList::data_len`].
    pub async fn data_len(&self, block: DynBlockRef) -> Result<usize, Error> {
        let inner = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || inner.data_len(block))
            .await
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e.to_string())))?
    }

    /// Async version of [`DynamicBlockList::data_end`].
    pub async fn data_end(&self, block: DynBlockRef) -> Result<u64, Error> {
        let inner = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || inner.data_end(block))
            .await
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e.to_string())))?
    }

    // ── convenience list operations ───────────────────────────────────────────

    /// Async version of [`DynamicBlockList::push_front`].
    ///
    /// `data` must be `Send + 'static` (e.g. `Vec<u8>`, `Box<[u8]>`, or a
    /// `&'static [u8]`).
    pub async fn push_front(
        &self,
        data: impl AsRef<[u8]> + Send + 'static,
    ) -> Result<DynBlockRef, Error> {
        let inner = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || inner.push_front(data.as_ref()))
            .await
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e.to_string())))?
    }

    /// Async version of [`DynamicBlockList::pop_front`].
    pub async fn pop_front(&self) -> Result<Option<Vec<u8>>, Error> {
        let inner = Arc::clone(&self.0);
        tokio::task::spawn_blocking(move || inner.pop_front())
            .await
            .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::Other, e.to_string())))?
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn tmp(label: &str) -> std::path::PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "bllist_async_dyn_{}_{}_{}.blld",
            std::process::id(),
            label,
            n
        ));
        p
    }

    #[tokio::test]
    async fn fresh_open_empty() {
        let path = tmp("fresh");
        let list = AsyncDynamicBlockList::open(&path).await.unwrap();
        assert_eq!(list.root().await.unwrap(), None);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn block_size_for_values() {
        assert_eq!(AsyncDynamicBlockList::block_size_for(0), 32);
        assert_eq!(AsyncDynamicBlockList::block_size_for(12), 32);
        assert_eq!(AsyncDynamicBlockList::block_size_for(13), 64);
        assert_eq!(AsyncDynamicBlockList::block_size_for(44), 64);
    }

    #[tokio::test]
    async fn alloc_free_reuse() {
        let path = tmp("alloc");
        let list = AsyncDynamicBlockList::open(&path).await.unwrap();

        let b0 = list.alloc(1).await.unwrap();
        let b1 = list.alloc(1).await.unwrap();
        list.free(b1).await.unwrap();
        let b2 = list.alloc(1).await.unwrap();
        assert_eq!(b2, b1);

        let _ = (b0, b2);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn write_read_roundtrip() {
        let path = tmp("rw");
        let list = AsyncDynamicBlockList::open(&path).await.unwrap();
        let block = list.alloc(14).await.unwrap();

        list.write(block, b"hello dynamic!".to_vec()).await.unwrap();
        let out = list.read(block).await.unwrap();
        assert_eq!(out, b"hello dynamic!");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn capacity_and_data_len() {
        let path = tmp("meta");
        let list = AsyncDynamicBlockList::open(&path).await.unwrap();
        let block = list.alloc(12).await.unwrap();

        assert_eq!(list.capacity(block).await.unwrap(), 12);
        assert_eq!(list.data_len(block).await.unwrap(), 0);
        list.write(block, b"five!".to_vec()).await.unwrap();
        assert_eq!(list.data_len(block).await.unwrap(), 5);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn data_end_offset() {
        let path = tmp("data_end");
        let list = AsyncDynamicBlockList::open(&path).await.unwrap();
        let block = list.alloc(12).await.unwrap();
        list.write(block, b"abc".to_vec()).await.unwrap();

        let start = block.data_start();
        let end = list.data_end(block).await.unwrap();
        assert_eq!(end - start, 3);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn set_get_next() {
        let path = tmp("next");
        let list = AsyncDynamicBlockList::open(&path).await.unwrap();
        let b0 = list.alloc(1).await.unwrap();
        let b1 = list.alloc(1).await.unwrap();

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
        let list = AsyncDynamicBlockList::open(&path).await.unwrap();

        list.push_front(b"first".to_vec()).await.unwrap();
        list.push_front(b"second longer".to_vec()).await.unwrap();
        list.push_front(b"third".to_vec()).await.unwrap();

        assert_eq!(list.pop_front().await.unwrap().unwrap(), b"third");
        assert_eq!(list.pop_front().await.unwrap().unwrap(), b"second longer");
        assert_eq!(list.pop_front().await.unwrap().unwrap(), b"first");
        assert_eq!(list.pop_front().await.unwrap(), None);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn push_static_bytes() {
        let path = tmp("static");
        let list = AsyncDynamicBlockList::open(&path).await.unwrap();
        list.push_front(b"static data").await.unwrap();
        let out = list.pop_front().await.unwrap().unwrap();
        assert_eq!(out, b"static data");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn push_mixed_sizes() {
        let path = tmp("mixed");
        let list = AsyncDynamicBlockList::open(&path).await.unwrap();

        list.push_front(vec![0u8; 1]).await.unwrap();
        list.push_front(vec![1u8; 100]).await.unwrap();
        list.push_front(vec![2u8; 10]).await.unwrap();

        assert_eq!(list.pop_front().await.unwrap().unwrap(), vec![2u8; 10]);
        assert_eq!(list.pop_front().await.unwrap().unwrap(), vec![1u8; 100]);
        assert_eq!(list.pop_front().await.unwrap().unwrap(), vec![0u8; 1]);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn pop_empty() {
        let path = tmp("pop_empty");
        let list = AsyncDynamicBlockList::open(&path).await.unwrap();
        assert_eq!(list.pop_front().await.unwrap(), None);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn clone_shared_state() {
        let path = tmp("clone");
        let list = AsyncDynamicBlockList::open(&path).await.unwrap();
        let list2 = list.clone();

        list.push_front(b"shared state".to_vec()).await.unwrap();
        let out = list2.pop_front().await.unwrap().unwrap();
        assert_eq!(out, b"shared state");

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn reopen_persists_data() {
        let path = tmp("reopen");
        {
            let list = AsyncDynamicBlockList::open(&path).await.unwrap();
            list.push_front(b"persisted data".to_vec()).await.unwrap();
        }
        {
            let list = AsyncDynamicBlockList::open(&path).await.unwrap();
            let data = list.pop_front().await.unwrap().unwrap();
            assert_eq!(data, b"persisted data");
            assert_eq!(list.pop_front().await.unwrap(), None);
        }
        let _ = std::fs::remove_file(&path);
    }
}
