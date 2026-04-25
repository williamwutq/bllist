use std::marker::PhantomData;

use bstack::BStack;

use crate::Error;

/// Describes the layout of a block's header region between the checksum
/// and the payload.
///
/// Every block has the on-disk form:
///
/// ```text
/// checksum(4) | header_content(HEADER_CONTENT_SIZE) | payload(payload_cap)
/// ```
///
/// The CRC32 checksum covers all bytes after the first four —
/// `header_content` plus the **full** `payload_cap` bytes (zero-padded
/// beyond the written data).  The meaning of `header_content` is defined
/// by the implementing type; this trait only encodes its byte count.
pub trait BlockLayout {
    /// Bytes in the header between the 4-byte checksum and the payload.
    ///
    /// These bytes are always included in the CRC32.
    const HEADER_CONTENT_SIZE: usize;
}

// ── Block ─────────────────────────────────────────────────────────────────────

/// A typed handle to a single block at a known offset in a [`BStack`] file.
///
/// `Block<L>` knows two things: **where** it lives (`offset`) and **how large
/// its fixed header is** (`L::HEADER_CONTENT_SIZE`).  Payload capacity is not
/// encoded in the type — it varies per block (dynamic lists) or per list type
/// (fixed lists) and must be supplied by the caller.
pub(crate) struct Block<L: BlockLayout> {
    pub(crate) offset: u64,
    _l: PhantomData<L>,
}

#[allow(unused)]
impl<L: BlockLayout> Block<L> {
    #[inline]
    pub(crate) fn new(offset: u64) -> Self {
        Self {
            offset,
            _l: PhantomData,
        }
    }

    /// Byte offset of the first payload byte. Pure, no I/O.
    #[inline]
    pub(crate) fn payload_start(&self) -> u64 {
        self.offset + 4 + L::HEADER_CONTENT_SIZE as u64
    }

    // ── writes ────────────────────────────────────────────────────────────────

    /// Write a full block with CRC stamp.
    ///
    /// On-disk layout: `[checksum(4)][header_content][data][zeros to payload_cap]`.
    /// CRC covers everything after the first 4 bytes.
    ///
    /// Returns [`Error::DataTooLarge`] if `data.len() > payload_cap`.
    pub(crate) fn write(
        &self,
        stack: &BStack,
        header_content: &[u8],
        payload_cap: usize,
        data: &[u8],
    ) -> Result<(), Error> {
        debug_assert_eq!(header_content.len(), L::HEADER_CONTENT_SIZE);
        if data.len() > payload_cap {
            return Err(Error::DataTooLarge {
                capacity: payload_cap,
                provided: data.len(),
            });
        }
        let total = 4 + L::HEADER_CONTENT_SIZE + payload_cap;
        let mut buf = vec![0u8; total];
        buf[4..4 + L::HEADER_CONTENT_SIZE].copy_from_slice(header_content);
        let pay_start = 4 + L::HEADER_CONTENT_SIZE;
        buf[pay_start..pay_start + data.len()].copy_from_slice(data);
        write_checksum(&mut buf);
        stack.set(self.offset, &buf)?;
        Ok(())
    }

    /// Write a block with CRC stamp while make no change to the header content.
    /// The payload is zero-padded to `payload_cap` bytes.
    ///
    /// Returns [`Error::DataTooLarge`] if `data.len() > payload_cap`.
    pub(crate) fn write_payload(
        &self,
        stack: &BStack,
        payload_cap: usize,
        data: &[u8],
    ) -> Result<(), Error> {
        debug_assert!(payload_cap > 0);
        if data.len() > payload_cap {
            return Err(Error::DataTooLarge {
                capacity: payload_cap,
                provided: data.len(),
            });
        }
        let total = 4 + L::HEADER_CONTENT_SIZE + payload_cap;
        let mut buf = stack.get(self.offset, self.offset + total as u64)?;
        if buf.len() != total {
            return Err(Error::InvalidBlock);
        }
        verify_checksum(&buf, self.offset)?;
        let pay_start = 4 + L::HEADER_CONTENT_SIZE;
        buf[pay_start..pay_start + data.len()].copy_from_slice(data);
        for b in &mut buf[pay_start + data.len()..] {
            *b = 0;
        }
        write_checksum(&mut buf);
        stack.set(self.offset, &buf)?;
        Ok(())
    }

    /// Write a block with CRC stamp while make no change to the header content.
    /// No zero-padding is done to the payload.
    pub(crate) fn write_payload_nopad(&self, stack: &BStack, data: &[u8]) -> Result<(), Error> {
        debug_assert!(!data.is_empty());
        let total = 4 + L::HEADER_CONTENT_SIZE + data.len();
        let mut buf = stack.get(self.offset, self.offset + total as u64)?;
        if buf.len() != total {
            return Err(Error::InvalidBlock);
        }
        verify_checksum(&buf, self.offset)?;
        let pay_start = 4 + L::HEADER_CONTENT_SIZE;
        buf[pay_start..pay_start + data.len()].copy_from_slice(data);
        write_checksum(&mut buf);
        stack.set(self.offset, &buf)?;
        Ok(())
    }

    // ── reads (unchecked) ─────────────────────────────────────────────────────

    /// Read the `HEADER_CONTENT_SIZE` bytes that follow the checksum.
    /// No CRC verification.
    pub(crate) fn read_header_unchecked(&self, stack: &BStack) -> Result<Vec<u8>, Error> {
        Ok(stack.get(
            self.offset + 4,
            self.offset + 4 + L::HEADER_CONTENT_SIZE as u64,
        )?)
    }

    /// Read the payload bytes that follow the header content, up to `payload_cap`.
    /// No CRC verification.
    pub(crate) fn read_payload_unchecked(
        &self,
        stack: &BStack,
        payload_cap: usize,
    ) -> Result<Vec<u8>, Error> {
        Ok(stack.get(
            self.offset + 4,
            self.offset + 4 + (L::HEADER_CONTENT_SIZE + payload_cap) as u64,
        )?)
    }

    pub(crate) fn read_payload_into_unchecked(
        &self,
        stack: &BStack,
        payload_cap: usize,
        buf: &mut [u8],
    ) -> Result<(), Error> {
        debug_assert!(payload_cap > 0);
        if buf.len() > payload_cap {
            return Err(Error::DataTooLarge {
                capacity: payload_cap,
                provided: buf.len(),
            });
        }
        let total = 4 + L::HEADER_CONTENT_SIZE + payload_cap;
        let full = stack.get(self.offset, self.offset + total as u64)?;
        // The length check is dropped since it's impossible to fail since bstack's reads are guaranteed to return the requested length or an error.
        let pay_start = 4 + L::HEADER_CONTENT_SIZE;
        buf.copy_from_slice(&full[pay_start..pay_start + buf.len()]);
        Ok(())
    }

    /// Read the first `buf.len()` payload bytes from `start` offset from the block's header or payload start.
    /// No CRC verification.
    pub(crate) fn read_at_into(
        &self,
        stack: &BStack,
        start: usize,
        buf: &mut [u8],
    ) -> Result<(), Error> {
        Ok(stack.get_into(self.offset + 4 + start as u64, buf)?)
    }

    /// Read the first `buf.len()` payload bytes from `start` offset from the block's header or payload start.
    /// No CRC verification.
    pub(crate) fn read_at(
        &self,
        stack: &BStack,
        start: usize,
        size: usize,
    ) -> Result<Vec<u8>, Error> {
        Ok(stack.get(
            self.offset + 4 + start as u64,
            self.offset + 4 + (start + size) as u64,
        )?)
    }

    /// CRC-verify the block, replace `header_content` with `new_hc`, and write back.
    /// The payload bytes are preserved unchanged.
    pub(crate) fn update_header(
        &self,
        stack: &BStack,
        new_hc: &[u8],
        payload_cap: usize,
    ) -> Result<(), Error> {
        debug_assert_eq!(new_hc.len(), L::HEADER_CONTENT_SIZE);
        let total = 4 + L::HEADER_CONTENT_SIZE + payload_cap;
        let mut buf = stack.get(self.offset, self.offset + total as u64)?;
        if buf.len() != total {
            return Err(Error::InvalidBlock);
        }
        verify_checksum(&buf, self.offset)?;
        buf[4..4 + L::HEADER_CONTENT_SIZE].copy_from_slice(new_hc);
        write_checksum(&mut buf);
        stack.set(self.offset, &buf)?;
        Ok(())
    }

    // ── reads (CRC-verified) ──────────────────────────────────────────────────

    /// Read and CRC-verify the full block.
    ///
    /// Returns `(header_content, payload)` where `payload` is exactly
    /// `payload_cap` bytes (zero-padded tail included).
    pub(crate) fn read(
        &self,
        stack: &BStack,
        payload_cap: usize,
    ) -> Result<(Vec<u8>, Vec<u8>), Error> {
        let total = 4 + L::HEADER_CONTENT_SIZE + payload_cap;
        let buf = stack.get(self.offset, self.offset + total as u64)?;
        if buf.len() != total {
            return Err(Error::InvalidBlock);
        }
        verify_checksum(&buf, self.offset)?;
        let hc = buf[4..4 + L::HEADER_CONTENT_SIZE].to_vec();
        let payload = buf[4 + L::HEADER_CONTENT_SIZE..].to_vec();
        Ok((hc, payload))
    }

    /// Zero-copy read: fill `buf` with the first `buf.len()` payload bytes
    /// after CRC-verifying the full block. Returns `header_content`.
    ///
    /// Returns [`Error::DataTooLarge`] if `buf.len() > payload_cap`.
    pub(crate) fn read_payload_into(
        &self,
        stack: &BStack,
        payload_cap: usize,
        buf: &mut [u8],
    ) -> Result<Vec<u8>, Error> {
        if buf.len() > payload_cap {
            return Err(Error::DataTooLarge {
                capacity: payload_cap,
                provided: buf.len(),
            });
        }
        let total = 4 + L::HEADER_CONTENT_SIZE + payload_cap;
        let full = stack.get(self.offset, self.offset + total as u64)?;
        if full.len() != total {
            return Err(Error::InvalidBlock);
        }
        verify_checksum(&full, self.offset)?;
        let hc = full[4..4 + L::HEADER_CONTENT_SIZE].to_vec();
        let pay_start = 4 + L::HEADER_CONTENT_SIZE;
        buf.copy_from_slice(&full[pay_start..pay_start + buf.len()]);
        Ok(hc)
    }

    /// CRC-verify the block without returning any data.
    pub(crate) fn verify(&self, stack: &BStack, payload_cap: usize) -> Result<(), Error> {
        let total = 4 + L::HEADER_CONTENT_SIZE + payload_cap;
        let buf = stack.get(self.offset, self.offset + total as u64)?;
        if buf.len() != total {
            return Err(Error::InvalidBlock);
        }
        verify_checksum(&buf, self.offset)
    }
}

// ── low-level checksum helpers ────────────────────────────────────────────────

/// Write CRC32 of `buf[4..]` into `buf[0..4]`.
#[inline]
pub(crate) fn write_checksum(buf: &mut [u8]) {
    let crc = crc32fast::hash(&buf[4..]);
    buf[0..4].copy_from_slice(&crc.to_le_bytes());
}

/// Verify that `buf[0..4]` matches CRC32 of `buf[4..]`.
///
/// Returns `Err(ChecksumMismatch { block: offset })` on mismatch.
#[inline]
pub(crate) fn verify_checksum(buf: &[u8], offset: u64) -> Result<(), Error> {
    let stored = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    if crc32fast::hash(&buf[4..]) != stored {
        return Err(Error::ChecksumMismatch { block: offset });
    }
    Ok(())
}
