//! Little-endian byte cursors shared by records and snapshots.

/// The input ended early or carried a tag the decoder does not know.
#[derive(Clone, Copy, PartialEq, Eq, Debug, thiserror::Error)]
pub enum CodecError {
    #[error("input ended at byte {0}")]
    Truncated(usize),
    #[error("unknown tag {tag} at byte {at}")]
    BadTag { tag: u8, at: usize },
}

pub(crate) fn put_u8(buf: &mut Vec<u8>, v: u8) {
    buf.push(v);
}

pub(crate) fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

/// Length-prefixed bytes. Payloads are capped by the gRPC message limit, far below `u32::MAX`.
pub(crate) fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    put_u32(
        buf,
        u32::try_from(bytes.len()).expect("byte string longer than u32::MAX"),
    );
    buf.extend_from_slice(bytes);
}

/// A presence byte, then the value if present.
pub(crate) fn put_opt_u32(buf: &mut Vec<u8>, v: Option<u32>) {
    match v {
        Some(x) => {
            put_u8(buf, 1);
            put_u32(buf, x);
        }
        None => put_u8(buf, 0),
    }
}

/// A read position over a byte slice. Every read is bounds-checked.
pub(crate) struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    pub(crate) const fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub(crate) const fn position(&self) -> usize {
        self.pos
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        // `checked_add` guards the arithmetic; `get` guards the slice.
        let end = self
            .pos
            .checked_add(n)
            .ok_or(CodecError::Truncated(self.pos))?;
        let bytes = self
            .buf
            .get(self.pos..end)
            .ok_or(CodecError::Truncated(self.pos))?;
        self.pos = end;
        Ok(bytes)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u32(&mut self) -> Result<u32, CodecError> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes(b.try_into().expect("four bytes")))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, CodecError> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes(b.try_into().expect("eight bytes")))
    }

    pub(crate) fn bytes(&mut self) -> Result<&'a [u8], CodecError> {
        let n = self.u32()? as usize;
        self.take(n)
    }

    pub(crate) fn opt_u32(&mut self) -> Result<Option<u32>, CodecError> {
        match self.tag(1)? {
            0 => Ok(None),
            _ => Ok(Some(self.u32()?)),
        }
    }

    /// A tag byte that must not exceed `max`, reported with its offset when it does.
    pub(crate) fn tag(&mut self, max: u8) -> Result<u8, CodecError> {
        let at = self.pos;
        let tag = self.u8()?;
        if tag > max {
            return Err(CodecError::BadTag { tag, at });
        }
        Ok(tag)
    }
}
