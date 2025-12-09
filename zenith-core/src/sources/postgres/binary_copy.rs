use crate::error::{Error, Result};
use bytes::{Buf, BytesMut};
use futures::{Stream, StreamExt};
use tracing::trace;

/// PostgreSQL Binary Copy Header Signature
const BINARY_SIGNATURE: &[u8] = b"PGCOPY\n\xFF\r\n\0";
const BINARY_SIGNATURE_LEN: usize = 11;

/// Flags field length (32-bit integer)
const FLAGS_LEN: usize = 4;
/// Header extension area length field (32-bit integer)
const EXTENSION_AREA_LEN_FIELD: usize = 4;

/// Parser for PostgreSQL Binary Copy format
pub struct BinaryCopyParser<S> {
    stream: S,
    buffer: BytesMut,
    header_read: bool,
}

impl<S, E> BinaryCopyParser<S> 
where 
    S: Stream<Item = std::result::Result<bytes::Bytes, E>> + Unpin,
    E: std::fmt::Display
{
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            buffer: BytesMut::with_capacity(8192),
            header_read: false,
        }
    }

    /// Read required bytes into buffer
    async fn read_at_least(&mut self, size: usize) -> Result<()> {
        while self.buffer.len() < size {
            match self.stream.next().await {
                Some(Ok(chunk)) => self.buffer.extend_from_slice(&chunk),
                Some(Err(e)) => return Err(Error::Config(format!("Snapshot stream error: {}", e))),
                None => return Err(Error::Config("Unexpected EOF in snapshot stream".into())),
            }
        }
        Ok(())
    }

    /// Read and verify the file header
    pub async fn read_header(&mut self) -> Result<()> {
        if self.header_read {
            return Ok(());
        }

        self.read_at_least(BINARY_SIGNATURE_LEN).await?;
        let signature = self.buffer.split_to(BINARY_SIGNATURE_LEN);
        
        if &signature[..] != BINARY_SIGNATURE {
            return Err(Error::parse("Invalid binary copy signature"));
        }

        self.read_at_least(FLAGS_LEN).await?;
        let flags_val = self.buffer.get_u32(); // Big Endian by default for get_u32 in bytes 1.x? No, bytes::Buf::get_u32 is BigEndian by default
        
        // Critical flag check: OID inclusion (bit 16)
        if (flags_val & (1 << 16)) != 0 {
            return Err(Error::parse("Binary copy with OIDs not supported"));
        }

        self.read_at_least(EXTENSION_AREA_LEN_FIELD).await?;
        let ext_len = self.buffer.get_u32() as usize;

        // Skip extension area
        if ext_len > 0 {
            self.read_at_least(ext_len).await?;
            let _ = self.buffer.split_to(ext_len);
        }

        self.header_read = true;
        trace!("Binary Copy Header verified");
        Ok(())
    }

    /// Read next tuple
    /// Returns Ok(Some(Vec<Option<Vec<u8>>>)) for a row
    /// Returns Ok(None) for end of stream
    pub async fn next_row(&mut self) -> Result<Option<Vec<Option<Vec<u8>>>>> {
        if !self.header_read {
            self.read_header().await?;
        }

        // Tuple field count (int16)
        self.read_at_least(2).await?;
        let field_count = self.buffer.get_i16();

        if field_count == -1 {
            // Trailer reached
            return Ok(None);
        }

        let count = field_count as usize;
        let mut row = Vec::with_capacity(count);

        for _ in 0..count {
            // Field length (int32)
            self.read_at_least(4).await?;
            let len = self.buffer.get_i32();

            if len == -1 {
                row.push(None); // NULL
            } else if len < 0 {
                return Err(Error::parse(format!("Invalid field length: {}", len)));
            } else {
                let len = len as usize;
                self.read_at_least(len).await?;
                let data = self.buffer.split_to(len);
                row.push(Some(data.to_vec()));
            }
        }

        Ok(Some(row))
    }
}
