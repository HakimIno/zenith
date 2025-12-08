//! PostgreSQL replication stream decoder

use super::pgoutput_parser::{PgOutputMessage, PgOutputParser};
use crate::error::Result;
use bytes::{Buf, BytesMut};
use std::time::Instant;
use tracing::trace;

/// XLogData message from replication stream
#[derive(Debug, Clone)]
pub struct XLogData {
    /// Start LSN of this WAL data
    pub start_lsn: u64,
    /// End LSN of this WAL data
    pub end_lsn: u64,
    /// Server send time (microseconds since 2000-01-01)
    pub send_time: i64,
    /// WAL data payload
    pub data: Vec<u8>,
}

/// Primary keepalive message from server
#[derive(Debug, Clone)]
pub struct PrimaryKeepalive {
    /// Current end of WAL on the server
    pub wal_end: u64,
    /// Server time
    pub send_time: i64,
    /// Whether the client should reply immediately
    pub reply_requested: bool,
}

/// Decoded replication message
#[derive(Debug)]
pub enum ReplicationMessage {
    /// XLogData containing pgoutput message
    XLogData(XLogData),
    /// Server keepalive
    PrimaryKeepalive(PrimaryKeepalive),
}

/// Replication stream decoder
///
/// Decodes the PostgreSQL streaming replication protocol messages
/// and parses the contained pgoutput data.
pub struct ReplicationDecoder {
    parser: PgOutputParser,
    #[allow(dead_code)]
    buffer: BytesMut,
    stats: DecoderStats,
}

#[derive(Debug, Default)]
pub struct DecoderStats {
    pub messages_decoded: u64,
    pub xlogdata_count: u64,
    pub keepalive_count: u64,
    pub bytes_processed: u64,
    pub parse_time_ns: u64,
}

impl ReplicationDecoder {
    /// Create a new decoder
    pub fn new() -> Self {
        Self {
            parser: PgOutputParser::new(),
            buffer: BytesMut::with_capacity(64 * 1024), // 64KB initial buffer
            stats: DecoderStats::default(),
        }
    }

    /// Decode a raw replication message
    pub fn decode_replication_message(&mut self, data: &[u8]) -> Result<ReplicationMessage> {
        if data.is_empty() {
            return Err(crate::Error::invalid_message("Empty replication message"));
        }

        self.stats.messages_decoded += 1;
        self.stats.bytes_processed += data.len() as u64;

        let msg_type = data[0];

        match msg_type {
            b'w' => {
                // XLogData (WAL data)
                self.stats.xlogdata_count += 1;
                self.decode_xlogdata(&data[1..])
            }
            b'k' => {
                // Primary keepalive
                self.stats.keepalive_count += 1;
                self.decode_keepalive(&data[1..])
            }
            _ => Err(crate::Error::invalid_message(format!(
                "Unknown replication message type: {} (0x{:02x})",
                msg_type as char, msg_type
            ))),
        }
    }

    /// Decode XLogData message
    fn decode_xlogdata(&self, data: &[u8]) -> Result<ReplicationMessage> {
        if data.len() < 24 {
            return Err(crate::Error::invalid_message(
                "XLogData message too short",
            ));
        }

        let mut buf = data;
        let start_lsn = buf.get_u64();
        let end_lsn = buf.get_u64();
        let send_time = buf.get_i64();
        let wal_data = buf.to_vec();

        trace!(
            "Decoded XLogData: start_lsn={:X}/{:X}, end_lsn={:X}/{:X}, data_len={}",
            (start_lsn >> 32) as u32,
            start_lsn as u32,
            (end_lsn >> 32) as u32,
            end_lsn as u32,
            wal_data.len()
        );

        Ok(ReplicationMessage::XLogData(XLogData {
            start_lsn,
            end_lsn,
            send_time,
            data: wal_data,
        }))
    }

    /// Decode Primary keepalive message
    fn decode_keepalive(&self, data: &[u8]) -> Result<ReplicationMessage> {
        if data.len() < 17 {
            return Err(crate::Error::invalid_message(
                "Keepalive message too short",
            ));
        }

        let mut buf = data;
        let wal_end = buf.get_u64();
        let send_time = buf.get_i64();
        let reply_requested = buf.get_u8() != 0;

        trace!(
            "Decoded Keepalive: wal_end={:X}/{:X}, reply_requested={}",
            (wal_end >> 32) as u32,
            wal_end as u32,
            reply_requested
        );

        Ok(ReplicationMessage::PrimaryKeepalive(PrimaryKeepalive {
            wal_end,
            send_time,
            reply_requested,
        }))
    }

    /// Parse pgoutput message from XLogData payload
    pub fn parse_pgoutput(&mut self, data: &[u8]) -> Result<PgOutputMessage> {
        let start = Instant::now();
        let result = self.parser.parse(data);
        self.stats.parse_time_ns += start.elapsed().as_nanos() as u64;
        result
    }

    /// Create a standby status update message
    ///
    /// This message is sent to PostgreSQL to acknowledge receipt of WAL data.
    pub fn create_standby_status_update(
        write_lsn: u64,
        flush_lsn: u64,
        apply_lsn: u64,
        timestamp: i64,
        reply_requested: bool,
    ) -> Vec<u8> {
        let mut msg = Vec::with_capacity(34);
        msg.push(b'r'); // Standby status update

        // Write positions
        msg.extend_from_slice(&write_lsn.to_be_bytes());
        msg.extend_from_slice(&flush_lsn.to_be_bytes());
        msg.extend_from_slice(&apply_lsn.to_be_bytes());

        // Timestamp
        msg.extend_from_slice(&timestamp.to_be_bytes());

        // Reply flag
        msg.push(if reply_requested { 1 } else { 0 });

        msg
    }

    /// Get current decoder statistics
    pub fn stats(&self) -> &DecoderStats {
        &self.stats
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats = DecoderStats::default();
    }
}

impl Default for ReplicationDecoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert PostgreSQL timestamp to Unix timestamp
///
/// PostgreSQL uses microseconds since 2000-01-01 00:00:00 UTC
pub fn pg_timestamp_to_unix(pg_ts: i64) -> i64 {
    // Seconds from 2000-01-01 to 1970-01-01
    const EPOCH_DIFF: i64 = 946_684_800;
    (pg_ts / 1_000_000) + EPOCH_DIFF
}

/// Convert Unix timestamp to PostgreSQL timestamp
pub fn unix_to_pg_timestamp(unix_ts: i64) -> i64 {
    const EPOCH_DIFF: i64 = 946_684_800;
    (unix_ts - EPOCH_DIFF) * 1_000_000
}

/// Get current time as PostgreSQL timestamp
pub fn current_pg_timestamp() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;
    now - (946_684_800 * 1_000_000)
}

/// Format LSN as PostgreSQL-style hex string
pub fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

/// Parse LSN from PostgreSQL-style hex string
pub fn parse_lsn(s: &str) -> Result<u64> {
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() != 2 {
        return Err(crate::Error::parse(format!("Invalid LSN format: {}", s)));
    }

    let high = u32::from_str_radix(parts[0], 16)
        .map_err(|e| crate::Error::parse(format!("Invalid LSN high part: {}", e)))?;
    let low = u32::from_str_radix(parts[1], 16)
        .map_err(|e| crate::Error::parse(format!("Invalid LSN low part: {}", e)))?;

    Ok(((high as u64) << 32) | (low as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_lsn() {
        assert_eq!(format_lsn(0x0000000100000000), "1/0");
        assert_eq!(format_lsn(0x0000000100000001), "1/1");
        assert_eq!(format_lsn(0x00000001ABCDEF00), "1/ABCDEF00");
    }

    #[test]
    fn test_parse_lsn() {
        assert_eq!(parse_lsn("1/0").unwrap(), 0x0000000100000000);
        assert_eq!(parse_lsn("1/1").unwrap(), 0x0000000100000001);
        assert_eq!(parse_lsn("1/ABCDEF00").unwrap(), 0x00000001ABCDEF00);
    }

    #[test]
    fn test_timestamp_conversion() {
        let pg_ts = 1700000000000000i64; // Some PostgreSQL timestamp
        let unix_ts = pg_timestamp_to_unix(pg_ts);
        let back = unix_to_pg_timestamp(unix_ts);
        // We lose microsecond precision in the round-trip
        assert!((pg_ts - back).abs() < 1_000_000);
    }

    #[test]
    fn test_standby_status_update() {
        let msg = ReplicationDecoder::create_standby_status_update(
            0x100000000,
            0x100000000,
            0x100000000,
            0,
            false,
        );
        assert_eq!(msg[0], b'r');
        assert_eq!(msg.len(), 34);
    }
}

