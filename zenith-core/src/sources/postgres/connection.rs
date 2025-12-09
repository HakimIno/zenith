//! PostgreSQL low-level connection handling
//!
//! This module handles TCP connection, protocol handshake, authentication,
//! and basic query execution using the raw PostgreSQL protocol.

use crate::error::{Error, Result};
use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tracing::{debug, trace};

/// Connection parameters parsed from URL
#[derive(Debug, Clone)]
pub struct ConnectionParams {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
}

impl ConnectionParams {
    pub fn from_url(url: &str) -> Result<Self> {
        // Parse postgres://user:pass@host:port/database
        let url = url.trim_start_matches("postgres://").trim_start_matches("postgresql://");
        
        let (userinfo, rest) = if let Some(at_pos) = url.find('@') {
            (&url[..at_pos], &url[at_pos + 1..])
        } else {
            ("", url)
        };

        let (user, password) = if let Some(colon_pos) = userinfo.find(':') {
            (
                userinfo[..colon_pos].to_string(),
                Some(userinfo[colon_pos + 1..].to_string()),
            )
        } else {
            (userinfo.to_string(), None)
        };

        // Remove query parameters
        let rest = rest.split('?').next().unwrap_or(rest);

        let (hostport, database) = if let Some(slash_pos) = rest.find('/') {
            (&rest[..slash_pos], rest[slash_pos + 1..].to_string())
        } else {
            (rest, "postgres".to_string())
        };

        let (host, port) = if let Some(colon_pos) = hostport.find(':') {
            (
                hostport[..colon_pos].to_string(),
                hostport[colon_pos + 1..].parse().unwrap_or(5432),
            )
        } else {
            (hostport.to_string(), 5432)
        };

        Ok(Self {
            host,
            port,
            user: if user.is_empty() { "postgres".to_string() } else { user },
            password,
            database,
        })
    }
}

/// A wrapper around a TCP stream for PostgreSQL communication
pub struct PostgresConnection {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: BufWriter<tokio::net::tcp::OwnedWriteHalf>,
}

impl PostgresConnection {
    /// Connect to PostgreSQL and perform handshake
    pub async fn connect(params: &ConnectionParams) -> Result<Self> {
        let addr = format!("{}:{}", params.host, params.port);
        let stream = TcpStream::connect(&addr).await.map_err(|e| {
            Error::connection(format!("Failed to connect to {}: {}", addr, e))
        })?;

        stream.set_nodelay(true).ok();

        let (reader, writer) = stream.into_split();
        let mut conn = Self {
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
        };

        conn.perform_startup(params).await?;

        Ok(conn)
    }

    /// Split into reader and writer parts (consumes self)
    pub fn into_split(self) -> (BufReader<tokio::net::tcp::OwnedReadHalf>, BufWriter<tokio::net::tcp::OwnedWriteHalf>) {
        (self.reader, self.writer)
    }

    /// Perform PostgreSQL startup handshake
    async fn perform_startup(&mut self, params: &ConnectionParams) -> Result<()> {
        let mut buf = BytesMut::new();

        // Build startup message with replication=database
        let startup_params: Vec<(&str, &str)> = vec![
            ("user", &params.user),
            ("database", &params.database),
            ("replication", "database"),
            ("client_encoding", "UTF8"),
        ];

        // Calculate message length
        let mut msg_len = 4 + 4; // length + protocol version
        for (key, value) in &startup_params {
            msg_len += key.len() + 1 + value.len() + 1;
        }
        msg_len += 1; // null terminator

        buf.put_i32(msg_len as i32);
        buf.put_i32(196608); // Protocol version 3.0

        for (key, value) in &startup_params {
            buf.put_slice(key.as_bytes());
            buf.put_u8(0);
            buf.put_slice(value.as_bytes());
            buf.put_u8(0);
        }
        buf.put_u8(0); // terminator

        self.writer.write_all(&buf).await.map_err(|e| Error::connection(e.to_string()))?;
        self.writer.flush().await.map_err(|e| Error::connection(e.to_string()))?;

        // Read response
        loop {
            let msg_type = self.reader.read_u8().await.map_err(|e| Error::connection(e.to_string()))?;
            let msg_len = self.reader.read_i32().await.map_err(|e| Error::connection(e.to_string()))? as usize - 4;

            let mut msg_buf = vec![0u8; msg_len];
            self.reader.read_exact(&mut msg_buf).await.map_err(|e| Error::connection(e.to_string()))?;

            match msg_type {
                b'R' => {
                    // Authentication request
                    let auth_type = if msg_buf.len() >= 4 {
                        i32::from_be_bytes([msg_buf[0], msg_buf[1], msg_buf[2], msg_buf[3]])
                    } else {
                        0
                    };

                    match auth_type {
                        0 => {
                            debug!("Authentication successful");
                        }
                        3 => {
                            // CleartextPassword
                            if let Some(ref password) = params.password {
                                self.send_password(password).await?;
                            } else {
                                return Err(Error::auth("Password required but not provided"));
                            }
                        }
                        5 => {
                            // MD5Password
                            if msg_buf.len() >= 8 {
                                let salt = &msg_buf[4..8];
                                if let Some(ref password) = params.password {
                                    self.send_md5_password(&params.user, password, salt).await?;
                                } else {
                                    return Err(Error::auth("Password required but not provided"));
                                }
                            }
                        }
                        10 => {
                            return Err(Error::auth(
                                "SASL authentication not yet supported. Use md5 or trust.",
                            ));
                        }
                        _ => {
                            return Err(Error::auth(format!(
                                "Unsupported authentication type: {}",
                                auth_type
                            )));
                        }
                    }
                }
                b'K' => { debug!("Received BackendKeyData"); }
                b'S' => { trace!("Received ParameterStatus"); }
                b'Z' => {
                    debug!("Server ready for queries");
                    return Ok(());
                }
                b'E' => {
                    let error_msg = parse_error_response(&msg_buf);
                    return Err(Error::connection(format!("Server error: {}", error_msg)));
                }
                _ => {
                    trace!("Unknown message type during startup: {}", msg_type as char);
                }
            }
        }
    }

    /// Send cleartext password
    async fn send_password(&mut self, password: &str) -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put_u8(b'p');
        buf.put_i32((4 + password.len() + 1) as i32);
        buf.put_slice(password.as_bytes());
        buf.put_u8(0);

        self.writer.write_all(&buf).await.map_err(|e| Error::connection(e.to_string()))?;
        self.writer.flush().await.map_err(|e| Error::connection(e.to_string()))?;
        Ok(())
    }

    /// Send MD5 password
    async fn send_md5_password(&mut self, user: &str, password: &str, salt: &[u8]) -> Result<()> {
        let inner = format!("{}{}", password, user);
        let inner_hash = md5::compute(inner.as_bytes());
        let inner_hex = format!("{:x}", inner_hash);

        let mut outer = inner_hex.as_bytes().to_vec();
        outer.extend_from_slice(salt);
        let outer_hash = md5::compute(&outer);
        let password_hash = format!("md5{:x}", outer_hash);

        let mut buf = BytesMut::new();
        buf.put_u8(b'p');
        buf.put_i32((4 + password_hash.len() + 1) as i32);
        buf.put_slice(password_hash.as_bytes());
        buf.put_u8(0);

        self.writer.write_all(&buf).await.map_err(|e| Error::connection(e.to_string()))?;
        self.writer.flush().await.map_err(|e| Error::connection(e.to_string()))?;
        Ok(())
    }

    /// Execute a simple query
    pub async fn simple_query(&mut self, query: &str) -> Result<Vec<Vec<Option<String>>>> {
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q');
        buf.put_i32((4 + query.len() + 1) as i32);
        buf.put_slice(query.as_bytes());
        buf.put_u8(0);

        self.writer.write_all(&buf).await.map_err(|e| Error::connection(e.to_string()))?;
        self.writer.flush().await.map_err(|e| Error::connection(e.to_string()))?;

        let mut rows = Vec::new();

        loop {
            let msg_type = self.reader.read_u8().await.map_err(|e| Error::connection(e.to_string()))?;
            let msg_len = self.reader.read_i32().await.map_err(|e| Error::connection(e.to_string()))? as usize - 4;

            let mut msg_buf = vec![0u8; msg_len];
            self.reader.read_exact(&mut msg_buf).await.map_err(|e| Error::connection(e.to_string()))?;

            match msg_type {
                b'T' => {} // RowDescription
                b'D' => {
                    rows.push(parse_data_row(&msg_buf)?);
                }
                b'C' => {} // CommandComplete
                b'Z' => { break; } // ReadyForQuery
                b'E' => {
                    let error_msg = parse_error_response(&msg_buf);
                    return Err(Error::parse(format!("Query error: {}", error_msg)));
                }
                b'N' => {} // NoticeResponse
                _ => {}
            }
        }
        Ok(rows)
    }

    /// Send a raw command packet (used for START_REPLICATION)
    pub async fn send_command_raw(&mut self, command: &str) -> Result<()> {
        let mut buf = BytesMut::new();
        buf.put_u8(b'Q');
        buf.put_i32((4 + command.len() + 1) as i32);
        buf.put_slice(command.as_bytes());
        buf.put_u8(0);

        self.writer.write_all(&buf).await.map_err(|e| Error::connection(e.to_string()))?;
        self.writer.flush().await.map_err(|e| Error::connection(e.to_string()))?;
        Ok(())
    }

    /// Receive raw response bytes (helper for START_REPLICATION response)
    pub async fn receive_raw_message(&mut self) -> Result<(u8, Vec<u8>)> {
        let msg_type = self.reader.read_u8().await.map_err(|e| Error::connection(e.to_string()))?;
        let msg_len = self.reader.read_i32().await.map_err(|e| Error::connection(e.to_string()))? as usize - 4;

        let mut msg_buf = vec![0u8; msg_len];
        if msg_len > 0 {
            self.reader.read_exact(&mut msg_buf).await.map_err(|e| Error::connection(e.to_string()))?;
        }
        Ok((msg_type, msg_buf))
    }
}

/// Parse error response message
pub fn parse_error_response(data: &[u8]) -> String {
    let mut message = String::new();
    let mut i = 0;
    while i < data.len() {
        let field_type = data[i];
        i += 1;
        if field_type == 0 {
            break;
        }
        let end = data[i..].iter().position(|&b| b == 0).unwrap_or(data.len() - i);
        let value = String::from_utf8_lossy(&data[i..i + end]);
        i += end + 1;

        match field_type {
            b'M' => message = value.to_string(),
            _ => {}
        }
    }
    message
}

/// Parse a DataRow message
fn parse_data_row(data: &[u8]) -> Result<Vec<Option<String>>> {
    if data.len() < 2 {
        return Ok(Vec::new());
    }

    let num_cols = i16::from_be_bytes([data[0], data[1]]) as usize;
    let mut cols = Vec::with_capacity(num_cols);
    let mut pos = 2;

    for _ in 0..num_cols {
        if pos + 4 > data.len() {
            break;
        }
        let col_len = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;

        if col_len == -1 {
            cols.push(None);
        } else {
            let col_len = col_len as usize;
            if pos + col_len > data.len() {
                break;
            }
            let value = String::from_utf8_lossy(&data[pos..pos + col_len]).to_string();
            cols.push(Some(value));
            pos += col_len;
        }
    }
    Ok(cols)
}
