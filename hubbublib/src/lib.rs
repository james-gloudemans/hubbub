#![allow(dead_code, unused_imports, unused_variables)]
use std::collections::VecDeque;
use std::slice::Iter;

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;

pub mod hcl;
pub mod hub;
pub mod msg;
pub mod topic;

/// A thin wrapper around [`tokio::net::TcpStream`] to simplify writing to the stream.
#[derive(Debug)]
pub struct HubWriter {
    stream: BufWriter<TcpStream>,
}

impl HubWriter {
    /// Construct a new [`HubWriter`].
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(stream),
        }
    }

    /// Consume this object and return the contained [`tokio::net::TcpStream`].
    pub fn into_inner(self) -> TcpStream {
        self.stream.into_inner()
    }

    /// Write `message` to the stream.
    ///
    /// # Errors
    /// Returns [`Err`] if any of the write operations fail.
    pub async fn write(&mut self, message: &Bytes) -> tokio::io::Result<()> {
        self.stream.write_all(message).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Write `message` to the stream and return `Ok(true)` if there was no `BrokenPipe` Error
    ///
    /// # Errors
    /// Returns `Err` for any error besides `BrokenPipe`
    pub async fn write_and_check(&mut self, message: &Bytes) -> Result<bool, tokio::io::Error> {
        if let Some(e) = self.stream.write_all(message).await.err() {
            if e.kind() == tokio::io::ErrorKind::BrokenPipe {
                return Ok(true);
            } else {
                return Err(e);
            }
        }
        if let Some(e) = self.stream.flush().await.err() {
            if e.kind() == tokio::io::ErrorKind::BrokenPipe {
                return Ok(true);
            } else {
                return Err(e);
            }
        }
        Ok(false)
    }
}

/// A thin wrapper around [`tokio::net::TcpStream`] to simplify reading from the stream.
#[derive(Debug)]
pub struct HubReader {
    stream: BufReader<TcpStream>,
}

impl HubReader {
    /// Construct a new [`HubReader`].
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufReader::new(stream),
        }
    }

    /// Read and return a message from the stream.
    ///
    /// Since this uses [`AsyncBufReadExt::read_line()`], this will read up to the
    /// first newline character.
    ///
    /// # Errors
    /// Returns [`Err`] if any of the underlying read operations fail.
    pub async fn read(&mut self) -> tokio::io::Result<Bytes> {
        let mut buf = String::new();
        self.stream
            .read_line(&mut buf)
            .await
            .map(|_| Bytes::from(buf))
    }
}

/// A type which can be used by node entities (i.e. publishers or subscribers) to
/// identify themselves to the `Hub` by sending a `Message<HubEntity>` as a greeting
/// immediately after connection.
#[derive(Serialize, Deserialize, Debug)]
pub enum HubEntity {
    Publisher {
        node_name: String,
        topic_name: String,
    },
    Subscriber {
        node_name: String,
        topic_name: String,
    },
    Node {
        node_name: String,
    },
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
