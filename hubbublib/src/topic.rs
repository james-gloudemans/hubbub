use std::slice::Iter;
use std::str::from_utf8;

use bytes::Bytes;
use tokio::net::TcpStream;

use crate::{HubReader, HubWriter};

/// A topic that is able to read messages from all of its publishers
/// and write those messages to all subscribers.
pub struct Topic {
    subscribers: Vec<HubWriter>,
    publishers: Vec<HubReader>,
}

impl Topic {
    /// Construct a new topic with no subscribers or publishers.
    pub fn new() -> Self {
        Self {
            subscribers: Vec::new(),
            publishers: Vec::new(),
        }
    }

    /// Get an iterator over the writer for each subscriber to a `Topic`.
    pub fn subscribers(&self) -> Iter<HubWriter> {
        self.subscribers.iter()
    }

    /// Get an iterator over the reader for each publisher to a `Topic`.
    pub fn publishers(&self) -> Iter<HubReader> {
        self.publishers.iter()
    }

    /// Add a new subscriber to a `Topic`.
    pub fn add_subscriber(&mut self, stream: TcpStream) {
        self.subscribers.push(HubWriter::new(stream));
    }

    /// Add a new publisher to a `Topic`.
    pub fn add_publisher(&mut self, stream: TcpStream) {
        self.publishers.push(HubReader::new(stream));
    }

    /// Push a message out to all subscribers to a `Topic`.
    pub async fn publish(&mut self, message: Bytes) -> tokio::io::Result<()> {
        for sub in self.subscribers.iter_mut() {
            sub.write(&message).await?;
        }
        Ok(())
    }
}
