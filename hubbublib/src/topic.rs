//! # Definition of Topic type for pub/sub
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

    /// Get an iterator over the writer for each subscriber to a [`Topic`].
    pub fn subscribers(&self) -> std::slice::Iter<HubWriter> {
        self.subscribers.iter()
    }

    /// Get an iterator over the reader for each publisher to a [`Topic`].
    pub fn publishers(&self) -> std::slice::Iter<HubReader> {
        self.publishers.iter()
    }

    /// Add a new subscriber to a [`Topic`].
    pub fn add_subscriber(&mut self, stream: TcpStream) {
        self.subscribers.push(HubWriter::new(stream));
    }

    /// Add a new publisher to a [`Topic`].
    pub fn add_publisher(&mut self, stream: TcpStream) {
        self.publishers.push(HubReader::new(stream));
    }

    /// Push a message out to all subscribers to a [`Topic`].
    ///
    /// # Errors
    /// Returns `Err` as soon as one of the writes fails.
    pub async fn publish(&mut self, message: Bytes) -> tokio::io::Result<()> {
        // `self.subscribers` will be drained into this `Vec`, retaining the subscribers
        // that are still connected.  Initial capacity is the same as `self.subscribers` based
        // on the assumption that disconnects will be rare compared to publishes.
        let mut connected_subs: Vec<HubWriter> = Vec::with_capacity(self.subscribers.capacity());

        // Write the message and check for a `BrokenPipe` error.  Drops the subscriber on
        // `BrokenPipe`, returns other errors, and retains the subscriber if no error.
        for mut sub in self.subscribers.drain(..) {
            let broken_pipe = sub.write_and_check(&message).await?;
            if !broken_pipe {
                connected_subs.push(sub);
            }
        }
        self.subscribers = connected_subs;
        Ok(())
    }
}
