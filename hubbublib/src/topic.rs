//! # Definition of Topic type for pub/sub
use std::collections::HashSet;
use std::str::from_utf8;

use bytes::{Bytes, BytesMut};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

use crate::{HubReader, HubWriter};

/// A topic that is able to publish messages to all its subscribers
pub struct Topic {
    subscriber_streams: Vec<HubWriter>,
    publisher_nodes: Vec<String>,
    subscriber_nodes: Vec<String>,
}

impl Topic {
    /// Construct a new topic with no subscribers or publishers.
    pub fn new() -> Self {
        Self {
            subscriber_streams: Vec::new(),
            publisher_nodes: Vec::new(),
            subscriber_nodes: Vec::new(),
        }
    }

    /// Get an iterator over the writer for each subscriber to a [`Topic`].
    pub fn subscribers(&self) -> std::slice::Iter<String> {
        self.subscriber_nodes.iter()
    }

    /// Get an iterator over the reader for each publisher to a [`Topic`].
    pub fn publishers(&self) -> std::slice::Iter<String> {
        self.publisher_nodes.iter()
    }

    /// Add a new subscriber to a [`Topic`].
    pub fn add_subscriber(&mut self, node_name: &str, stream: TcpStream) {
        self.subscriber_streams.push(HubWriter::new(stream));
        self.subscriber_nodes.push(String::from(node_name))
    }

    /// Add a new publisher to a [`Topic`].
    pub fn add_publisher(&mut self, node_name: &str) {
        self.publisher_nodes.push(String::from(node_name));
    }

    /// Push a message out to all subscribers to a [`Topic`].
    ///
    /// # Errors
    /// Returns `Err` as soon as one of the writes fails.
    pub async fn publish(&mut self, message: Bytes) -> tokio::io::Result<()> {
        // `self.subscriber_streams` will be drained into this `Vec`, retaining the subscribers
        // that are still connected.  Initial capacity is the same as `self.subscriber_streams` based
        // on the assumption that disconnects will be rare compared to publishes.
        let mut connected_subs: Vec<HubWriter> =
            Vec::with_capacity(self.subscriber_streams.capacity());

        // Write the message and check for a `BrokenPipe` error.  Drops the subscriber on
        // `BrokenPipe`, returns other errors, and retains the subscriber if no error.
        for mut sub in self.subscriber_streams.drain(..) {
            let broken_pipe = sub.write_and_check(&message).await?;
            if !broken_pipe {
                connected_subs.push(sub);
            }
        }
        self.subscriber_streams = connected_subs;
        Ok(())
    }
}
