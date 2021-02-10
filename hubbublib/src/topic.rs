//! # Definition of Topic type for pub/sub

// #![allow(dead_code, unused_imports, unused_variables)]

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use tokio::net::TcpStream;

use crate::msg::MessageSchema;
use crate::HubWriter;

type NodeName = String;

/// A topic that is able to publish messages to all its subscribers
#[derive(Debug)]
pub struct Topic {
    // TODO: should these be Dashes?
    subscribers: HashMap<NodeName, HubWriter>,
    publishers: HashSet<NodeName>,
    msg_schema: MessageSchema,
}

impl Topic {
    /// Construct a new topic with no subscribers or publishers.
    pub fn new(msg_schema: &MessageSchema) -> Self {
        Self {
            subscribers: HashMap::new(),
            publishers: HashSet::new(),
            msg_schema: msg_schema.to_owned(),
        }
    }

    /// Get an iterator over the writer for each subscriber to a [`Topic`].
    pub fn subscribers(&self) -> std::collections::hash_map::Iter<NodeName, HubWriter> {
        self.subscribers.iter()
    }

    /// Get an iterator over the reader for each publisher to a [`Topic`].
    pub fn publishers(&self) -> std::collections::hash_set::Iter<NodeName> {
        self.publishers.iter()
    }

    /// Add a new subscriber to a [`Topic`].
    ///
    /// # Errors
    /// Returns `Err` if the message schema does not match the schema for this `Topic`.
    pub fn add_subscriber(
        &mut self,
        node_name: &str,
        msg_schema: &MessageSchema,
        stream: TcpStream,
    ) -> Result<()> {
        if msg_schema != &self.msg_schema {
            Err(TopicError::MessageTypeError)
        } else {
            self.subscribers
                .insert(String::from(node_name), HubWriter::new(stream));
            Ok(())
        }
    }

    /// Add a new publisher to a [`Topic`].
    ///
    /// # Errors
    /// Returns `Err` if the message schema does not match the schema for this `Topic`.
    pub fn add_publisher(&mut self, node_name: &str, msg_schema: &MessageSchema) -> Result<()> {
        if msg_schema != &self.msg_schema {
            Err(TopicError::MessageTypeError)
        } else {
            self.publishers.insert(String::from(node_name));
            Ok(())
        }
    }

    /// Remove a subscriber from a [`Topic`]
    pub fn remove_subscriber(&mut self, node_name: &str) {
        self.subscribers.remove(node_name);
    }

    /// Remove a publisher from a [`Topic`]
    pub fn remove_publisher(&mut self, node_name: &str) {
        self.publishers.remove(node_name);
    }

    /// Push a message out to all subscribers to a [`Topic`].
    ///
    /// # Errors
    /// Returns `Err` as soon as one of the writes fails.
    pub async fn publish(&mut self, message: Bytes) -> tokio::io::Result<()> {
        // `self.subscriber_streams` will be drained into this `HashMap`, retaining the
        // subscribers that are still connected.  Initial capacity is the same as
        // `self.subscriber_streams` based on the assumption that disconnects will be
        // rare compared to publishes.
        let mut connected_subs: HashMap<NodeName, HubWriter> =
            HashMap::with_capacity(self.subscribers.capacity());

        // Write the message and check for a `BrokenPipe` error.  Drops the subscriber on
        // `BrokenPipe`, returns other errors, and retains the subscriber if no error.
        for (node, mut sub) in self.subscribers.drain() {
            let broken_pipe = sub.write_and_check(&message).await?;
            if !broken_pipe {
                connected_subs.insert(node, sub);
            }
        }
        self.subscribers = connected_subs;
        Ok(())
    }
}

#[derive(Debug)]
pub enum TopicError {
    MessageTypeError,
}

pub type Result<T> = std::result::Result<T, TopicError>;
