//! # Definition of Topic type for pub/sub

// #![allow(dead_code, unused_imports, unused_variables)]

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use dashmap::{DashMap, DashSet};
use tokio::net::TcpStream;

use crate::msg::MessageSchema;
use crate::HubWriter;

type NodeName = String;

/// A topic that is able to publish messages to all its subscribers
#[derive(Debug)]
pub struct Topic {
    subscribers: Arc<DashMap<NodeName, HubWriter>>,
    publishers: Arc<DashSet<NodeName>>,
    msg_schema: MessageSchema,
}

impl Topic {
    /// Construct a new topic with no subscribers or publishers.
    pub fn new(msg_schema: &MessageSchema) -> Self {
        Self {
            subscribers: Arc::new(DashMap::new()),
            publishers: Arc::new(DashSet::new()),
            msg_schema: msg_schema.to_owned(),
        }
    }

    /// Return a `HashSet` of the names of the nodes subscribed to a `Topic`.
    pub fn subscribers(&self) -> HashSet<NodeName> {
        self.subscribers
            .iter()
            .map(|item| item.key().to_owned())
            .collect()
    }

    /// Return a `HashSet` of the names of the nodes subscribed to a `Topic`.
    pub fn publishers(&self) -> HashSet<NodeName> {
        self.publishers.iter().map(|name| name.to_owned()).collect()
    }

    pub fn schema(&self) -> &str {
        self.msg_schema.as_str()
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
        let mut disconnected: Vec<NodeName> = Vec::new();
        for mut item in self.subscribers.iter_mut() {
            let (node, sub) = item.pair_mut();
            if sub.write_and_check(&message).await? {
                disconnected.push(node.to_owned());
            }
        }
        for node in disconnected {
            self.subscribers.remove(&node);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum TopicError {
    MessageTypeError,
}

pub type Result<T> = std::result::Result<T, TopicError>;
