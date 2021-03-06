//! Definition of the central `Hub` data structure which tracks and routes all the
//! entities in a Hubbub network.

#![allow(dead_code, unused_imports, unused_variables)]

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::str::from_utf8;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use dashmap::{DashMap, DashSet};
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};

use crate::msg::{Message, MessageSchema};
use crate::topic::Topic;
use crate::{HubEntity, HubRequest, HubWriter};

// TODO: Should these be struct XName(String)?
type TopicName = String;
type NodeName = String;
type ItemRef<'a> = dashmap::mapref::multiple::RefMulti<'a, String, Topic>;

/// The master [`Hub`] that manages connections between Hubbub nodes and tracks them
/// for introspection of the Hubbub graph.
#[derive(Debug)]
pub struct Hub {
    address: SocketAddr,
    listener: TcpListener,
    topics: TopicRegistry,
    nodes: NodeRegistry,
}

impl Hub {
    /// Construct a new [`Hub`] listening on the given IP address `addr`
    ///
    /// # Panics
    /// Panics if the given IP address is malformed or if the [`TcpListener`] fails to bind
    /// to the address.
    pub async fn new(addr: &str) -> Self {
        let address: SocketAddr = addr.parse().expect("Malformed IP address.");
        let listener = TcpListener::bind(address).await.unwrap();
        Self {
            address,
            listener,
            topics: TopicRegistry(Arc::new(DashMap::new())),
            nodes: NodeRegistry(Arc::new(DashSet::new())),
        }
    }

    /// Get the address that a [`Hub`]'s is listening on.
    pub fn address(&self) -> &SocketAddr {
        &self.address
    }

    /// Connect a new entity to this [`Hub`]
    pub async fn connect(greeting: &Message<HubEntity>) -> crate::hcl::Result<TcpStream> {
        let stream = TcpStream::connect("127.0.0.1:8080").await?;
        let mut writer = HubWriter::new(stream);
        writer.write(&greeting.as_bytes()?).await?;
        Ok(writer.into_inner())
    }

    // TODO: make connection a request / reply so the reply can contain info about errors
    pub async fn process_new_entity(&self, mut stream: TcpStream) {
        let mut buf = BytesMut::with_capacity(256);
        // Wait for greeting message from new entity
        let size = stream.read_buf(&mut buf).await.unwrap();
        let greeting: Message<HubEntity> =
            serde_json::from_str(from_utf8(&buf).unwrap()).expect(&format!(
                "Node connection failed due to malformed greeting: {}",
                from_utf8(&buf).unwrap()
            ));
        // Use greeting to determine the entity's type
        match greeting.data() {
            // If publisher, listen for messages and push them to subscribers
            HubEntity::Publisher {
                node_name,
                topic_name,
                msg_schema,
            } => match self.add_publisher(node_name, topic_name, msg_schema) {
                Ok(_) => {
                    println!(
                        "New publisher on topic '{}' from node '{}'",
                        topic_name, node_name
                    );
                    loop {
                        let mut buf = BytesMut::with_capacity(4096);
                        if stream.read_buf(&mut buf).await.unwrap() == 0 {
                            break;
                        } else {
                            if let Some(mut topic) = self.topics.0.get_mut(topic_name) {
                                topic.publish(buf.freeze()).await.unwrap();
                            }
                        }
                    }
                    self.remove_publisher(node_name, topic_name);
                }
                Err(HubError::MessageTypeError) => {
                    println!("Failed to connect publisher due to type mismatch.")
                }
                Err(_) => panic!("Error encountered while connecting publisher."),
            },
            // If subscriber, register in `self` and return
            HubEntity::Subscriber {
                node_name,
                topic_name,
                msg_schema,
            } => match self.add_subscriber(node_name, topic_name, msg_schema, stream) {
                Err(HubError::MessageTypeError) => {
                    println!("Failed to connect subscriber due to type mismatch.")
                }
                Err(_) => panic!("Error encountered while connecting subscriber."),
                Ok(_) => {
                    println!(
                        "New subscriber on topic '{}' from node '{}'",
                        topic_name, node_name
                    );
                }
            },
            // If new Node, register in `self` and return
            HubEntity::Node { node_name } => {
                println!("New node '{}'", node_name);
                self.add_node(node_name).unwrap()
            }

            // If CLI request, parse request and send response based on the current Hub graph.
            HubEntity::Cli(request) => match request {
                HubRequest::NodeList => {
                    Hub::cli_response(&self.nodes(), stream).await;
                }
                HubRequest::NodeInfo(node_name) => {
                    Hub::cli_response(
                        &(
                            self.node_publishers(node_name),
                            self.node_subscribers(node_name),
                        ),
                        stream,
                    )
                    .await;
                }
                HubRequest::TopicList => {
                    Hub::cli_response(&self.topics(), stream).await;
                }
                HubRequest::TopicSchema(topic_name) => {
                    let schema: String = self
                        .topics
                        .0
                        .get(topic_name)
                        .map(|t| t.schema().to_owned())
                        .unwrap_or("".to_owned());
                    Hub::cli_response(&schema, stream).await;
                }
                HubRequest::TopicInfo(topic_name) => {
                    Hub::cli_response(
                        &(
                            self.topic_publishers(topic_name),
                            self.topic_subscribers(topic_name),
                        ),
                        stream,
                    )
                    .await;
                }
                HubRequest::TopicEcho(topic_name) => {
                    let topic = match self.topics.0.get(topic_name) {
                        None => return,
                        Some(topic) => topic,
                    };
                    let schema = topic.schema().to_owned();
                    // Drop reference into dashmap when done w/ it to avoid deadlock.
                    drop(topic);
                    let node_name = "CLI_NODE";
                    self.add_node("CLI_NODE").unwrap();
                    self.add_subscriber(
                        node_name,
                        topic_name,
                        &MessageSchema::from_str(&schema),
                        stream,
                    )
                    .expect("Lazy expect message lol.");
                }
            },
        }
    }

    /// Boilerplate for responding to request from CLI
    async fn cli_response<T>(response: &T, stream: TcpStream)
    where
        T: Serialize,
    {
        let response: Bytes = serde_json::to_vec(response)
            .map(|mut s| {
                s.push(b'\n' as u8);
                Bytes::from(s)
            })
            .unwrap();
        let mut writer = BufWriter::new(stream);
        writer.write_all(&response).await.unwrap();
        writer.flush().await.unwrap();
    }

    /// Return a `HashSet` of the names of the topics on a `Hub`.
    pub fn topics(&self) -> HashSet<TopicName> {
        self.topics
            .0
            .iter()
            .map(|item| item.key().to_owned())
            .collect()
    }

    /// Return a `HashSet` of the names of nodes on a `Hub`.
    pub fn nodes(&self) -> HashSet<NodeName> {
        self.nodes.0.iter().map(|name| name.to_owned()).collect()
    }

    /// Return a `HashSet` of the names of topics that a node named `node_name` is publishing on.
    pub fn node_publishers(&self, node_name: &str) -> HashSet<TopicName> {
        let mut result = HashSet::new();
        for topic in self.topics.0.iter() {
            if topic.publishers().contains(node_name) {
                result.insert(topic.key().to_owned());
            }
        }
        result
    }

    /// Return a `HashSet` of the names of topics that a node named `node_name` is subscribed to.
    pub fn node_subscribers(&self, node_name: &str) -> HashSet<TopicName> {
        let mut result = HashSet::new();
        for topic in self.topics.0.iter() {
            if topic.subscribers().contains(node_name) {
                result.insert(topic.key().to_owned());
            }
        }
        result
    }

    /// Return a `HashSet` of the names of nodes publishing on the topic named `topic_name`.
    pub fn topic_publishers(&self, topic_name: &str) -> HashSet<NodeName> {
        match self.topics.0.get(topic_name) {
            None => HashSet::new(),
            Some(topic) => topic.publishers(),
        }
    }

    /// Return a `HashSet` of the names of nodes subscribed to the topic named `topic_name`.
    pub fn topic_subscribers(&self, topic_name: &str) -> HashSet<NodeName> {
        match self.topics.0.get(topic_name) {
            None => HashSet::new(),
            Some(topic) => topic.subscribers(),
        }
    }

    /// Add a new Node for the [`Hub`] to track.
    pub fn add_node(&self, node_name: &str) -> Result<()> {
        if self.nodes.0.contains(node_name) {
            return Err(HubError::DuplicateNodeNameError {
                name: String::from(node_name),
            });
        } else {
            self.nodes.0.insert(String::from(node_name));
        }
        Ok(())
    }

    /// Add a new publisher for the [`Hub`] to track.
    ///
    /// # Errors
    /// Returns `Err` if the message type does not match the message type for the `Topic`
    pub fn add_publisher(
        &self,
        node_name: &str,
        topic_name: &str,
        msg_schema: &MessageSchema,
    ) -> Result<()> {
        if !self.topics.0.contains_key(topic_name) {
            self.topics
                .0
                .insert(topic_name.to_string(), Topic::new(msg_schema));
        }
        let mut topic = self.topics.0.get_mut(topic_name).unwrap();
        match topic.add_publisher(node_name, msg_schema) {
            Ok(_) => Ok(()),
            Err(crate::topic::TopicError::MessageTypeError) => Err(HubError::MessageTypeError),
        }
    }

    /// Add a new subscriber for the [`Hub`] to track.
    ///
    /// # Errors
    /// Returns `Err` if the message type does not match the message type for the `Topic`
    pub fn add_subscriber(
        &self,
        node_name: &str,
        topic_name: &str,
        msg_schema: &MessageSchema,
        stream: TcpStream,
    ) -> Result<()> {
        if !self.topics.0.contains_key(topic_name) {
            self.topics
                .0
                .insert(topic_name.to_string(), Topic::new(msg_schema));
        }
        let mut topic = self.topics.0.get_mut(topic_name).unwrap();
        match topic.add_subscriber(node_name, msg_schema, stream) {
            Ok(_) => Ok(()),
            Err(crate::topic::TopicError::MessageTypeError) => Err(HubError::MessageTypeError),
        }
    }

    /// Stop tracking a publisher with the given node and topic names.
    pub fn remove_publisher(&self, node_name: &str, topic_name: &str) {
        if let Some(mut topic) = self.topics.0.get_mut(topic_name) {
            topic.remove_publisher(node_name);
        }
    }

    /// Listen for incoming connections to the [`Hub`].
    ///
    /// This function will yield once a new TCP connection is established.
    /// When established, the corresponding [`TcpStream`] and the remote peer's
    /// address will be returned.
    pub async fn listen(&self) -> tokio::io::Result<(TcpStream, SocketAddr)> {
        self.listener.accept().await
    }
}

#[derive(Debug)]
struct TopicRegistry(Arc<DashMap<TopicName, Topic>>);

#[derive(Debug)]
struct NodeRegistry(Arc<DashSet<NodeName>>);

/// Error type representing all possible errors that can happen in [`Hub`] operations
#[derive(Debug)]
pub enum HubError {
    DuplicateNodeNameError { name: String },
    MessageTypeError,
}

/// A specialized [`Result`] type for [`Hub`] operations.
pub type Result<T> = std::result::Result<T, HubError>;
