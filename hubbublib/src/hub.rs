use std::net::SocketAddr;
use std::str::from_utf8;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use dashmap::{DashMap, DashSet};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};

use crate::msg::Message;
use crate::topic::Topic;
use crate::{HubEntity, HubWriter};

type TopicName = String;
type NodeName = String;

/// The master [`Hub`] that manages connections between Hubbub nodes and tracks them
/// for introspection of the Hubbub graph.
#[derive(Debug)]
pub struct Hub {
    address: SocketAddr,
    listener: TcpListener,
    topics: TopicRegistry,
    nodes: NodeRegistry,
}

// TODO: #1 improve Hub API for introspection
impl Hub {
    /// Construct a new [`Hub`]listening on the given IP address `addr`
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

    pub fn address(&self) -> &SocketAddr {
        &self.address
    }

    pub async fn connect(greeting: &Message<HubEntity>) -> crate::hcl::Result<TcpStream> {
        let stream = TcpStream::connect("127.0.0.1:8080").await?;
        let mut writer = HubWriter::new(stream);
        writer.write(&greeting.as_bytes()?).await?;
        Ok(writer.into_inner())
    }

    pub async fn process_new_node(&self, mut stream: TcpStream) {
        let mut buf = BytesMut::with_capacity(256);
        // Wait for greeting message from new entity
        let size = stream.read_buf(&mut buf).await.unwrap();
        let greeting: Message<HubEntity> = serde_json::from_str(from_utf8(&buf).unwrap())
            .expect("Node connection failed due to malformed greeting");
        println!("Received Greeting: {:?}", greeting);
        // Use greeting to determine the entity's type
        match greeting.data() {
            // If publisher, listen for messages and push them to subscribers
            HubEntity::Publisher {
                node_name,
                topic_name,
            } => {
                self.add_publisher(node_name, topic_name);
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
            // If subscriber, register in `self` and return
            HubEntity::Subscriber {
                node_name,
                topic_name,
            } => {
                self.add_subscriber(node_name, topic_name, stream);
            }
            // If new Node, register in `self` and return
            HubEntity::Node { node_name } => self.add_node(node_name).unwrap(),
        }
    }

    /// Return an iterator over the names of the nodes.
    pub fn nodes(&self) /* Iter<String> */
    {
        // self.nodes.0.into_iter()
    }

    /// Return an iterator over the names of the topics.
    pub fn topics(&self) /* -> Iter<String> */
    {
        let x = self.topics.0.iter().map(|i| i.key().to_owned());
        // for t in &*self.topics.0 {
        //     let k = t.key();
        //     let v = t.value();
        // }
    }

    /// Return an iterator over the topics published on by the node named `node_name`
    pub fn node_publishers(&self, node_name: &str) /*-> Iter<String> */ {}

    /// Return an iterator over the topics subscribed to by the node named `node_name`
    pub fn node_subscribers(&self, node_name: &str) /*-> Iter<String> */ {}

    /// Return an iterator over the nodes publishing on the topic named `topic_name`
    pub fn topic_publishers(&self, topic_name: &str) /*Iter<String> */ {}

    /// Return an iterator over the nodes subscribed to the topic named `topic_name`
    pub fn topic_subscribers(&self, topic_name: &str) /*Iter<String> */ {}

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
    pub fn add_publisher(&self, node_name: &str, topic_name: &str) {
        if !self.topics.0.contains_key(topic_name) {
            self.topics.0.insert(topic_name.to_string(), Topic::new());
        }
        let mut topic = self.topics.0.get_mut(topic_name).unwrap();
        topic.add_publisher(node_name);
    }

    /// Add a new subscriber for the [`Hub`] to track.
    pub fn add_subscriber(&self, node_name: &str, topic_name: &str, stream: TcpStream) {
        if !self.topics.0.contains_key(topic_name) {
            self.topics.0.insert(topic_name.to_string(), Topic::new());
        }
        let mut topic = self.topics.0.get_mut(topic_name).unwrap();
        topic.add_subscriber(node_name, stream);
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

// impl IntoIter for TopicRegistry

#[derive(Debug)]
struct NodeRegistry(Arc<DashSet<NodeName>>);

// impl IntoIter for NodeRegistry

/// Error type representing all possible errors that can happen in [`Hub`] operations
#[derive(Debug)]
pub enum HubError {
    DuplicateNodeNameError { name: String },
}

/// A specialized [`Result`] type for [`Hub`] operations.
pub type Result<T> = std::result::Result<T, HubError>;
