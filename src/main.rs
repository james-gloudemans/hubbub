#![allow(dead_code, unused_imports, unused_variables)]
use std::net::SocketAddr;
use std::str::from_utf8;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};

use hubbublib::msg::Message;
use hubbublib::topic::Topic;
use hubbublib::NodeEntity;

#[tokio::main]
async fn main() {
    let hub = Arc::new(Hub::new("127.0.0.1:8080").await);
    println!("Hub listening at {}", hub.address);
    loop {
        let (stream, client_addr) = hub.listen().await.unwrap();
        println!("Accepting connecton from client at: {}", client_addr);
        let hub = Arc::clone(&hub);
        tokio::spawn(async move {
            hub.process_new_node(stream).await;
        });
    }
}

type TopicName = String;

/// The master [`Hub`] that manages connections between Hubbub nodes and tracks them
/// for introspection of the Hubbub graph.
#[derive(Debug)]
struct Hub {
    address: SocketAddr,
    listener: TcpListener,
    topics: Arc<DashMap<TopicName, Topic>>,
}

// TODO: improve Hub API for introspection
impl Hub {
    /// Construct a new [`Hub`]listening on the given IP address `addr`
    ///
    /// # Panics
    /// Panics if the given IP address is malformed or if the [`TcpListener`] fails to bind
    /// to the address.
    async fn new(addr: &str) -> Self {
        let address: SocketAddr = addr.parse().expect("Malformed IP address.");
        let listener = TcpListener::bind(address).await.unwrap();
        Self {
            address,
            listener,
            topics: Arc::new(DashMap::new()),
        }
    }

    async fn process_new_node(&self, mut stream: TcpStream) {
        let mut buf = BytesMut::with_capacity(256);
        // Wait for greeting message from new node
        let size = stream.read_buf(&mut buf).await.unwrap();
        let greeting: Message<NodeEntity> = serde_json::from_str(from_utf8(&buf).unwrap())
            .expect("Node connection failed due to malformed greeting");
        println!("Received Greeting: {:?}", greeting);
        // Use greeting to determine the node entity's type
        match greeting.data() {
            // If publisher, listen for messages and push them to subscribers
            NodeEntity::Publisher {
                node_name,
                topic_name,
            } => {
                self.add_publisher(node_name, topic_name);
                loop {
                    let mut buf = BytesMut::with_capacity(4096);
                    if stream.read_buf(&mut buf).await.unwrap() == 0 {
                        break;
                    } else {
                        if let Some(mut topic) = self.topics.get_mut(topic_name) {
                            topic.publish(buf.freeze()).await.unwrap();
                        }
                    }
                }
                self.remove_publisher(node_name, topic_name);
            }
            // If subscriber, register in `self` and return
            NodeEntity::Subscriber {
                node_name,
                topic_name,
            } => {
                self.add_subscriber(node_name, topic_name, stream);
            }
        }
    }

    /// Add a new publisher for the [`Hub`] to track.
    fn add_publisher(&self, node_name: &str, topic_name: &str) {
        if !self.topics.contains_key(topic_name) {
            self.topics.insert(topic_name.to_string(), Topic::new());
        }
        let mut topic = self.topics.get_mut(topic_name).unwrap();
        topic.add_publisher(node_name);
    }

    /// Add a new subscriber for the [`Hub`] to track.
    fn add_subscriber(&self, node_name: &str, topic_name: &str, stream: TcpStream) {
        if !self.topics.contains_key(topic_name) {
            self.topics.insert(topic_name.to_string(), Topic::new());
        }
        let mut topic = self.topics.get_mut(topic_name).unwrap();
        topic.add_subscriber(node_name, stream);
    }

    /// Stop tracking a publisher with the given node and topic names.
    fn remove_publisher(&self, node_name: &str, topic_name: &str) {
        if let Some(mut topic) = self.topics.get_mut(topic_name) {
            topic.remove_publisher(node_name);
        }
    }

    /// Listen for incoming connections to the [`Hub`].
    ///
    /// This function will yield once a new TCP connection is established.
    /// When established, the corresponding [`TcpStream`] and the remote peer's
    /// address will be returned.
    async fn listen(&self) -> tokio::io::Result<(TcpStream, SocketAddr)> {
        self.listener.accept().await
    }
}
