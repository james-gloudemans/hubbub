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
    println!("Master listening at {}", hub.address);
    loop {
        let (stream, client_addr) = hub.listener.accept().await.unwrap();
        println!("Accepting connecton from client at: {}", client_addr);
        let hub = Arc::clone(&hub);
        tokio::spawn(async move {
            process_new_node(stream, hub).await;
        });
    }
}

async fn process_new_node(mut stream: TcpStream, hub: Arc<Hub>) {
    let mut buf = BytesMut::with_capacity(256);
    // Wait for greeting message from new node
    let size = stream.read_buf(&mut buf).await.unwrap();
    let greeting: Message<NodeEntity> = serde_json::from_str(from_utf8(&buf).unwrap())
        .expect("Node connection failed due to malformed greeting");
    println!("Received Greeting: {:?}", greeting);
    // Use greeting to determine the node entity's type
    if let Some(greeting) = greeting.data() {
        match greeting {
            // If publisher, listen for messages and push them to subscribers
            NodeEntity::Publisher { topic_name } => loop {
                let mut buf = BytesMut::with_capacity(4096);
                let size = stream.read_buf(&mut buf).await.unwrap();
                if let Some(mut topic) = hub.topics.get_mut(topic_name) {
                    topic.publish(buf.freeze()).await.unwrap();
                }
            },
            // If subscriber, register in hub.topics and return
            NodeEntity::Subscriber { topic_name } => {
                if let Some(mut topic) = hub.topics.get_mut(topic_name) {
                    topic.add_subscriber(stream);
                } else {
                    let mut topic = Topic::new();
                    topic.add_subscriber(stream);
                    hub.topics.insert(topic_name.to_string(), topic);
                }
            }
        }
    } else {
        // Empty greeting message is ignored
        println!("Node connection request failed due to empty greeting.")
    }
}

struct Hub {
    address: SocketAddr,
    listener: TcpListener,
    topics: Arc<DashMap<String, Topic>>,
}

impl Hub {
    async fn new(addr: &str) -> Self {
        let address: SocketAddr = addr.parse().expect("Malformed IP address.");
        let listener = TcpListener::bind(address).await.unwrap();
        Self {
            address,
            listener,
            topics: Arc::new(DashMap::new()),
        }
    }
}
