use std::slice::Iter;
use std::str::from_utf8;

use bytes::Bytes;
use tokio::net::TcpStream;

use crate::{HubMQReader, HubMQWriter};

pub struct Topic {
    subscribers: Vec<HubMQWriter>,
    publishers: Vec<HubMQReader>,
}

impl Topic {
    pub fn new() -> Self {
        Self {
            subscribers: Vec::new(),
            publishers: Vec::new(),
        }
    }

    pub fn subscribers(&self) -> Iter<HubMQWriter> {
        self.subscribers.iter()
    }

    pub fn publishers(&self) -> Iter<HubMQReader> {
        self.publishers.iter()
    }

    pub fn add_subscriber(&mut self, stream: TcpStream) {
        self.subscribers.push(HubMQWriter::new(stream));
    }

    pub fn add_publisher(&mut self, stream: TcpStream) {
        self.publishers.push(HubMQReader::new(stream));
    }

    pub async fn publish(&mut self, message: Bytes) -> tokio::io::Result<()> {
        for sub in self.subscribers.iter_mut() {
            sub.write(&message).await?;
        }
        Ok(())
    }
}
