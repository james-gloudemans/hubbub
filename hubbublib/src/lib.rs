#![allow(dead_code, unused_imports, unused_variables)]
use std::collections::VecDeque;
use std::slice::Iter;

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;

pub mod hcl;
pub mod msg;
pub mod topic;

pub struct HubMQWriter {
    stream: BufWriter<TcpStream>,
    queue: VecDeque<Bytes>,
}

impl HubMQWriter {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(stream),
            queue: VecDeque::new(),
        }
    }

    pub fn into_inner(self) -> TcpStream {
        self.stream.into_inner()
    }

    pub async fn write(&mut self, message: &Bytes) -> tokio::io::Result<()> {
        self.stream.write_all(message).await?;
        self.stream.flush().await?;
        Ok(())
    }
}

pub struct HubMQReader {
    stream: BufReader<TcpStream>,
    queue: VecDeque<Bytes>,
}

impl HubMQReader {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufReader::new(stream),
            queue: VecDeque::new(),
        }
    }

    pub async fn read(&mut self) -> tokio::io::Result<Bytes> {
        let mut buf = String::new();
        self.stream
            .read_line(&mut buf)
            .await
            .map(|_| Bytes::from(buf))
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum NodeEntity {
    Publisher { topic_name: String },
    Subscriber { topic_name: String },
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
