#![allow(dead_code, unused_imports, unused_variables)]
use std::marker::PhantomData;
use std::str::from_utf8;

use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::net::TcpStream;

use crate::msg::{Message, MessageError};
use crate::{HubMQReader, HubMQWriter, NodeEntity};

#[derive(Debug)]
pub enum HubbubError {
    IoError(tokio::io::Error),
    MessageError(MessageError),
    NotImplemented,
}

impl From<tokio::io::Error> for HubbubError {
    fn from(e: tokio::io::Error) -> Self {
        HubbubError::IoError(e)
    }
}

impl From<MessageError> for HubbubError {
    fn from(e: MessageError) -> Self {
        HubbubError::MessageError(e)
    }
}

pub type Result<T> = std::result::Result<T, HubbubError>;

pub struct Publisher<T> {
    topic_name: String,
    writer: HubMQWriter,
    phantom_msg_type: PhantomData<T>,
}

impl<T> Publisher<T>
where
    T: Serialize + DeserializeOwned,
{
    // Idea: could the Hub send us back a reference to the topic?
    // Then, we could publish directly to the subscriber's streams
    // without needing to communicate through the master.
    // Might need to wrap the topic in an `Arc`
    pub async fn new(topic_name: &str) -> Result<Self> {
        let stream = TcpStream::connect("127.0.0.1:8080").await?;
        let mut writer = HubMQWriter::new(stream);
        let greeting = Message::new(Some(NodeEntity::Publisher {
            topic_name: String::from(topic_name),
        }));
        writer.write(&greeting.as_bytes()?).await?;
        Ok(Self {
            topic_name: String::from(topic_name),
            writer,
            phantom_msg_type: PhantomData,
        })
    }

    pub fn topic(&self) -> &str {
        &self.topic_name
    }

    pub async fn publish(&mut self, message: T) -> Result<()> {
        let msg_bytes: Bytes = Message::new(Some(message)).as_bytes()?;
        self.writer.write(&msg_bytes).await?;
        Ok(())
    }
}

pub struct Subscriber<T> {
    topic_name: String,
    reader: HubMQReader,
    phantom_msg_type: PhantomData<T>,
}

impl<T> Subscriber<T>
where
    T: Serialize + DeserializeOwned,
{
    pub async fn new(topic_name: &str) -> Result<Self> {
        let stream = TcpStream::connect("127.0.0.1:8080").await?;
        let mut writer = HubMQWriter::new(stream);
        let greeting = Message::new(Some(NodeEntity::Subscriber {
            topic_name: String::from(topic_name),
        }));
        writer.write(&greeting.as_bytes()?).await?;
        let reader = HubMQReader::new(writer.into_inner());
        Ok(Self {
            topic_name: String::from(topic_name),
            reader,
            phantom_msg_type: PhantomData,
        })
    }

    pub fn topic(&self) -> &str {
        &self.topic_name
    }

    pub async fn listen<F>(&mut self, callback: F) -> Result<()>
    where
        F: Fn(&Message<T>) -> (),
    {
        loop {
            match self.reader.read().await {
                Ok(buf) => callback(&Message::from_bytes(&buf)?),
                Err(e) => return Err(HubbubError::from(e)),
            };
        }
    }
}
