//! # Hubbub Client Library
// #![allow(dead_code, unused_imports, unused_variables)]
use std::marker::PhantomData;
use std::str::from_utf8;

use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::net::TcpStream;

use crate::msg::{Message, MessageError};
use crate::{HubReader, HubWriter, NodeEntity};

/// An entity that can publish `Message<T>`s to all `Subscriber`s on a topic.
///
/// # Examples
/// This example takes a topic name and a message from the command line, then publishes
/// the message, along with a counter, once per second
/// ```
/// use tokio::io::{AsyncWriteExt, BufWriter};
/// use tokio::net::TcpStream;
/// use tokio::time;
///
/// use hubbublib::hcl::Publisher;

/// #[tokio::main]
/// async fn main() {
///     // CL arg parsing
///     let args: Vec<String> = std::env::args().collect();
///     let message: String;
///     let topic: String;
///     match args.len() {
///         3 => {
///             topic = args[1].to_owned();
///             message = args[2].to_owned();
///         }
///         _ => {
///             panic!("Usage: talker <topic name> <message>");
///         }
///     }
///
///     let mut publ: Publisher<String> = Publisher::new(&topic).await.unwrap();
///     for i in 1u32.. {
///         time::sleep(time::Duration::from_secs(1)).await;
///         let next_msg = format!("{} {}", message, i);
///         println!("Sending message: '{}'", &next_msg);
///         publ.publish(next_msg).await.unwrap();
///     }
/// }
/// ```
pub struct Publisher<T> {
    topic_name: String,
    writer: HubWriter,
    phantom_msg_type: PhantomData<T>,
}

impl<T> Publisher<T>
where
    T: Serialize + DeserializeOwned,
{
    // TODO Idea: could the Hub send us back a reference to the topic?
    // Then, we could publish directly to the subscriber's streams
    // without needing to communicate through the master.
    // Might need to wrap the topic in an `Arc`
    /// Construct a `Publisher<T>` to publish `Message<T>`s to the topic named `topic_name`.
    ///
    /// # Errors
    /// Returns `Err` if it fails to connect to the `Hub`.
    ///
    /// # Examples
    /// ```
    /// use hubbublib::hcl::Publisher;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut publ: Publisher<String> = Publisher::new("topic").await.expect("Failed to connect to the Hub.");
    /// }
    /// ```
    pub async fn new(topic_name: &str) -> Result<Self> {
        let stream = TcpStream::connect("127.0.0.1:8080").await?;
        let mut writer = HubWriter::new(stream);
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

    /// Get the name of the topic this `Publisher` is publishing to.
    pub fn topic(&self) -> &str {
        &self.topic_name
    }

    /// Publish a `Message<T>` containing `message` on the connected topic.
    ///
    /// # Errors
    /// Returns `Err` if the message fails to serialize or if the underlying write
    /// operations fail (typically due to disconnection).
    pub async fn publish(&mut self, message: T) -> Result<()> {
        let msg_bytes: Bytes = Message::new(Some(message)).as_bytes()?;
        self.writer.write(&msg_bytes).await?;
        Ok(())
    }
}

/// An entity than can listen for `Message<T>`s published on a topic.
///
/// # Examples
/// This example takes a topic name from the command line and prints it to stdout.
/// ```
/// use hubbublib::hcl::Subscriber;
/// use hubbublib::msg::Message;

/// #[tokio::main]
/// async fn main() {
///     // CL arg parsing
///     let args: Vec<String> = std::env::args().collect();
///     let topic: String;
///     match args.len() {
///         2 => {
///             topic = args[1].to_owned();
///         }
///         _ => {
///             panic!("Usage: listener <topic name>");
///         }
///     }

///     let mut sub: Subscriber<String> = Subscriber::new(&topic).await.unwrap();
///     tokio::spawn(async move {
///         sub.listen(echo_cb).await.unwrap();
///     });
///     loop {}
/// }
///
/// fn echo_cb(msg: &Message<String>) {
///     println!("Received message: '{}'", msg.data().unwrap_or(&String::from("")));
/// }
/// ```
pub struct Subscriber<T> {
    topic_name: String,
    reader: HubReader,
    phantom_msg_type: PhantomData<T>,
}

impl<T> Subscriber<T>
where
    T: Serialize + DeserializeOwned,
{
    /// Construct a `Subscriber<T>` to listen for `Message<T>`s on a given topic.
    ///
    /// # Errors
    /// Returns `Err` if it fails to connect to the `Hub`.
    ///
    /// # Examples
    /// ```
    /// use hubbublib::hcl::Subscriber;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut publ: Subscriber<String> = Subscriber::new("topic").await.expect("Failed to connect to the Hub.");
    /// }
    /// ```
    pub async fn new(topic_name: &str) -> Result<Self> {
        let stream = TcpStream::connect("127.0.0.1:8080").await?;
        let mut writer = HubWriter::new(stream);
        let greeting = Message::new(Some(NodeEntity::Subscriber {
            topic_name: String::from(topic_name),
        }));
        writer.write(&greeting.as_bytes()?).await?;
        let reader = HubReader::new(writer.into_inner());
        Ok(Self {
            topic_name: String::from(topic_name),
            reader,
            phantom_msg_type: PhantomData,
        })
    }

    /// Get the name of the topic a `Subscriber` is connected to.
    pub fn topic(&self) -> &str {
        &self.topic_name
    }

    /// Listen for messages and process them with the given `callback` function.
    ///
    /// # Errors
    /// Returns `Err` if the message fails to deserialize or if the underlying read
    /// operations fail (typically due to disconnection).
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

/// Error type representing all possible errors that can occur with exchanging messages over
/// a network with Hubbub.
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

/// A specialized `Result` type for Hubbub message exchange operations.
pub type Result<T> = std::result::Result<T, HubbubError>;
