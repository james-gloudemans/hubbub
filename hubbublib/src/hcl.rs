//! # Hubbub Client Library
// #![allow(dead_code, unused_imports, unused_variables)]
use std::collections::HashSet;
use std::marker::PhantomData;
use std::ops::Deref;
use std::str::from_utf8;
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::Bytes;
use dashmap::DashSet;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

use crate::hub::Hub;
use crate::msg::{Message, MessageError};
use crate::{HubEntity, HubReader, HubWriter};

/// A node in the Hubbub network
#[derive(Debug)]
pub struct Node {
    name: String,
    subscriptions: DashSet<String>,
    publishers: DashSet<String>,
}

impl Node {
    /// Construct a new [`Node`] with the given `name`.
    pub fn new(name: &str) -> Self {
        // TODO: Connect to Hub and have master track nodes for introspection.
        // TODO Idea: Upon connection to Hub, keep stream and share it with all
        // connected entities.
        Self {
            name: String::from(name),
            subscriptions: DashSet::new(),
            publishers: DashSet::new(),
        }
    }

    /// Get the name of a [`Node`].
    pub fn name(&self) -> &str {
        &self.name
    }

    // TODO
    // /// Get an iterator over the topic names for each subscriber on a [`Node`].
    // pub fn subscriptions(&self) -> std::collections::hash_set::Iter<String> {
    //     let mut res: HashSet<String> = HashSet::new();
    //     for topic_name in self.subscriptions.borrow().iter() {
    //         res.insert(String::from(topic_name));
    //     }
    //     res.iter();
    // }

    // /// Get an iterator over the topic names for each publisher on a [`Node`].
    // pub fn publishers(&self) -> std::collections::hash_set::Iter<String> {
    //     self.publishers.borrow().iter()
    // }

    /// Create a new publisher on this [`Node`] and return it.
    pub async fn create_publisher<M>(&self, topic: &str) -> Result<Publisher<M>>
    where
        M: Serialize + DeserializeOwned,
    {
        if self.publishers.insert(String::from(topic)) {
            Ok(Publisher::new(self.name(), topic).await?)
        } else {
            Err(HubbubError::DuplicatePublisher {
                topic: String::from(topic),
            })
        }
    }

    /// Create a new subscriber on this [`Node`] and return it.
    pub async fn create_subscriber<M, R>(&self, topic: String, receiver: R) -> Result<UserData<R>>
    where
        M: Serialize + DeserializeOwned + Send + 'static,
        R: Receiver<M> + Send + 'static,
    {
        if self.subscriptions.insert(String::from(&topic)) {
            let receiver = Arc::new(Mutex::new(receiver));
            let mut sub = Subscriber::new(String::from(self.name()), topic, Arc::clone(&receiver))
                .await
                .unwrap();
            Ok(UserData::new(
                tokio::spawn(async move {
                    sub.listen().await.unwrap();
                }),
                Arc::clone(&receiver),
            ))
        } else {
            Err(HubbubError::DuplicateSubscriber {
                topic: String::from(topic),
            })
        }
    }
}

pub struct UserDataRef<'a, T>(MutexGuard<'a, T>);

impl<'a, T> Deref for UserDataRef<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

/// An entity that can publish [`Message<T>`]s to all [`Subscriber`]s on a topic.
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
    node_name: String,
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
    // Might need to wrap the topic in an [`Arc`]
    /// Construct a [`Publisher<T>`] to publish [`Message<T>`]s to the topic named `topic_name`.
    ///
    /// # Errors
    /// Returns [`Err`] if it fails to connect to the `Hub`.
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
    async fn new(node_name: &str, topic_name: &str) -> Result<Publisher<T>> {
        let stream = Hub::connect(&Message::new(HubEntity::Publisher {
            node_name: String::from(node_name),
            topic_name: String::from(topic_name),
        }))
        .await
        .unwrap();
        // let stream = TcpStream::connect("127.0.0.1:8080").await?;
        // let mut writer = HubWriter::new(stream);
        // let greeting = Message::new(Some(HubEntity::Publisher {
        //     node_name: String::from(node_name),
        //     topic_name: String::from(topic_name),
        // }));
        // writer.write(&greeting.as_bytes()?).await?;
        Ok(Self {
            node_name: String::from(node_name),
            topic_name: String::from(topic_name),
            writer: HubWriter::new(stream),
            phantom_msg_type: PhantomData,
        })
    }

    /// Get the name of the topic this [`Publisher`] is publishing to.
    pub fn topic(&self) -> &str {
        &self.topic_name
    }

    /// Publish a [`Message<T>`] containing `message` on the connected topic.
    ///
    /// # Errors
    /// Returns [`Err`] if the message fails to serialize or if the underlying write
    /// operations fail (typically due to disconnection).
    pub async fn publish(&mut self, message: T) -> Result<()> {
        let msg_bytes: Bytes = Message::new(Some(message)).as_bytes()?;
        self.writer.write(&msg_bytes).await?;
        Ok(())
    }
}

/// An entity than can listen for [`Message<T>`]s published on a topic.
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
pub struct Subscriber<M, R>
where
    R: Receiver<M>,
{
    receiver: Arc<Mutex<R>>,
    node_name: String,
    topic_name: String,
    reader: HubReader,
    phantom_msg_type: PhantomData<M>,
}

impl<M, R> Subscriber<M, R>
where
    M: Serialize + DeserializeOwned + Send,
    R: Receiver<M> + Send,
{
    /// Construct a [`Subscriber<T>`] to listen for [`Message<T>`]s on a given topic.
    ///
    /// # Errors
    /// Returns [`Err`] if it fails to connect to the `Hub`.
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
    async fn new(
        node_name: String,
        topic_name: String,
        receiver: Arc<Mutex<R>>,
    ) -> Result<Subscriber<M, R>> {
        let stream = TcpStream::connect("127.0.0.1:8080").await?;
        let mut writer = HubWriter::new(stream);
        let greeting = Message::new(Some(HubEntity::Subscriber {
            node_name: String::from(&node_name),
            topic_name: String::from(&topic_name),
        }));
        writer.write(&greeting.as_bytes()?).await?;
        let reader = HubReader::new(writer.into_inner());
        Ok(Self {
            receiver,
            node_name,
            topic_name,
            reader,
            phantom_msg_type: PhantomData,
        })
    }

    /// Get the name of the topic a [`Subscriber`] is connected to.
    pub fn topic(&self) -> &str {
        &self.topic_name
    }

    /// Listen for messages and process them with the given `callback` function.
    ///
    /// # Errors
    /// Returns [`Err`] if the message fails to deserialize or if the underlying read
    /// operations fail (typically due to disconnection).
    pub async fn listen(&mut self) -> Result<()> {
        loop {
            match self.reader.read().await {
                Ok(buf) => self
                    .receiver
                    .lock()
                    .unwrap()
                    .callback(&Message::from_bytes(&buf).unwrap()),
                Err(e) => {
                    return Err(HubbubError::from(e));
                }
            };
        }
    }
}

/// An object that can receive messages and proccess them with a callback.
pub trait Receiver<M> {
    fn callback(&mut self, msg: &Message<M>);
}

/// Node state data that can be mutated in a [`Receiver`] callback.
pub struct UserData<T> {
    thread_handle: JoinHandle<()>,
    userdata: Arc<Mutex<T>>,
}

impl<T> UserData<T> {
    pub fn new(thread_handle: JoinHandle<()>, userdata: Arc<Mutex<T>>) -> Self {
        Self {
            thread_handle,
            userdata,
        }
    }

    pub fn get<'a>(&'a self) -> UserDataRef<'a, T> {
        let ud = self.userdata.lock().unwrap();
        UserDataRef(ud)
    }
}

/// Error type representing all possible errors that can occur in the Hubbub Client Library
#[derive(Debug)]
pub enum HubbubError {
    IoError(tokio::io::Error),
    MessageError(MessageError),
    DuplicatePublisher { topic: String },
    DuplicateSubscriber { topic: String },
    // NotImplemented,
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

/// A specialized [`Result`] type for Hubbub message exchange operations.
pub type Result<T> = std::result::Result<T, HubbubError>;
