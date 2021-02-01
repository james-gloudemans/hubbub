//! # Definition of Hubbub messages
use std::str::from_utf8;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// A message used to exhange Rust data structures between Hubbub nodes.
///
/// Can contain any type that `derive`s [`serde`]'s [`Serialize`] and [`Deserialize`] traits,
/// or empty ([`None`]).
#[derive(Serialize, Deserialize, Debug)]
pub struct Message<T> {
    header: Header,
    data: Option<T>,
}

impl<T> Message<T>
where
    T: Serialize + DeserializeOwned,
{
    // TODO: see if empty messages can be created with Message<()> somehow.
    /// Construct a new [`Message<T>`].
    ///
    /// Messages should be sent as soon as possible after they are created to ensure
    /// that they receive an accurate timestamp.
    /// # Examples
    /// ```
    /// use hubbublib::msg::{Empty, Message};
    ///
    /// let msg: Message<String> = Message::new(Some(String::from("Hello Hubbub!")));
    /// // Empty messages are created this way for now.  Will change if
    /// // experimental type `!` is added to stable Rust.
    /// let empty: Message<Empty> = Message::new(None);
    /// ```
    pub fn new(data: Option<T>) -> Self {
        Self {
            header: Header::new(),
            data,
        }
    }

    /// Construct a new [`Message<T>`] from a `Bytes` buffer.
    ///
    /// # Errors
    /// Returns [`Err` ]if `buf` is not valid UTF8 or if it does not deserialize to
    /// a [`Message<T>`]
    pub fn from_bytes(buf: &Bytes) -> Result<Self> {
        Ok(serde_json::from_str(from_utf8(buf)?)?)
    }

    /// Convert a [`Message<T>`] to a `Bytes` instance which can be sent over the
    /// network to other Hubbub nodes.
    /// # Errors
    /// Returns [`Err` ]if the [`Message<T>`] fails to serialize.
    pub fn as_bytes(&self) -> Result<Bytes> {
        Ok(serde_json::to_string(self).map(|s| Bytes::from(s + "\n"))?)
    }

    /// Get a reference to the data held in a [`Message<T>`] or `None` for empty message.
    pub fn data(&self) -> Option<&T> {
        self.data.as_ref()
    }

    /// Get a [`Message`]'s timestamp corresponding to the time the [`Message`] was created.
    pub fn stamp(&self) -> DateTime<Utc> {
        self.header.stamp
    }
}

/// A header that is attached to every [`Message<T>`].  Currently, just contains a timestamp.
#[derive(Serialize, Deserialize, Debug)]
struct Header {
    stamp: DateTime<Utc>,
}

impl Header {
    /// Construct a new header and stamp it.
    fn new() -> Self {
        // TODO: move the stamping to just before a message is sent.
        Self { stamp: Utc::now() }
    }
}

/// Placeholder type for empty messages.
/// # Examples
/// use hubbublib::msg::{Empty, Message};
///
/// let empty: Message<Empty> = Message::new(None);
#[derive(Serialize, Deserialize, Debug)]
pub struct Empty;

/// Error type representing all possible errors that can occur with [`Message<T>`]s
/// and related functions and methods.
#[derive(Debug)]
pub enum MessageError {
    SerdeError(serde_json::Error),
    Utf8Error(std::str::Utf8Error),
}

impl From<serde_json::Error> for MessageError {
    fn from(e: serde_json::Error) -> Self {
        MessageError::SerdeError(e)
    }
}

impl From<std::str::Utf8Error> for MessageError {
    fn from(e: std::str::Utf8Error) -> Self {
        MessageError::Utf8Error(e)
    }
}

/// A specialized [`Result`] type for [`Message<T>`] operations.
pub type Result<T> = std::result::Result<T, MessageError>;
