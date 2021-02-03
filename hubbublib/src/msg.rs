//! # Definition of Hubbub messages
use std::str::from_utf8;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// A message used to exhange Rust data structures between Hubbub nodes.
///
/// Can contain any type that derives [`serde`]'s [`Serialize`] and [`Deserialize`] traits,
/// or empty (`()`).
#[derive(Serialize, Deserialize, Debug)]
pub struct Message<T> {
    header: Header,
    data: T,
}

impl<T> Message<T>
where
    T: Serialize + DeserializeOwned,
{
    /// Construct a new [`Message<T>`].
    ///
    /// Messages should be sent as soon as possible after they are created to ensure
    /// that they receive an accurate timestamp.
    /// # Examples
    /// ```
    /// use hubbublib::msg::Message;
    ///
    /// let msg: Message<String> = Message::new(Some(String::from("Hello Hubbub!")));
    /// // Empty messages are created with the `()` type.
    /// let empty: Message<()> = Message::new(());
    /// ```
    pub fn new(data: T) -> Self {
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
    ///
    /// # Errors
    /// Returns [`Err` ]if the [`Message<T>`] fails to serialize.
    pub fn as_bytes(&self) -> Result<Bytes> {
        // Ok(serde_json::to_string(self).map(|s| Bytes::from(s + "\n"))?)
        Ok(serde_json::to_vec(self).map(|mut s| {
            s.push(b'\n' as u8);
            Bytes::from(s)
        })?)
    }

    /// Get a JSON string representation of a [`Message<T>`]
    ///
    /// Can be used for printing if `Debug` is not derived for `T`
    ///
    /// # Errors
    /// Returns [`Err` ]if the [`Message<T>`] fails to serialize.
    pub fn as_str(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    /// Get a JSON string representation of a [`Message<T>`] for pretty printing
    ///
    /// Can be used for printing if `Debug` is not derived for `T`
    ///
    /// # Errors
    /// Returns [`Err` ]if the [`Message<T>`] fails to serialize.
    pub fn as_str_pretty(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    /// Get a reference to the data held in a [`Message<T>`].
    pub fn data(&self) -> &T {
        &self.data
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
