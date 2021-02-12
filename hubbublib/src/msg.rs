//! # Definition of Hubbub messages
use std::str::from_utf8;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_reflection::{ContainerFormat, Samples, Tracer, TracerConfig};

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
pub struct Header {
    #[serde(with = "timestamp")]
    stamp: DateTime<Utc>,
}

impl Header {
    /// Construct a new header and stamp it.
    fn new() -> Self {
        // TODO: move the stamping to just before a message is sent.
        Self { stamp: Utc::now() }
    }
}

mod timestamp {
    use chrono::{DateTime, TimeZone, Utc};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    struct TimeStamp(i64);

    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        TimeStamp(date.timestamp_millis()).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        TimeStamp::deserialize(deserializer).map(|time| Utc.timestamp_millis(time.0))
    }
}

/// A type for representing [`Message`] type formats.  This is used, e.g., to enforce
/// type consistency among all [`Publisher`]s and [`Subscribers`] on a [`Topic`].
///
/// The schema is a YAML string which is obtained using the [`serde_reflection`] crate.
/// It is obtained by using that crate's `trace_type` function on [`Message<T>`],
/// then stripping out information about the `header` field to obtain the part that
/// describes the `data` field.  This is not tested very well and it is unknown if these
/// YAML string can serve as good runtime type identifiers across processes.
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub struct MessageSchema(String);

impl MessageSchema {
    /// Create a new `MessageSchema` for the given deserializable type `T`.
    pub fn new<T>() -> Result<Self>
    where
        T: DeserializeOwned,
    {
        let mut tracer = Tracer::new(TracerConfig::default());
        let samples = Samples::new();
        tracer.trace_type::<Message<T>>(&samples)?;
        let registry = tracer.registry()?;
        // Strip the `header` field from `Message` and just display the schema of the
        // `data` field
        let schema = if let ContainerFormat::Struct(fields) = &registry["Message"] {
            serde_yaml::to_string(&fields[1].value).unwrap()
        } else {
            serde_yaml::to_string(&()).unwrap()
        };
        Ok(Self(schema))
    }
}

use std::fmt;
impl fmt::Display for MessageSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Error type representing all possible errors that can occur with [`Message<T>`]s
/// and related functions and methods.
#[derive(Debug)]
pub enum MessageError {
    SerdeError(serde_json::Error),
    Utf8Error(std::str::Utf8Error),
    ReflectionError(serde_reflection::Error),
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

impl From<serde_reflection::Error> for MessageError {
    fn from(e: serde_reflection::Error) -> Self {
        MessageError::ReflectionError(e)
    }
}

/// A specialized [`Result`] type for [`Message<T>`] operations.
pub type Result<T> = std::result::Result<T, MessageError>;
