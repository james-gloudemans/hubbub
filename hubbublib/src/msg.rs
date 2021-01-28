use std::str::from_utf8;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

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

pub type Result<T> = std::result::Result<T, MessageError>;

#[derive(Serialize, Deserialize, Debug)]
pub struct Message<T> {
    header: Header,
    data: Option<T>,
}

impl<T> Message<T>
where
    T: Serialize + DeserializeOwned,
{
    pub fn new(data: Option<T>) -> Self {
        Self {
            header: Header::new(),
            data,
        }
    }

    pub fn from_bytes(buf: &Bytes) -> Result<Self> {
        Ok(serde_json::from_str(from_utf8(buf)?)?)
    }

    pub fn as_bytes(&self) -> Result<Bytes> {
        Ok(Bytes::from(serde_json::to_string(self).map(|s| s + "\n")?))
    }

    pub fn data(&self) -> Option<&T> {
        self.data.as_ref()
    }

    pub fn stamp(&self) -> DateTime<Utc> {
        self.header.stamp
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Header {
    stamp: DateTime<Utc>,
}

impl Header {
    fn new() -> Self {
        Self { stamp: Utc::now() }
    }
}
