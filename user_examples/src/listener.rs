#![allow(dead_code, unused_imports, unused_variables)]
use bytes::BytesMut;
use chrono::{Duration, Utc};
use std::str::from_utf8;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

use hubbublib::hcl::Subscriber;
use hubbublib::msg::Message;

#[tokio::main]
async fn main() {
    // CL arg parsing
    let args: Vec<String> = std::env::args().collect();
    let topic: String;
    match args.len() {
        2 => {
            topic = args[1].to_owned();
        }
        _ => {
            panic!("Usage: listener <topic name>");
        }
    }

    let mut sub: Subscriber<String> = Subscriber::new(&topic).await.unwrap();
    tokio::spawn(async move {
        sub.listen(latency).await.unwrap();
    });
    loop {}
}

fn latency(msg: &Message<String>) {
    let latency: Duration = Utc::now() - msg.stamp();
    let latency_disp: String;
    if latency < Duration::milliseconds(1) {
        latency_disp = format!("{} us", latency.num_microseconds().unwrap());
    } else if latency < Duration::seconds(1) {
        latency_disp = format!("{} ms", latency.num_milliseconds());
    } else {
        latency_disp = format!("{} s", latency.num_seconds());
    }
    println!(
        "Received message: '{}', latency: {}",
        msg.data().unwrap_or(&String::from("")),
        latency_disp
    );
}