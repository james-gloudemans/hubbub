#![allow(dead_code, unused_imports, unused_variables)]
use std::sync::{Arc, Mutex};

use chrono::{Duration, Utc};
use tokio::time;

use hubbublib::hcl::{Node, Receiver};
use hubbublib::msg::Message;

#[derive(Debug)]
pub struct Counter {
    count: usize,
}

impl Counter {
    pub fn count_and_echo(&mut self, msg: &Message<String>) {
        self.count += 1;
        println!(
            "Received message: '{}', count = {}",
            msg.data().unwrap_or(&String::from("")),
            self.count
        );
    }

    pub fn increment_count(&mut self, msg: &Message<String>) {
        self.count += 1;
    }

    pub fn count_latency_and_echo(&mut self, msg: &Message<String>) {
        self.count += 1;
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
            "Received message: '{}', count: {}, latency: {}",
            msg.data().unwrap_or(&String::from("")),
            self.count,
            latency_disp
        );
    }

    pub fn count(&self) -> &usize {
        &self.count
    }
}

impl Receiver<String> for Counter {
    fn callback(&mut self, msg: &Message<String>) {
        self.increment_count(msg);
    }
}

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

    let node = Node::new("Listener");
    let msg_counter = Counter { count: 0 };
    let sub = node.create_subscriber(topic, msg_counter).await.unwrap();
    println!("Node '{}' is listening...", node.name());
    loop {
        time::sleep(time::Duration::from_secs(1)).await;
        let counter = sub.get();
        println!("Current count is {}", counter.count());
    }
}

fn echo_cb(msg: &Message<String>) {
    println!(
        "Received message: '{}'",
        msg.data().unwrap_or(&String::from(""))
    );
}

fn latency_cb(msg: &Message<String>) {
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
