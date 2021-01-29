#![allow(dead_code, unused_imports, unused_variables)]
use chrono::{Duration, Utc};

use hubbublib::hcl::Node;
use hubbublib::msg::Message;

#[derive(Debug, Copy, Clone)]
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

    let mut msg_counter = Counter { count: 0 };
    let node = Node::new("Listener");
    let mut sub = node.create_subscriber(&topic).await.unwrap();
    sub.listen(|msg| msg_counter.count_and_echo(msg))
        .await
        .unwrap();
    // tokio::spawn(async move {
    //     sub.listen(echo_cb).await.unwrap();
    // });
    println!("Node '{}' is listening...", node.name());
    loop {}
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
