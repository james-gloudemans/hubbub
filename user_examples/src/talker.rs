#![allow(dead_code, unused_imports, unused_variables)]
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::time;

use hubbublib::hcl::Node;

#[tokio::main]
async fn main() {
    // CL arg parsing
    let args: Vec<String> = std::env::args().collect();
    let message: String;
    let topic: String;
    match args.len() {
        3 => {
            topic = args[1].to_owned();
            message = args[2].to_owned();
        }
        _ => {
            panic!("Usage: talker <topic name> <message>");
        }
    }

    let node = Node::new("Talker");
    let mut publ = node.create_publisher(&topic).await.unwrap();
    for i in 1u32.. {
        time::sleep(time::Duration::from_millis(100)).await;
        let next_msg = format!("{}", message);
        println!("Sending message: '{}'", &next_msg);
        publ.publish(next_msg).await.unwrap();
    }
}
