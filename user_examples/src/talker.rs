#![allow(dead_code, unused_imports, unused_variables)]
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::time;

use hubbublib::hcl::Publisher;

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

    // To be replaced by client code
    let mut publ: Publisher<String> = Publisher::new(&topic).await.unwrap();
    for i in 1u32.. {
        time::sleep(time::Duration::from_secs(1)).await;
        let next_msg = format!("{} {}", message, i);
        println!("Sending message: '{}'", &next_msg);
        publ.publish(next_msg).await.unwrap();
    }
}
