#![allow(unused_variables)]

extern crate clap;
extern crate hubbublib;

use std::collections::HashSet;

use clap::{App, AppSettings, Arg, SubCommand};
use tokio::io::{AsyncBufReadExt, BufReader};

use hubbublib::hub::Hub;
use hubbublib::msg::Message;
use hubbublib::{HubEntity, HubRequest};

#[tokio::main]
async fn main() {
    // CLI definition
    let matches = App::new("hub")
        .version("0.1")
        .author("James Gloudemans <james.gloudemans@gmail.com")
        .about("Interact with Hubbub")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(
            SubCommand::with_name("topic")
                .about("Inspect and interact with Hubbub topics.")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .subcommand(
                    SubCommand::with_name("list").about("List all the topics in the Hubbub graph."),
                )
                .subcommand(
                    SubCommand::with_name("schema")
                        .about("Get the message schema for a topic.")
                        .arg(
                            Arg::with_name("TOPIC")
                                .help("Sets the name of the topic.")
                                .required(true)
                                .index(1),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("info")
                        .about("Get detailed info about a topic.")
                        .arg(
                            Arg::with_name("TOPIC")
                                .help("The name of the topic.")
                                .required(true)
                                .index(1),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("echo")
                        .about("Echo messages being sent on a topic.")
                        .arg(
                            Arg::with_name("TOPIC")
                                .help("The name of the topic.")
                                .required(true)
                                .index(1),
                        ),
                ),
        )
        .subcommand(
            SubCommand::with_name("node")
                .about("Inspect nodes in the Hubbub graph.")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .subcommand(
                    SubCommand::with_name("list").about("List all the nodes in the Hubbub graph."),
                )
                .subcommand(
                    SubCommand::with_name("info")
                        .about("Get detailed info about a node.")
                        .arg(
                            Arg::with_name("NODE")
                                .help("Sets the name of the node.")
                                .required(true)
                                .index(1),
                        ),
                ),
        )
        .get_matches();

    // Arg / subcommand matching
    if let Some(matches) = matches.subcommand_matches("topic") {
        if let Some(matches) = matches.subcommand_matches("list") {
            let req = HubRequest::TopicList;
            let response: HashSet<String> =
                serde_json::from_str(&request(req).await).expect("Malformed response from server.");
            for topic in response {
                println!("{}", topic);
            }
        } else if let Some(matches) = matches.subcommand_matches("schema") {
            let topic = matches.value_of("TOPIC").unwrap();
            let req = HubRequest::TopicSchema(topic.to_owned());
            let response: String =
                serde_json::from_str(&request(req).await).expect("Malformed response from server.");
            println!("Message schema for '{}':", topic);
            println!("{}", response);
        } else if let Some(matches) = matches.subcommand_matches("info") {
            let topic = matches.value_of("TOPIC").unwrap();
            let req = HubRequest::TopicInfo(topic.to_owned());
            let response: (HashSet<String>, HashSet<String>) =
                serde_json::from_str(&request(req).await).expect("Malformed response from server.");
            println!("Info for topic '{}':", topic);
            println!("Publishers:");
            for node in response.0 {
                println!("\t{}", node);
            }
            println!("Subscribers:");
            for node in response.1 {
                println!("\t{}", node);
            }
        } else if let Some(matches) = matches.subcommand_matches("echo") {
            let topic = matches.value_of("TOPIC").unwrap();
            let request = Message::new(HubEntity::Cli(HubRequest::TopicEcho(topic.to_owned())));
            let stream = Hub::connect(&request).await.unwrap();
            let mut reader = BufReader::new(stream);
            loop {
                let mut buf = String::new();
                reader.read_line(&mut buf).await.unwrap();
                let data: serde_json::Value = serde_json::from_str(&buf).unwrap();
                println!("{}", data["data"]);
            }
        }
    } else if let Some(matches) = matches.subcommand_matches("node") {
        if let Some(matches) = matches.subcommand_matches("list") {
            let req = HubRequest::NodeList;
            let response: HashSet<String> =
                serde_json::from_str(&request(req).await).expect("Malformed response from server.");
            for node in response {
                println!("{}", node);
            }
        } else if let Some(matches) = matches.subcommand_matches("info") {
            let node = matches.value_of("NODE").unwrap();
            let req = HubRequest::NodeInfo(node.to_owned());
            let response: (HashSet<String>, HashSet<String>) =
                serde_json::from_str(&request(req).await).expect("Malformed response from server.");
            println!("Info for node '{}':", node);
            println!("Publishes on topics:");
            for topic in response.0 {
                println!("\t{}", topic);
            }
            println!("Subscribes to topics:");
            for topic in response.1 {
                println!("\t{}", topic);
            }
        }
    }
}

/// Boilerplate for sending request to Hub and waiting for reply.
async fn request(req: HubRequest) -> String {
    let request = Message::new(HubEntity::Cli(req));
    let stream = Hub::connect(&request).await.unwrap();
    let mut reader = BufReader::new(stream);
    let mut buf = String::new();
    reader.read_line(&mut buf).await.unwrap();
    buf
}
