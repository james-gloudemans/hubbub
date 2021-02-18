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
                ),
        )
        .subcommand(
            SubCommand::with_name("node")
                .about("Inspect nodes in the Hubbub graph.")
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .subcommand(
                    SubCommand::with_name("list").about("List all the nodes in the Hubbub graph."),
                ),
        )
        .get_matches();

    // Arg / subcommand matching
    // TODO: lots of repitition, need abstraction.
    if let Some(matches) = matches.subcommand_matches("topic") {
        if let Some(matches) = matches.subcommand_matches("list") {
            let request = Message::new(HubEntity::Cli(HubRequest::TopicList));
            let stream = Hub::connect(&request).await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut buf = String::new();
            reader.read_line(&mut buf).await.unwrap();
            let response: HashSet<String> =
                serde_json::from_str(&buf).expect("Malformed response from server.");
            for topic in response {
                println!("{}", topic);
            }
        } else if let Some(matches) = matches.subcommand_matches("schema") {
            let topic = matches.value_of("TOPIC").unwrap();
            let request = Message::new(HubEntity::Cli(HubRequest::TopicSchema(topic.to_owned())));
            let stream = Hub::connect(&request).await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut buf = String::new();
            reader.read_line(&mut buf).await.unwrap();
            let response: String =
                serde_json::from_str(&buf).expect("Malformed response from server.");
            println!("Message schema for '{}':", topic);
            println!("{}", response);
        }
    } else if let Some(matches) = matches.subcommand_matches("node") {
        if let Some(matches) = matches.subcommand_matches("list") {
            let request = Message::new(HubEntity::Cli(HubRequest::NodeList));
            let stream = Hub::connect(&request).await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut buf = String::new();
            reader.read_line(&mut buf).await.unwrap();
            let response: HashSet<String> =
                serde_json::from_str(&buf).expect("Malformed response from server.");
            for node in response {
                println!("{}", node);
            }
        }
    }
}
