#![allow(unused_variables)]

extern crate clap;
extern crate hubbublib;
use clap::{App, AppSettings, SubCommand};
use hubbublib::hub::Hub;
use hubbublib::msg::Message;
use hubbublib::HubEntity;

fn main() {
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
    if let Some(matches) = matches.subcommand_matches("topic") {
        if let Some(matches) = matches.subcommand_matches("list") {
            println!("topic list");
        }
    } else if let Some(matches) = matches.subcommand_matches("node") {
        if let Some(matches) = matches.subcommand_matches("list") {
            println!("node list");
        }
    }
}
