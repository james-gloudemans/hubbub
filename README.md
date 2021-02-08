# Hubbub
Pub/Sub any Rust data structure over TCP.

## Problems
1. There is no correspondence of topic with a type.  
   - At the moment, a malicious publisher could intentionally publish the wrong type on a topic, and all the subscribers would panic upon receiving the message.  This could easily be fixed by having subscribers ignore messages that don't deserialize correctly, but that would just make the type of the messages like an extra filter within each topic.  It would be better if publishing a message of wrong type panicked the publisher.
   - This problem also affects introspection, as the Hub has no information about the type of messages being published on a topic.
   - How would a CLI tool subscribe to a topic and print messages to the terminal?  It could print the JSON string, but it would be better if it looked like a Rust object (i.e. `Debug` printing with `"{:?}"`).
     - I think I will just use JSON pretty printing or YAML to print messages.  No need to overcomplicate this.
   - Could this be solved using [serde-reflection](https://github.com/novifinancial/serde-reflection)?


