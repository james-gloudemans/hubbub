use std::sync::Arc;

use hubbublib::hub::Hub;

#[tokio::main]
async fn main() {
    let hub = Arc::new(Hub::new("127.0.0.1:8080").await);
    println!("Hub listening at {}", hub.address());
    loop {
        let (stream, _) = hub.listen().await.unwrap();
        // println!("Accepting connecton from client at: {}", client_addr);
        let hub = Arc::clone(&hub);
        tokio::spawn(async move {
            hub.process_new_entity(stream).await;
        });
    }
}
