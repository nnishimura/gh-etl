use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use tokio;
use reqwest::Client;
use dotenv::dotenv;
use crate::consumer::consume_messages;

mod consumer;

#[tokio::main]
async fn main() {
    dotenv().ok();

    let github_owner = "nnishimura";
    let github_repo = "ubc-python"; 
    let broker = "localhost:9092";
    let topic = "gh-issues";
    let group_id = "example_group"; 

    match consume_messages(broker, group_id, topic).await {
        Ok(_) => println!("Message consumed successfully."),
        Err(e) => eprintln!("Failed to consume message: {:?}", e),
    }
}
