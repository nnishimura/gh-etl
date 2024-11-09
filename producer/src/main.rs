use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use tokio;
use reqwest::Client;
use std::env;
use dotenv::dotenv;
use crate::producer::{fetch_issues, publish_to_kafka, GitHubIssue};
use crate::setup::ensure_topic;

mod producer;
mod setup;

#[tokio::main]
async fn main() {
    dotenv().ok();

    let github_owner = env::var("GITHUB_OWNER").unwrap();
    let github_repo = env::var("GITHUB_REPO").unwrap(); 
    let broker = "localhost:9092";
    let topic = "gh-issues";
    let client = Client::new();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", broker)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let _ = ensure_topic(broker, topic).await;

    match fetch_issues(&client, &github_owner, &github_repo).await {
        Ok(res) => {
            let issues: Vec<GitHubIssue> = serde_json::from_str(&res).unwrap();
            for issue in issues {
                if let Err(e) = publish_to_kafka(&producer, topic, &issue).await {
                    eprintln!("Failed to publish issue {:?}", e);
                } else {
                    println!("Published issue to Kafka");
                }
            }
        }
        Err(e) => eprintln!("Failed to fetch issues from GitHub: {:?}", e),
    }
}
