use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use serde::{Deserialize, Serialize};
use reqwest::Client;
use std::env;
use serde_with::skip_serializing_none;

#[skip_serializing_none]
#[derive(Deserialize, Serialize, Debug)]
pub struct GitHubIssue {
    id: u64,
    title: String,
    state: String,
    number: u64,
}

pub async fn fetch_issues(client: &Client, owner: &str, repo: &str) -> Result<String, reqwest::Error> {
    let url = format!("https://api.github.com/repos/{}/{}/issues", owner, repo);
    let response = client
        .get(&url)
        .header("Accept", "application/vnd.github.v3+json")
        .header("User-Agent", "gh-etl")
        .header("Authorization", format!("Bearer {}", env::var("GITHUB_TOKEN").unwrap()))
        .send()
        .await?;

        let body = response.text().await?;
        println!("Response body: {}", body);
        Ok(body)
}

pub async fn publish_to_kafka(producer: &FutureProducer, topic: &str, issue: &GitHubIssue) -> Result<(), rdkafka::error::KafkaError> {
    let payload = serde_json::to_string(issue).unwrap();
    
    producer
        .send(
            FutureRecord::to(topic)
                .payload(&payload)
                .key(&issue.id.to_string()),
            Duration::from_secs(0),
        )
        .await
        .map(|_| ())
        .map_err(|(e, _)| e)
}

