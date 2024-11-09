use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaResult;
use rdkafka::util::Timeout;

pub async fn ensure_topic(broker: &str, topic: &str) -> KafkaResult<()> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", broker)
        .create()
        .expect("Failed to create admin client");

    let metadata = admin.inner().fetch_metadata(Some(topic), Timeout::Never)?;
    if metadata.topics().is_empty() {
        println!("Topic '{}' does not exist. Creating with cleanup.policy=compact,delete", topic);

        let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1))
            .set("cleanup.policy", "compact,delete");
        admin.create_topics(&[new_topic], &AdminOptions::new()).await?;
    } else {
        println!("Topic '{}' exists.", topic);
    }

    Ok(())
}
