use rdkafka::config::ClientConfig;
use rdkafka::consumer::CommitMode;
use rdkafka::consumer::{Consumer, StreamConsumer};
use futures_util::StreamExt;
use rdkafka::Message;

pub async fn consume_messages(broker: &str, group_id: &str, topic: &str) -> Result<(), rdkafka::error::KafkaError> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", broker)
        .set("group.id", group_id)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("partition.assignment.strategy", "roundrobin")
        .create()?;

    consumer.subscribe(&[topic])?;

    println!("Consuming messages from topic '{}'", topic);

    let mut message_stream = consumer.stream();
    while let Some(message_result) = message_stream.next().await {
        match message_result {
            Ok(message) => {
                if let Some(payload) = message.payload() {
                    println!("Received message: {}", String::from_utf8_lossy(payload));
                }
                // Manually commit the message offset
                consumer.commit_message(&message, CommitMode::Async)?;
            }
            Err(e) => {
                eprintln!("Error receiving message: {:?}", e);
            }
        }
    }

    Ok(())
}
