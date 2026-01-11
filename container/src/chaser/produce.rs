use apache_avro::{to_avro_datum, to_value, Schema};
use chrono::{DateTime, Utc};
use log::info;
use rdkafka::{
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use schema_registry_converter::schema_registry_common::get_payload;

use crate::schemas::chaser::Chaser;
use std::time::Duration;

pub(crate) async fn produce(
    brokers: &str,
    topic_name: &str,
    schema: &Schema,
    id: u32,
    count: u32,
    ttl: u32,
    msg_id: &str,
) {
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    // This loop is non blocking: all messages will be sent one after the other, without waiting
    // for the results.
    let futures = (0..count)
        .map(|i| async move {
            // The send operation on the topic returns a future, which will be
            // completed once the result or failure from Kafka is received.

            let utc_now: DateTime<Utc> = Utc::now();

            let user = Chaser {
                name: msg_id.to_owned(),
                id: format!("{msg_id}-{i}"),
                ttl,
                sent: utc_now.timestamp_nanos_opt().unwrap(),
                previous: None,
                // b: "ABC".to_string(),
            };

            let avro_value = to_value(user).unwrap();
            let avro_bytes = to_avro_datum(schema, avro_value).unwrap();
            let avro_payload = get_payload(id, avro_bytes);

            let delivery_status = producer
                .send(
                    FutureRecord::to(topic_name)
                        .payload(&avro_payload)
                        .key(&format!("Key-{i}"))
                        .headers(OwnedHeaders::new().insert(Header {
                            key: "header_key",
                            value: Some("header_value"),
                        })),
                    Duration::from_secs(0),
                )
                .await;

            // This will be executed when the result is received.
            info!("Delivery status for message {i} received");
            delivery_status
        })
        .collect::<Vec<_>>();

    // This loop will wait until all delivery statuses have been received.
    for future in futures {
        info!("Future completed. Result: {:?}", future.await);
    }
}
