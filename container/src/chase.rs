use std::sync::Arc;
// Read a kafka message. Deserialise it then update and write it.
use std::time::Duration;

use apache_avro::{from_avro_datum, from_value, to_avro_datum, to_value};
use chrono::Utc;
use log::{error, info, warn};

use futures::TryStreamExt;

use opentelemetry::metrics::MeterProvider;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use schema_registry_converter::schema_registry_common::BytesResult::Valid;
use schema_registry_converter::schema_registry_common::{get_bytes_result, get_payload};

use crate::error::MyError;
use crate::schemas::chaser::Chaser;
use crate::MyState;

use std::io::Cursor;

async fn record_borrowed_message_receipt(msg: &BorrowedMessage<'_>) {
    // Simulate some work that must be done in the same order as messages are
    // received; i.e., before truly parallel processing can begin.
    info!("Message received: {}", msg.offset());
}

async fn record_owned_message_receipt(_msg: &OwnedMessage) {
    // Like `record_borrowed_message_receipt`, but takes an `OwnedMessage`
    // instead, as in a real-world use case  an `OwnedMessage` might be more
    // convenient than a `BorrowedMessage`.
}

// Emulates an expensive, synchronous computation.
fn expensive_computation(
    state: &MyState,
    msg: &OwnedMessage,
    received_counter: &opentelemetry::metrics::Counter<u64>,
    ttl_histogram: &opentelemetry::metrics::Histogram<u64>,
) -> Result<(Vec<u8>, Vec<u8>), MyError> {
    info!("Starting expensive computation on message {}", msg.offset());

    let payload = msg.payload_view::<[u8]>().and_then(|res| res.ok());

    let bytes_result = get_bytes_result(payload);
    if let Valid(msg_id, payload) = bytes_result {
        let mut reader = Cursor::new(payload);
        let schema = state
            .schemas
            .get(&msg_id)
            .ok_or(MyError::Message("Schema not found"))?;
        let myval = from_avro_datum(schema, &mut reader, None)?;

        let mut chaser = from_value::<Chaser>(&myval)?;

        let key_str = msg
            .key_view::<str>()
            .and_then(|k| k.ok())
            .unwrap_or("unknown");

        let attributes = [
            opentelemetry::KeyValue::new("chaser_name", chaser.name.clone()),
            opentelemetry::KeyValue::new("chaser_key", key_str.to_string()),
        ];

        received_counter.add(1, &attributes);
        ttl_histogram.record(chaser.ttl as u64, &attributes);

        // Update chaser with content for next message
        chaser.ttl -= 1;
        if chaser.ttl == 0 {
            return Err(MyError::DynMessage(format!(
                "TTL reached for {}",
                chaser.id
            )));
        }
        // chaser.previous = Some(chaser.sent);
        chaser.sent = Utc::now()
            .timestamp_nanos_opt()
            .ok_or(MyError::Message("Could not get Nanoseconds"))?;

        // thread::sleep(Duration::from_millis(rand::random::<u64>() % 5000));
        info!(
            "Expensive computation completed on message {}",
            msg.offset()
        );

        // Serialize the chaser object again
        let avro_value = to_value(chaser)?;
        let avro_bytes = to_avro_datum(schema, avro_value)?;
        let avro_payload = get_payload(msg_id, avro_bytes);

        Ok((
            msg.key_view::<[u8]>()
                .ok_or(MyError::Message("Could not get a key"))??
                .to_vec(),
            avro_payload,
        ))
    } else {
        Err(MyError::Message("no go"))
    }
}

// Creates all the resources and runs the event loop. The event loop will:
//   1) receive a stream of messages from the `StreamConsumer`.
//   2) filter out eventual Kafka errors.
//   3) send the message to a thread pool for processing.
//   4) produce the result to the output topic.
// `tokio::spawn` is used to handle IO-bound tasks in parallel (e.g., producing
// the messages), while `tokio::task::spawn_blocking` is used to handle the
// simulated CPU-bound task.
pub async fn run_async_processor(
    state: std::sync::Arc<MyState>,
    // brokers: String,
    // group_id: String,
    // schemas: HashMap<u32, Schema>,
    // input_topic: String,
    // output_topic: String,
) {
    info!("Using schemas: {:?}", &state.schemas);

    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &state.config.kafka.group_id)
        .set("bootstrap.servers", &state.config.kafka.brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[&state.config.kafka.input_topic])
        .expect("Can't subscribe to specified topic");

    // Create the `FutureProducer` to produce asynchronously.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &state.config.kafka.brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    log::info!("Starting Prosumer");

    let meter = state.telemetry.meter_provider.meter("kafka-chase");
    let counter = meter.u64_counter("messages_processed").build();
    let chaser_received = meter.u64_counter("chaser.received.total").build();
    let chaser_ttl = meter.u64_histogram("chaser.ttl").build();

    let _msg_size = meter.u64_observable_gauge("message_size").build();

    // Create the outer pipeline on the message stream.
    let stream_processor = consumer.stream().try_for_each(|borrowed_message| {
        let producer = producer.clone();
        let state = Arc::clone(&state);
        let chaser_received = chaser_received.clone();
        let chaser_ttl = chaser_ttl.clone();
        let output_topic = state.config.kafka.output_topic.to_string();
        counter.add(1, &[]);

        async move {
            // Process each message
            record_borrowed_message_receipt(&borrowed_message).await;
            // Borrowed messages can't outlive the consumer they are received from, so they need to
            // be owned in order to be sent to a separate thread.
            let owned_message = borrowed_message.detach();
            record_owned_message_receipt(&owned_message).await;
            tokio::spawn(async move {
                // The body of this block will be executed on the main thread pool,
                // but we perform `expensive_computation` on a separate thread pool
                // for CPU-intensive tasks via `tokio::task::spawn_blocking`.
                // let my_key = owned_message.key().expect("keyed").clone();
                let state_for_comp = Arc::clone(&state);
                let counter_for_comp = chaser_received.clone();
                let ttl_for_comp = chaser_ttl.clone();
                let computation_result = tokio::task::spawn_blocking(move || {
                    expensive_computation(
                        &state_for_comp,
                        &owned_message,
                        &counter_for_comp,
                        &ttl_for_comp,
                    )
                })
                .await
                .expect("failed to wait for expensive computation");

                match computation_result {
                    Ok((msg_key, msg_payload)) => {
                        let produce_future = producer.send(
                            FutureRecord::to(&output_topic)
                                .key(&msg_key)
                                .payload(&msg_payload),
                            Duration::from_secs(0),
                        );

                        match produce_future.await {
                            Ok(delivery) => info!("Sent: {delivery:?}"),
                            Err((e, _)) => error!("Error: {e:?}"),
                        }
                    }
                    Err(e) => warn!("Payload not returned {e:?}"),
                }
            });
            Ok(())
        }
    });

    info!("Starting event loop");
    stream_processor.await.expect("stream processing failed");
    info!("Stream processing terminated");
}

// cargo run --bin chaser -- --num-workers 1 --input-topic input --output-topic output --group-id gid2
