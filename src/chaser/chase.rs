use std::collections::HashMap;
// Read a kafka message. Deserialise it then update and write it.
use std::time::Duration;

use apache_avro::schema::RecordSchema;
use apache_avro::{from_avro_datum, from_value, to_avro_datum, to_value, AvroSchema, Schema};
use chrono::Utc;
use clap::{Args, Parser, Subcommand};
use env_logger::Env;
use log::{error, info, warn};

use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;
use rdkafka::Message;
use schema_registry_converter::async_impl::schema_registry::{post_schema, SrSettings};
use schema_registry_converter::schema_registry_common::BytesResult::Valid;
use schema_registry_converter::schema_registry_common::{
    get_bytes_result, get_payload, SchemaType, SuppliedSchema,
};

use crate::chase_structures::Chaser;

mod chase_structures;
use std::io::Cursor;

mod produce;
use produce::produce;

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
fn expensive_computation(msg: &OwnedMessage) -> Result<(Vec<u8>, Vec<u8>), String> {
    info!("Starting expensive computation on message {}", msg.offset());

    match msg.payload_view::<[u8]>() {
        Some(Ok(payload)) => {
            let bytes_result = get_bytes_result(Some(payload));
            if let Valid(msg_id, payload) = bytes_result {
                let schema = Chaser::get_schema();

                // TODO: get schema from the schemas object not created directly
                // TODO: confirm we have a matching msg_id
                let mut reader = Cursor::new(payload);
                let myval = from_avro_datum(&schema, &mut reader, None).unwrap();

                let mut chaser = from_value::<Chaser>(&myval).unwrap();

                // Update chaser with content for next message
                chaser.ttl -= 1;
                if chaser.ttl == 0 {
                    return Err(format!("TTL reached for {}", chaser.id));
                }
                // chaser.previous = Some(chaser.sent);
                chaser.sent = Utc::now().timestamp_nanos();

                // thread::sleep(Duration::from_millis(rand::random::<u64>() % 5000));
                info!(
                    "Expensive computation completed on message {}",
                    msg.offset()
                );

                // Serialize the chaser object again
                let avro_value = to_value(chaser).unwrap();
                let avro_bytes = to_avro_datum(&schema, avro_value).unwrap();
                let avro_payload = get_payload(msg_id, avro_bytes);

                Ok((msg.key_view::<[u8]>().expect("").expect("found key").to_vec(),avro_payload))
            } else {
                Err("no go".to_owned())
            }
        }
        Some(Err(_)) => Err("Message payload is not a string".to_owned()),
        None => Err("No payload".to_owned()),
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
async fn run_async_processor(
    brokers: String,
    group_id: String,
    schemas: HashMap<u32, Schema>,
    input_topic: String,
    output_topic: String,
) {
    info!("Using schemas: {:?}", schemas);

    // Create the `StreamConsumer`, to receive the messages from the topic in form of a `Stream`.
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", &group_id)
        .set("bootstrap.servers", &brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[&input_topic])
        .expect("Can't subscribe to specified topic");

    // Create the `FutureProducer` to produce asynchronously.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    // Create the outer pipeline on the message stream.
    let stream_processor = consumer.stream().try_for_each(|borrowed_message| {
        let producer = producer.clone();
        let output_topic = output_topic.to_string();
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
                let computation_result =
                    tokio::task::spawn_blocking(move || expensive_computation(&owned_message))
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
                            Ok(delivery) => info!("Sent: {:?}", delivery),
                            Err((e, _)) => error!("Error: {:?}", e),
                        }
                    }
                    Err(e) => warn!("Payload not returned {:?}", e),
                }
            });
            Ok(())
        }
    });

    info!("Starting event loop");
    stream_processor.await.expect("stream processing failed");
    info!("Stream processing terminated");
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Injects message
    Inject {
        #[command(flatten)]
        kafka: KafkaService,

        /// Output topic
        #[arg(long)]
        output_topic: String,

        /// Message count
        #[arg(long, default_value_t = 1)]
        count: u32,

        /// Message ttl
        #[arg(long, default_value_t = 10)]
        ttl: u32,

        /// Message id
        #[arg(long, default_value_t=String::from("unlabelled"))]
        msg_id: String,
    },
    /// Run the processing loop
    Run {
        #[command(flatten)]
        kafka: KafkaService,

        /// Number of workers
        #[arg(long = "num-workers", short, default_value_t = 1)]
        num_workers: i32,

        /// Input topic
        #[arg(long = "input-topic")]
        input_topic: String,

        /// Output topic
        #[arg(long = "output-topic")]
        output_topic: String,

        /// Group id
        #[arg(long)]
        group_id: String,
    },
    /// Trying another leg of clap
    Me(KafkaService),
}

#[derive(Debug, Args)]
struct KafkaService {
    /// Broker list in kafka format
    #[arg(long, default_value_t = String::from("localhost:9092"))]
    brokers: String,

    /// Schema server in host:port format
    #[arg(long, default_value_t = String::from("http://localhost:8081"))]
    registry: String,
}

// cargo run --bin chaser -- --num-workers 1 --input-topic input --output-topic output --group-id gid2

async fn get_schema_id(registry: &str, topic: &str) -> Result<(u32, Schema), String> {
    let testme_schema = Chaser::get_schema();
    info!("Schema is {}", testme_schema.canonical_form());

    if let Schema::Record(RecordSchema { name, .. }) = testme_schema {
        let my_schema = Chaser::get_schema();

        let schema_query = SuppliedSchema {
            name: Some(name.to_string()),
            schema_type: SchemaType::Avro,
            schema: serde_json::to_string(&my_schema).unwrap(),
            references: vec![],
        };

        let sr_settings = SrSettings::new(registry.to_owned());

        let result = post_schema(&sr_settings, format!("{topic}-{name}"), schema_query)
            .await
            .expect("Reply from registry");

        info!("Registry replied: {:?}", result);

        return Ok((result.id, my_schema));
    }
    Err("Got a schema that was not Record".to_string())
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();



    // Start HERE

    #[derive(Debug, AvroSchema, Clone, PartialEq, Eq)]
    enum MyEnum {
        Foo,
        Bar,
        Baz,
    }

    #[derive(Debug, AvroSchema, Clone, PartialEq)]
    struct TestBasicStructWithDefaultValues {
        #[avro(default = "123")]
        a: i32,
        #[avro(default = r#""The default value for 'b'""#)]
        b: String,
        #[avro(default = "true")]
        condition: bool,
        // no default value for 'c'
        c: f64,
        #[avro(default = r#"{"a": 1, "b": 2}"#)]
        map: HashMap<String, i32>,

        #[avro(default = "[1, 2, 3]")]
        array: Vec<i32>,

        #[avro(default = r#""Foo""#)]
        myenum: MyEnum,

        #[avro(default = "null")]
        previous: Option<i64>,


    }
    println!("{:?}", TestBasicStructWithDefaultValues::get_schema());
    println!("Schema is {}", TestBasicStructWithDefaultValues::get_schema().canonical_form());
    println!("Schema is {}", TestBasicStructWithDefaultValues::get_schema().canonical_form());

    // END here


    let log_level = Env::default().default_filter_or("info");
    env_logger::Builder::from_env(log_level).init();

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    match args.command {
        Commands::Inject {
            kafka,
            output_topic,
            count,
            ttl,
            msg_id,
        } => {
            info!("Inject with {:?} to {}", kafka, output_topic);

            let (schema_id, schema) = get_schema_id(&kafka.registry, &output_topic)
                .await
                .expect("valid schema");

            produce(
                &kafka.brokers,
                &output_topic,
                &schema,
                schema_id,
                count,
                ttl,
                &msg_id,
            )
            .await
        }
        Commands::Run {
            kafka,
            num_workers,
            input_topic,
            output_topic,
            group_id,
        } => {
            let (schema_id, schema) = get_schema_id(&kafka.registry, &output_topic).await.unwrap();

            let mut schemas = HashMap::new();

            schemas.insert(schema_id, schema);

            (0..num_workers)
                .map(|_| {
                    tokio::spawn(run_async_processor(
                        kafka.brokers.to_owned(),
                        group_id.to_owned(),
                        schemas.clone(),
                        input_topic.to_owned(),
                        output_topic.to_owned(),
                    ))
                })
                .collect::<FuturesUnordered<_>>()
                .for_each(|_| async {})
                .await
        }
        Commands::Me(service) => {
            error!("Me called with {:?}", service);
            todo!()
        }
    }
}
