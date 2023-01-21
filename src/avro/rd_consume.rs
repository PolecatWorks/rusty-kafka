use std::io::Cursor;

use apache_avro::{from_avro_datum, from_value, AvroSchema, Schema};
use clap::Parser;
use env_logger::Env;
use log::{error, info, warn};

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::util::get_rdkafka_version;
use schema_registry_converter::async_impl::schema_registry::{post_schema, SrSettings};
use schema_registry_converter::schema_registry_common::BytesResult::Valid;

mod structures;
use schema_registry_converter::schema_registry_common::{
    get_bytes_result, SchemaType, SuppliedSchema,
};
use structures::TestMe;

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

async fn consume_and_print(
    brokers: &str,
    group_id: &str,
    topics: &[&str],
    schema: &Schema,
    id: u32,
) {
    let context = CustomContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        //.set("statistics.interval.ms", "30000")
        .set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&topics.to_vec())
        .expect("Can't subscribe to specified topics");

    loop {
        match consumer.recv().await {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(m) => {
                let payload_len = m.payload().unwrap().len();

                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };

                info!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                if let Some(headers) = m.headers() {
                    for header in headers.iter() {
                        info!("  Header {:#?}: {:?}", header.key, header.value);
                    }
                }

                let bytes_result = get_bytes_result(Some(m.payload().unwrap()));
                if let Valid(msg_id, payload) = bytes_result {
                    if msg_id == id {
                        let mut reader = Cursor::new(payload);
                        let myval = from_avro_datum(schema, &mut reader, None).unwrap();

                        let testme_out = from_value::<TestMe>(&myval).unwrap();
                        println!(
                            "I got the content : {:?} of length {}",
                            testme_out, payload_len
                        );
                    } else {
                        error!("Looking for id {} but found {}", id, msg_id);
                    }
                } else {
                    error!("Message was not valid");
                }

                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Optional name to operate on

    /// Broker list in kafka format
    #[arg(long, default_value_t = String::from("localhost:9092"))]
    brokers: String,

    /// Schema server in host:port format
    #[arg(long, default_value_t = String::from("http://localhost:8081"))]
    registry: String,

    /// Destination topic
    #[arg(long = "topic", short)]
    topics: Vec<String>,

    /// Group id
    #[arg(long)]
    group: String,
}
// cargo run --bin rd_consume -- --brokers localhost:9092 --topic test.topic --log-conf rdkafka=trace --group gid1

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let log_level = Env::default().default_filter_or("info");
    env_logger::Builder::from_env(log_level).init();

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topic_list: Vec<&str> = args.topics.iter().map(AsRef::as_ref).collect();

    let testme_schema = TestMe::get_schema();
    println!("Schema is {}", testme_schema.canonical_form());

    if let Schema::Record { ref name, .. } = testme_schema {
        println!("{}", name);

        let schema_query = SuppliedSchema {
            name: Some(name.to_string()).to_owned(),
            schema_type: SchemaType::Avro,
            schema: TestMe::get_schema().canonical_form(),
            references: vec![],
        };

        let sr_settings = SrSettings::new(args.registry);

        let result = post_schema(
            &sr_settings,
            format!("{}-value", args.topics[0]),
            schema_query,
        )
        .await
        .expect("Reply from registry");

        println!("Registry replied: {:?}", result);

        let schema_id = result.id;

        consume_and_print(
            &args.brokers,
            &args.group,
            &topic_list,
            &testme_schema,
            schema_id,
        )
        .await
        // produce(&args.brokers, &args.topic, &testme_schema, schema_id).await
    } else {
        error!("Schema was not a record. Can only deal with records");
    }
}
