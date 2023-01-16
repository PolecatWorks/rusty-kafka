use std::collections::HashMap;
use std::time::Duration;

use apache_avro::{AvroSchema, Schema, to_value, to_avro_datum};
use clap::Parser;
use log::{info, error};
use env_logger::Env;

use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;
use schema_registry_converter::async_impl::schema_registry::{SrSettings, post_schema};
use schema_registry_converter::schema_registry_common::{SuppliedSchema, SchemaType, get_payload};
use serde::{Deserialize, Serialize};


mod structures;
use structures::TestMe;


async fn produce(brokers: &str, topic_name: &str, schema: &Schema, id: u32) {
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    // This loop is non blocking: all messages will be sent one after the other, without waiting
    // for the results.
    let futures = (0..5)
        .map(|i| async move {
            // The send operation on the topic returns a future, which will be
            // completed once the result or failure from Kafka is received.

            let user = TestMe {
                name: format!("Ben-{}", i),
                uuid: None,
                ancestors: vec!(),
                meta: HashMap::new(),
            };

            let avro_value = to_value(user).unwrap();
            let avro_bytes = to_avro_datum(&schema, avro_value).unwrap();
            let avro_payload = get_payload(id, avro_bytes);


            let delivery_status = producer
                .send(
                    FutureRecord::to(topic_name)
                        .payload(&avro_payload)
                        .key(&format!("Key {}", i))
                        .headers(OwnedHeaders::new().insert(Header {
                            key: "header_key",
                            value: Some("header_value"),
                        })),
                    Duration::from_secs(0),
                )
                .await;

            // This will be executed when the result is received.
            info!("Delivery status for message {} received", i);
            delivery_status
        })
        .collect::<Vec<_>>();

    // This loop will wait until all delivery statuses have been received.
    for future in futures {
        info!("Future completed. Result: {:?}", future.await);
    }
}





#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {

    /// Broker list in kafka format for host:port
    #[arg(long, default_value_t = String::from("localhost:9092"))]
    brokers: String,

    /// Schema server in host:port format
    #[arg(long, default_value_t = String::from("http://localhost:8081"))]
    registry: String,

    /// Destination topic
    #[arg(long)]
    topic: String,
}
// cargo run --bin rd_produce -- --topic test.topic



#[tokio::main]
async fn main() {
    let args = Args::parse();

    let log_level = Env::default().default_filter_or("info");
    env_logger::Builder::from_env(log_level).init();

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

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

        let result = post_schema(&sr_settings, format!("{}-value",args.topic), schema_query)
            .await
            .expect("Reply from registry");

        println!("Registry replied: {:?}", result);

        let schema_id = result.id;


        produce(&args.brokers, &args.topic, &testme_schema, schema_id).await
    } else {
        error!("Schema was not a record. Can only deal with records");
    }



}
