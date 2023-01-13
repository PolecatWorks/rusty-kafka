use std::error::Error;
use std::io::{self, BufRead};
use std::time::Duration;

use avro_rs::types::Record as AvroRecord;
use avro_rs::{from_value, Codec, Reader, Writer};
use kafka::client::KafkaClient;
use kafka::consumer::Consumer;
use kafka::producer::{Producer, Record, RequiredAcks};
// use kafka::utils::Shutdown;

const AVRO_SCHEMA: &str = r#"{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "favourite_number",  "type": "int"}
  ]
}"#;

fn main() -> Result<(), Box<dyn Error>> {
    let broker = "localhost:9092".to_owned();

    // Create a Kafka producer and specify the topic that we want to write messages to.
    let mut producer = Producer::from_hosts(vec![broker])
        // .with_ack_timeout(1000)
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()?;

    // Create an Avro writer and specify the Avro schema that we want to use to encode the messages.
    let schema = avro_rs::Schema::parse_str(AVRO_SCHEMA)?;
    let mut writer = Writer::new(&schema, Vec::new());
    // let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Deflate);

    println!("{:?}", schema);

    let mut record = AvroRecord::new(writer.schema()).unwrap();
    record.put("name", "Ben");
    record.put("favourite_number", 3);

    writer.append(record).unwrap();

    let encoded = writer.into_inner().unwrap();

    println!("Encoded = {}", encoded.len());


    // ToDo: Update this to use Confluent compatible Avro : https://docs.confluent.io/3.2.0/schema-registry/docs/serializer-formatter.html#wire-format

    producer.send(&Record {
        topic: "test.topic",
        partition: -1,
        key: (),
        value: encoded,
    })?;


    println!("Finished");
    Ok(())
}
