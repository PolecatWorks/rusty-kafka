use std::error::Error;

use apache_avro::{Reader, Schema, Writer};

use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::error::Error as KafkaError;

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

    // Create an Avro writer and specify the Avro schema that we want to use to encode the messages.
    let schema = Schema::parse_str(AVRO_SCHEMA)?;
    let mut _writer = Writer::new(&schema, Vec::new());
    // let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Deflate);

    println!("{:?}", schema);

    let topic = "test.topic".to_owned();
    let group = "my-group".to_owned();

    if let Err(e) = consume_messages(group, topic, vec![broker], &schema) {
        println!("Failed consuming messages: {}", e);
    }

    println!("Finished");
    Ok(())
}

fn consume_messages(
    group: String,
    topic: String,
    brokers: Vec<String>,
    schema: &Schema,
) -> Result<(), KafkaError> {
    let mut con = Consumer::from_hosts(brokers)
        .with_topic(topic)
        .with_group(group)
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()?;

    loop {
        let mss = con.poll()?;
        if mss.is_empty() {
            println!("No messages available right now.");
            return Ok(());
        }

        let mut count = 0;

        for ms in mss.iter() {
            for m in ms.messages() {
                let reader = Reader::with_schema(schema, m.value).unwrap();
                for value in reader {
                    println!(
                        "{}:{}@{} {:?}",
                        ms.topic(),
                        ms.partition(),
                        m.offset,
                        value.unwrap(),
                    );
                }

                count += 1;
            }
            let _ = con.consume_messageset(ms);
        }
        con.commit_consumed()?;
        println!("Consumed {} messages", count);
    }
}
