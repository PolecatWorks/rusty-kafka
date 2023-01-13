use std::error::Error;

use avro_rs::types::Record as AvroRecord;
use avro_rs::{from_value, Codec, Reader, Writer, to_avro_datum};

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


    // Create an Avro writer and specify the Avro schema that we want to use to encode the messages.
    let schema = avro_rs::Schema::parse_str(AVRO_SCHEMA)?;
    let mut writer = Writer::new(&schema, Vec::new());
    // let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Deflate);

    println!("{:?}", schema);

    let mut record = AvroRecord::new(writer.schema()).unwrap();
    record.put("name", "Ben");
    record.put("favourite_number", 3);

    writer.append(record).unwrap();

    let mut record = AvroRecord::new(writer.schema()).unwrap();
    record.put("name", "Ben2");
    record.put("favourite_number", 4);

    writer.append(record).unwrap();

    let encoded = writer.into_inner().unwrap();

    println!("Encoded = {}", encoded.len());


    let reader = Reader::with_schema(&schema, &encoded[..]).unwrap();

    for value in reader {
        println!("{:?}", value.unwrap());
    }

    println!("Finished with using the full avro record (inc schema, etc)");
    let mut record = AvroRecord::new(&schema).unwrap();
    record.put("name", "Ben2 is great");
    record.put("favourite_number", 4);

    let justval = to_avro_datum(&schema, record).unwrap();
    println!("justval length is {}", justval.len());



    Ok(())
}
