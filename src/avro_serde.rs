use std::error::Error;

use apache_avro::types::Record as AvroRecord;
use apache_avro::{from_value, to_avro_datum, to_value, AvroSchema, Reader, Schema, Writer};
use schema_registry_converter::schema_registry_common::{get_bytes_result, get_payload};
use serde::{Deserialize, Serialize};

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
    let schema = Schema::parse_str(AVRO_SCHEMA)?;
    // let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Deflate);

    println!("{:?}", schema);

    {
        let mut writer = Writer::new(&schema, Vec::new());

        let mut record = AvroRecord::new(writer.schema()).unwrap();
        record.put("name", "Ben2 is great");
        record.put("favourite_number", 3);

        writer.append(record).unwrap();

        let mut record = AvroRecord::new(writer.schema()).unwrap();
        record.put("name", "Ben2 is great");
        record.put("favourite_number", 4);

        writer.append(record).unwrap();

        let encoded = writer.into_inner().unwrap();

        println!("Encoded = {}", encoded.len());

        let reader = Reader::with_schema(&schema, &encoded[..]).unwrap();

        for value in reader {
            println!("{:?}", value.unwrap());
        }
    }

    println!("Finished with using the full avro record (inc schema, etc)");

    {
        let mut record = AvroRecord::new(&schema).unwrap();
        record.put("name", "Ben2 is great");
        record.put("favourite_number", 4);

        let justval = to_avro_datum(&schema, record).unwrap();
        println!("justval length is {}", justval.len());

        let kafkajustval = get_payload(1, justval);

        println!("kafkajustval length is {}", kafkajustval.len());

        let bytes_result = get_bytes_result(Some(&kafkajustval));
        println!("result = {:?}", bytes_result);
    }

    println!("using Serde");
    {
        #[derive(Debug, Serialize, Deserialize)]
        struct User {
            name: String,
            favourite_number: i32,
        }

        let mut writer = Writer::new(&schema, Vec::new());

        let user = User {
            name: "Ben2 is great".to_owned(),
            favourite_number: 4,
        };

        writer.append_ser(user).unwrap();

        let encoded = writer.into_inner().unwrap();

        println!("Encodded via serde length = {}", encoded.len());

        let reader = Reader::with_schema(&schema, &encoded[..]).unwrap();

        for value in reader {
            println!("{:?}", value.unwrap());
        }

        let reader = Reader::new(&encoded[..]).unwrap();

        for value in reader {
            println!("{:?}", from_value::<User>(&value.unwrap()));
        }
    }
    println!("Using serdes model");
    {
        #[derive(Debug, Serialize, Deserialize)]
        struct User {
            name: String,
            favourite_number: i32,
        }

        let user = User {
            name: "Ben2 is great".to_owned(),
            favourite_number: 4,
        };

        // let mut record = AvroRecord::new(&schema).unwrap();
        // record.put("name", "Ben2 is great");
        // record.put("favourite_number", 4);

        // let avro_value = user.serialize(serializer)
        let avro_value = to_value(user).unwrap();

        let justval = to_avro_datum(&schema, avro_value).unwrap();
        println!("justval length is {}", justval.len());

        let kafkajustval = get_payload(1, justval);

        println!("kafkajustval length is {}", kafkajustval.len());

        let bytes_result = get_bytes_result(Some(&kafkajustval));
        println!("result = {:?}", bytes_result);
    }

    {
        #[derive(Debug, Serialize, Deserialize, AvroSchema)]
        struct User {
            name: String,
            favourite_number: i32,
        }

        let schema = User::get_schema();

        println!("Derrived Schema is {:?}", schema);
    }

    Ok(())
}
