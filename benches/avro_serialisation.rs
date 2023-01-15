use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use avro_rs::types::Record as AvroRecord;
use avro_rs::{from_value, Codec, Reader, Writer, to_value};
use avro_rs::{to_avro_datum, Schema};
use schema_registry_converter::schema_registry_common::get_payload;
use serde::{Deserialize, Serialize};
use std::iter;

fn encode(schema: &Schema, name: &str) -> Vec<u8> {
    let mut writer = Writer::new(&schema, Vec::new());

    let mut record = AvroRecord::new(writer.schema()).unwrap();
    record.put("name", name);
    record.put("favourite_number", 3);

    writer.append(record).unwrap();

    let encoded = writer.into_inner().unwrap();
    return encoded;
}

fn encode_datum(schema: &Schema, name: &str) -> Vec<u8> {
    let mut record = AvroRecord::new(schema).unwrap();
    record.put("name", name);
    record.put("favourite_number", 3);

    let encoded = to_avro_datum(schema, record).unwrap();
    return encoded;
}
fn encode_kafka(schema: &Schema, name: &str) -> Vec<u8> {
    let mut record = AvroRecord::new(schema).unwrap();
    record.put("name", name);
    record.put("favourite_number", 3);

    let encoded = to_avro_datum(schema, record).unwrap();

    get_payload(1, encoded)
}

#[derive(Debug, Serialize, Deserialize)]
struct User {
    name: String,
    favourite_number: i32,
}

fn serdes_encode_kafka(schema: &Schema, name: &str) -> Vec<u8> {

    let user = User {
            name: name.to_owned(),
            favourite_number: 3,
    };

    let avro_value = to_value(user).unwrap();

    let encoded = to_avro_datum(&schema, avro_value).unwrap();

    get_payload(1, encoded)
}

const AVRO_SCHEMA: &str = r#"{
    "type": "record",
    "name": "User",
    "fields": [
      {"name": "name", "type": "string"},
      {"name": "favourite_number",  "type": "int"}
    ]
  }"#;

fn encode_benchmark(c: &mut Criterion) {
    let schema = avro_rs::Schema::parse_str(AVRO_SCHEMA).unwrap();

    let name = "Ben";

    let mut avro_group = c.benchmark_group("Avro Encode");

    avro_group.bench_function("writer", |b| {
        b.iter(|| encode(black_box(&schema), black_box(name)))
    });

    avro_group.bench_function("encode_datum", |b| {
        b.iter(|| encode_datum(black_box(&schema), black_box(name)))
    });

    avro_group.bench_function("kafka", |b| {
        b.iter(|| encode_kafka(black_box(&schema), black_box(name)))
    });

    avro_group.bench_function("serdes kafka", |b| {
        b.iter(|| serdes_encode_kafka(black_box(&schema), black_box(name)))
    });

    avro_group.finish();

    c.bench_with_input(BenchmarkId::new("avro encodes", name), &name, |b, &name| {
        b.iter(|| encode(&schema, name));
    });

    let mut group = c.benchmark_group("from_elem");
    for size in [
        1, 2, // 3,4,5,6,7,8,9,
        10, 20, // 30 ,40,50,60,70,80,90,
        100, 200,
        // 300
    ]
    .iter()
    {
        let name = String::from_utf8(vec![b'X'; 10]).unwrap();

        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &name, |b, name| {
            b.iter(|| encode(&schema, name));
        });
    }
    group.finish();
}

criterion_group!(benches, encode_benchmark);
criterion_main!(benches);
