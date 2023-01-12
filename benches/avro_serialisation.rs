

use criterion::{
    black_box,
    criterion_group,
    criterion_main,
    BenchmarkId,
    Criterion,
    Throughput,
};

use avro_rs::Schema;
use avro_rs::types::Record as AvroRecord;
use avro_rs::{from_value, Codec, Reader, Writer};
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

    c.bench_function("avro encode", |b| b.iter(||
        encode(
            black_box(&schema),
            black_box(name)
    )));



    c.bench_with_input(BenchmarkId::new("avro encodes", name), &name, |b, &name| {
        b.iter(|| encode(&schema, name));
    });




    let mut group = c.benchmark_group("from_elem");
    for size in [1,2,3,4,5,6,7,8,9,10, 20 , 30 ,40,50,60,70,80,90,100,200,300].iter() {

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
