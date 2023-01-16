use apache_avro::AvroSchema;
use clap::Parser;
use schema_registry_converter::{
    async_impl::schema_registry::{post_schema, SrSettings},
    schema_registry_common::{SchemaType, SuppliedSchema},
};
use serde::{Deserialize, Serialize};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Optional name to operate on

    /// Broker list in kafka format
    #[arg(long)]
    registry: String,
}
// cargo run --bin schema_registry -- --registry http://localhost:8081

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // https://docs.rs/schema_registry_converter/latest/schema_registry_converter/index.html

    #[derive(Debug, Serialize, Deserialize, AvroSchema)]
    struct User {
        name: String,
        favourite_number: i32,
    }

    let schema_generated = User::get_schema();

    let mystringschema = schema_generated.canonical_form();
    println!("Canonical form {}", mystringschema);


    let schema = SuppliedSchema {
        name: Some(String::from("nl.openweb.data.User")),
        schema_type: SchemaType::Avro,
        // schema: manualschema,
        schema: mystringschema,
        // schema: otherstringschema,

        // schema: String::from(
        //     r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#,
        // ),

        // schema: String::from(r#"{
        //     "type": "record",
        //     "name": "User",
        //     "fields": [
        //       {"name": "name", "type": "string"},
        //       {"name": "favourite_number",  "type": "int"}
        //     ]
        //   }"#),
        references: vec![],
    };

    let sr_settings = SrSettings::new(args.registry);

    let result = post_schema(&sr_settings, "test3-value".to_string(), schema)
        .await
        .expect("Reply from registry");

    println!("Registry replied: {:?}", result);

    // curl -X GET http://localhost:8081/subjects
    // curl -X GET http://localhost:8081/subjects/test3-value/versions

    // View soft deleted
    // curl -X GET http://localhost:8081/subjects\?deleted=true

    // Delete soft first then permanent
    // curl -X DELETE http://localhost:8081/subjects/test0-value
    // curl -X DELETE http://localhost:8081/subjects/test0-value\?permanent=true


}
