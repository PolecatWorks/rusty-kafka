use clap::{Parser};
use schema_registry_converter::{async_impl::schema_registry::{post_schema, SrSettings}, schema_registry_common::{SuppliedSchema, SchemaType}};


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

    let schema = SuppliedSchema {
        name: Some(String::from("nl.openweb.data.Heartbeat")),
        schema_type: SchemaType::Avro,
        schema: String::from(r#"{"type":"record","name":"Heartbeat","namespace":"nl.openweb.data","fields":[{"name":"beat","type":"long"}]}"#),
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

    let result = post_schema(&sr_settings, "test2-value".to_string(), schema).await.expect("Reply from registry");

    println!("Registry replied: {:?}", result);

}
