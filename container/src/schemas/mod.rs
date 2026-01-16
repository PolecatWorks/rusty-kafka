use apache_avro::{schema::RecordSchema, AvroSchema, Schema};
use schema_registry_converter::{
    async_impl::schema_registry::{post_schema, SrSettings},
    schema_registry_common::{SchemaType, SuppliedSchema},
};
use tracing::info;

pub mod billing;
pub mod chaser;

pub async fn get_schema_id<T: AvroSchema>(
    registry: &str,
    topic: &str,
) -> Result<(u32, Schema), String> {
    let testme_schema = T::get_schema();
    info!("Schema is {}", testme_schema.canonical_form());

    if let Schema::Record(RecordSchema { name, .. }) = testme_schema {
        let my_schema = T::get_schema();

        let schema_query = SuppliedSchema {
            name: Some(name.to_string()),
            schema_type: SchemaType::Avro,
            schema: serde_json::to_string(&my_schema).unwrap(),
            references: vec![],
            properties: None,
            tags: None,
        };

        let sr_settings = SrSettings::new(registry.to_owned());

        let result = post_schema(&sr_settings, format!("{topic}-{name}"), schema_query)
            .await
            .expect("Reply from registry");

        info!("Registry replied: {result:?}");

        return Ok((result.id, my_schema));
    }
    Err("Got a schema that was not Record".to_string())
}
