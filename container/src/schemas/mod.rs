use apache_avro::{schema::RecordSchema, AvroSchema, Schema};
use schema_registry_converter::{
    async_impl::schema_registry::{post_schema, SrSettings},
    schema_registry_common::{SchemaType, SuppliedSchema},
};
use tracing::{error, info};

pub mod billing;
pub mod chaser;

pub async fn get_schema_id<T: AvroSchema>(
    registry: &str,
    topic: &str,
) -> Result<(u32, Schema), String> {
    let testme_schema = T::get_schema();
    let canonical_form = testme_schema.canonical_form();
    info!("Schema is {}", canonical_form);

    if let Schema::Record(RecordSchema { name, .. }) = testme_schema {
        let my_schema = T::get_schema();

        let schema_query = SuppliedSchema {
            name: None,
            schema_type: SchemaType::Avro,
            schema: canonical_form,
            references: vec![],
            properties: None,
            tags: None,
        };

        let subject = format!("{topic}-{name}");
        let sr_settings = SrSettings::new(registry.to_owned());

        let result = post_schema(&sr_settings, subject.clone(), schema_query).await;

        match result {
            Ok(r) => {
                info!("Registry replied: {r:?}");
                return Ok((r.id, my_schema));
            }
            Err(e) => {
                let err_msg = format!("Failed to register schema for subject {subject}: {e:?}");
                error!("{}", err_msg);
                return Err(err_msg);
            }
        }
    }
    Err("Got a schema that was not Record".to_string())
}
