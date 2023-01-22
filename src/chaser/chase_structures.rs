use apache_avro::AvroSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, AvroSchema)]
#[avro(namespace = "com.polecatworks.chaser")]
pub(crate) struct Chaser {
    pub(crate) name: String,
    pub(crate) id: String,
    pub(crate) sent: i64,
    pub(crate) ttl: u32,
    pub(crate) previous: Option<i64>,
}
