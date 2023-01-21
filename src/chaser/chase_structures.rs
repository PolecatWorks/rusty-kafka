use apache_avro::AvroSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, AvroSchema)]
#[avro(namespace = "com.polecatworks.chaser")]
pub(crate) struct Chaser {
    pub(crate) name: String,
    pub(crate) uuid: Option<String>,
    pub(crate) sent: i64,
    pub(crate) previous: i64,
}
