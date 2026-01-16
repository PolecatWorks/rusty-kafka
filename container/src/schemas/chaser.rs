use apache_avro::AvroSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, AvroSchema)]
#[avro(namespace = "com.polecatworks.chaser")]
pub struct Chaser {
    pub name: String,
    pub id: String,
    pub sent: i64,
    pub ttl: u32,
    #[avro(default = "null")]
    pub previous: Option<i64>,
}
