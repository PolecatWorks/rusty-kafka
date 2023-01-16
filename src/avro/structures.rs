use std::collections::HashMap;

use apache_avro::AvroSchema;
use serde::{Serialize, Deserialize};




#[derive(Debug, Serialize, Deserialize, AvroSchema)]
#[avro(namespace = "com.polecatworks.avrotest")]
pub(crate) struct TestMe {
    pub(crate) name: String,
    pub(crate) uuid: Option<String>,
    pub(crate) ancestors: Vec<String>,
    pub(crate) meta: HashMap<String, String>,
}
