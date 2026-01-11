use apache_avro::AvroSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone)]
#[avro(namespace = "com.polecatworks.billing")]
#[allow(non_snake_case)]
pub(crate) struct Bill {
    pub(crate) billId: String,
    pub(crate) customerId: String,
    pub(crate) orderId: String,
    pub(crate) amountCents: i64,
    pub(crate) currency: String,
    pub(crate) issuedAt: i64,
    pub(crate) dueDate: i64,
}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone)]
#[avro(namespace = "com.polecatworks.billing")]
#[allow(non_snake_case)]
pub(crate) struct PaymentRequest {
    pub(crate) paymentId: String,
    pub(crate) billId: String,
    pub(crate) customerId: String,
    pub(crate) amountCents: i64,
    pub(crate) currency: String,
    pub(crate) requestedAt: i64,
}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone)]
#[avro(namespace = "com.polecatworks.billing")]
#[allow(non_snake_case)]
pub(crate) struct PaymentFailed {
    pub(crate) paymentId: String,
    pub(crate) billId: String,
    pub(crate) customerId: String,
    pub(crate) failureReason: String,
    pub(crate) failedAt: i64,
}
