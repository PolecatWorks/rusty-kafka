use apache_avro::AvroSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone)]
#[avro(namespace = "com.polecatworks.billing")]
#[allow(non_snake_case)]
pub struct Bill {
    pub billId: String,
    pub customerId: String,
    pub orderId: String,
    pub amountCents: i64,
    pub currency: String,
    pub issuedAt: i64,
    pub dueDate: i64,
}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone)]
#[avro(namespace = "com.polecatworks.billing")]
#[allow(non_snake_case)]
pub struct PaymentRequest {
    pub paymentId: String,
    pub billId: String,
    pub customerId: String,
    pub amountCents: i64,
    pub currency: String,
    pub requestedAt: i64,
}

#[derive(Debug, Serialize, Deserialize, AvroSchema, Clone)]
#[avro(namespace = "com.polecatworks.billing")]
#[allow(non_snake_case)]
pub struct PaymentFailed {
    pub paymentId: String,
    pub billId: String,
    pub customerId: String,
    pub failureReason: String,
    pub failedAt: i64,
}
