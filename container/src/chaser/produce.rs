use apache_avro::{to_avro_datum, to_value, Schema};
use chrono::{DateTime, Utc};
use log::info;
use rdkafka::{
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use schema_registry_converter::schema_registry_common::get_payload;

use crate::schemas::billing::{Bill, PaymentFailed, PaymentRequest};
use crate::schemas::chaser::Chaser;
use std::time::Duration;
use uuid::Uuid;

pub(crate) async fn produce(
    brokers: &str,
    topic_name: &str,
    schema: &Schema,
    id: u32,
    count: u32,
    ttl: u32,
    msg_id: &str,
) {
    let producer: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let futures = (0..count)
        .map(|i| async move {
            let utc_now: DateTime<Utc> = Utc::now();

            let user = Chaser {
                name: msg_id.to_owned(),
                id: format!("{msg_id}-{i}"),
                ttl,
                sent: utc_now.timestamp_nanos_opt().unwrap(),
                previous: None,
            };

            let avro_value = to_value(user).unwrap();
            let avro_bytes = to_avro_datum(schema, avro_value).unwrap();
            let avro_payload = get_payload(id, avro_bytes);

            let delivery_status = producer
                .send(
                    FutureRecord::to(topic_name)
                        .payload(&avro_payload)
                        .key(&format!("Key-{i}"))
                        .headers(OwnedHeaders::new().insert(Header {
                            key: "header_key",
                            value: Some("header_value"),
                        })),
                    Duration::from_secs(0),
                )
                .await;

            info!("Delivery status for message {i} received");
            delivery_status
        })
        .collect::<Vec<_>>();

    for future in futures {
        info!("Future completed. Result: {:?}", future.await);
    }
}

pub(crate) async fn produce_bill(
    brokers: &str,
    topic_name: &str,
    schema: &Schema,
    id: u32,
    customer_prefix: &str,
    num_customers: u32,
    bills_per_customer: u32,
    start_customer_index: u32,
    start_bill_index: u32,
    amount_cents: i64,
) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let mut futures = Vec::new();

    for c_idx in start_customer_index..(start_customer_index + num_customers) {
        let customer_id = format!("{customer_prefix}_{c_idx:05}");
        for b_idx in start_bill_index..(start_bill_index + bills_per_customer) {
            let producer = producer.clone();
            let customer_id = customer_id.clone();
            let topic_name = topic_name.to_string();

            futures.push(async move {
                let utc_now = Utc::now().timestamp_millis();
                let bill_id = format!("{customer_id}_{b_idx:05}");

                let bill = Bill {
                    billId: bill_id.clone(),
                    customerId: customer_id,
                    orderId: format!("order-{b_idx}"),
                    amountCents: amount_cents,
                    currency: "USD".to_string(),
                    issuedAt: utc_now,
                    dueDate: utc_now + 30 * 24 * 60 * 60 * 1000,
                };

                let avro_value = to_value(bill).unwrap();
                let avro_bytes = to_avro_datum(schema, avro_value).unwrap();
                let avro_payload = get_payload(id, avro_bytes);

                producer
                    .send(
                        FutureRecord::to(&topic_name)
                            .payload(&avro_payload)
                            .key(&bill_id),
                        Duration::from_secs(0),
                    )
                    .await
            });
        }
    }

    for future in futures {
        info!("Bill sent. Result: {:?}", future.await);
    }
}

pub(crate) async fn produce_payment_request(
    brokers: &str,
    topic_name: &str,
    schema: &Schema,
    id: u32,
    count: u32,
    bill_id: &str,
    customer_id: &str,
    amount_cents: i64,
) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let futures = (0..count)
        .map(|_| {
            let producer = producer.clone();
            async move {
                let utc_now = Utc::now().timestamp_millis();
                let payment_id = Uuid::new_v4().to_string();

                let pr = PaymentRequest {
                    paymentId: payment_id.clone(),
                    billId: bill_id.to_owned(),
                    customerId: customer_id.to_owned(),
                    amountCents: amount_cents,
                    currency: "USD".to_string(),
                    requestedAt: utc_now,
                };

                let avro_value = to_value(pr).unwrap();
                let avro_bytes = to_avro_datum(schema, avro_value).unwrap();
                let avro_payload = get_payload(id, avro_bytes);

                producer
                    .send(
                        FutureRecord::to(topic_name)
                            .payload(&avro_payload)
                            .key(bill_id),
                        Duration::from_secs(0),
                    )
                    .await
            }
        })
        .collect::<Vec<_>>();

    for future in futures {
        info!("PaymentRequest sent. Result: {:?}", future.await);
    }
}

pub(crate) async fn produce_payment_failed(
    brokers: &str,
    topic_name: &str,
    schema: &Schema,
    id: u32,
    count: u32,
    payment_id: &str,
    bill_id: &str,
    customer_id: &str,
    reason: &str,
) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    let futures = (0..count)
        .map(|_| {
            let producer = producer.clone();
            async move {
                let utc_now = Utc::now().timestamp_millis();

                let pf = PaymentFailed {
                    paymentId: payment_id.to_owned(),
                    billId: bill_id.to_owned(),
                    customerId: customer_id.to_owned(),
                    failureReason: reason.to_owned(),
                    failedAt: utc_now,
                };

                let avro_value = to_value(pf).unwrap();
                let avro_bytes = to_avro_datum(schema, avro_value).unwrap();
                let avro_payload = get_payload(id, avro_bytes);

                producer
                    .send(
                        FutureRecord::to(topic_name)
                            .payload(&avro_payload)
                            .key(bill_id),
                        Duration::from_secs(0),
                    )
                    .await
            }
        })
        .collect::<Vec<_>>();

    for future in futures {
        info!("PaymentFailed sent. Result: {:?}", future.await);
    }
}
