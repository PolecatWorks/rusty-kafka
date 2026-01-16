pub mod chase;
pub mod config;
pub mod error;
pub mod metrics;
pub mod produce;
pub mod schemas;
pub mod tokio_tools;
use futures::StreamExt;
use std::collections::HashMap;

use futures::stream::FuturesUnordered;
use hamsrs::Hams;
use prometheus::Registry;
use tokio_util::sync::CancellationToken;

use crate::{
    chase::run_async_processor,
    config::MyConfig,
    error::MyError,
    schemas::{
        billing::{Bill, PaymentFailed, PaymentRequest},
        chaser::Chaser,
    },
    tokio_tools::run_in_tokio_with_cancel,
};

/// Name of the Crate
pub const NAME: &str = env!("CARGO_PKG_NAME");
/// Version of the Crate
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub struct MyState {
    registry: Registry,
}

use crate::schemas::get_schema_id;

pub fn chaser_start(config: &MyConfig) -> Result<(), MyError> {
    let ct = CancellationToken::new();

    run_in_tokio_with_cancel(&config.runtime, ct.clone(), async {
        println!("chaser_start");

        let _hams = Hams::new(ct.clone(), &config.hams).unwrap();

        let (schema_id, schema) =
            get_schema_id::<Chaser>(&config.kafka.registry, &config.kafka.input_topic)
                .await
                .expect("valid schema");

        let (bill_schema_id, bill_schema) =
            get_schema_id::<Bill>(&config.kafka.registry, &config.kafka.input_topic)
                .await
                .expect("valid schema");

        let (pr_schema_id, pr_schema) =
            get_schema_id::<PaymentRequest>(&config.kafka.registry, &config.kafka.input_topic)
                .await
                .expect("valid schema");

        let (pf_schema_id, pf_schema) =
            get_schema_id::<PaymentFailed>(&config.kafka.registry, &config.kafka.input_topic)
                .await
                .expect("valid schema");

        let mut schemas = HashMap::new();

        schemas.insert(schema_id, schema);
        schemas.insert(bill_schema_id, bill_schema);
        schemas.insert(pr_schema_id, pr_schema);
        schemas.insert(pf_schema_id, pf_schema);

        log::info!("Schemas are {:?}", schemas);

        (0..config.kafka.num_workers)
            .map(|_| {
                tokio::spawn(run_async_processor(
                    config.kafka.brokers.to_owned(),
                    config.kafka.group_id.to_owned(),
                    schemas.clone(),
                    config.kafka.input_topic.to_owned(),
                    config.kafka.output_topic.to_owned(),
                ))
            })
            .collect::<FuturesUnordered<_>>()
            .for_each(|_| async {})
            .await;

        Ok(())
    })
}
