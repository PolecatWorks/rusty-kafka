pub mod chase;
pub mod config;
pub mod error;
pub mod metrics;
pub mod produce;
pub mod schemas;
pub mod tokio_tools;
use apache_avro::Schema;
use futures::StreamExt;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use std::ffi::c_void;
use std::{collections::HashMap, sync::Arc};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use futures::stream::FuturesUnordered;
use hamsrs::Hams;
use metrics::{prometheus_response_free, prometheus_response_mystate};
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

use opentelemetry_prometheus_text_exporter::PrometheusExporter;

struct TelemetryState {
    exporter: PrometheusExporter,
    meter_provider: SdkMeterProvider,
    // registry: Registry,
}

pub struct MyState {
    config: MyConfig,
    schemas: HashMap<u32, Schema>,
    telemetry: Arc<TelemetryState>,
}

impl MyState {
    pub fn new(config: &MyConfig, schemas: HashMap<u32, Schema>) -> Result<MyState, MyError> {
        // let registry = Registry::new();
        let exporter = PrometheusExporter::new();
        // 2. Build the MeterProvider with the exporter as a reader
        let meter_provider = SdkMeterProvider::builder()
            .with_reader(exporter.clone())
            .build();
        let otel_layer = tracing_opentelemetry::MetricsLayer::new(meter_provider.clone());

        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(otel_layer)
            .try_init()
            .expect("failed to init tracing");

        let telemetry = Arc::new(TelemetryState {
            exporter,
            meter_provider,
            // registry,
        });

        Ok(MyState {
            config: config.clone(),
            schemas,
            telemetry,
        })
    }
}

use crate::schemas::get_schema_id;

pub fn chaser_start(config: &MyConfig) -> Result<(), MyError> {
    let ct = CancellationToken::new();

    run_in_tokio_with_cancel(&config.runtime, ct.clone(), async {
        println!("chaser_start");

        let registry_url = config.kafka.registry.as_str().trim_end_matches('/');

        let (schema_id, schema) = get_schema_id::<Chaser>(registry_url, &config.kafka.input_topic)
            .await
            .expect("valid schema for Chaser");

        let (bill_schema_id, bill_schema) =
            get_schema_id::<Bill>(registry_url, &config.kafka.input_topic)
                .await
                .expect("valid schema for Bill");

        let (pr_schema_id, pr_schema) =
            get_schema_id::<PaymentRequest>(registry_url, &config.kafka.input_topic)
                .await
                .expect("valid schema for PaymentRequest    ");

        let (pf_schema_id, pf_schema) =
            get_schema_id::<PaymentFailed>(registry_url, &config.kafka.input_topic)
                .await
                .expect("valid schema for PaymentFailed");

        let mut schemas = HashMap::new();

        schemas.insert(schema_id, schema);
        schemas.insert(bill_schema_id, bill_schema);
        schemas.insert(pr_schema_id, pr_schema);
        schemas.insert(pf_schema_id, pf_schema);

        log::info!("Schemas are {schemas:?}");

        let state = Arc::new(MyState::new(config, schemas)?);

        let hams = Hams::new(ct.clone(), &config.hams).unwrap();

        hams.register_prometheus(
            // prometheus_response,
            prometheus_response_mystate,
            prometheus_response_free,
            Arc::as_ptr(&state) as *const c_void,
        )?;

        hams.start().unwrap();

        (0..config.kafka.num_workers)
            .map(|_| {
                let state = Arc::clone(&state);
                tokio::spawn(run_async_processor(
                    state, // config.kafka.brokers.to_owned(),
                          // config.kafka.group_id.to_owned(),
                          // schemas.clone(),
                          // config.kafka.input_topic.to_owned(),
                          // config.kafka.output_topic.to_owned(),
                ))
            })
            .collect::<FuturesUnordered<_>>()
            .for_each(|_| async {})
            .await;

        hams.stop()?;
        hams.deregister_prometheus()?;

        Ok(())
    })
}
