// use apache_avro::AvroSchema;
use clap::{Args, Parser, Subcommand};
use rdkafka::util::get_rdkafka_version;
use std::path::PathBuf;
use std::process::ExitCode;
use tracing::level_filters::LevelFilter;
use tracing::{debug, error, info, Level};
use tracing_subscriber::EnvFilter;

use ffi_log2::log_param;
use hamsrs::hams_logger_init;
use kafka_chase::config::MyConfig;
use kafka_chase::error::MyError;
use kafka_chase::produce::{
    produce, produce_bill, produce_payment_failed, produce_payment_request,
};
use kafka_chase::schemas::billing::{Bill, PaymentFailed, PaymentRequest};
use kafka_chase::schemas::chaser::Chaser;
use kafka_chase::schemas::get_schema_id;
use kafka_chase::tokio_tools::{run_in_tokio, ThreadRuntime};
use kafka_chase::{chaser_start, NAME, VERSION};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Injects message
    Inject {
        #[command(flatten)]
        kafka: KafkaConfig,

        /// Output topic
        #[arg(long)]
        output_topic: String,

        /// Message count
        #[arg(long, default_value_t = 1)]
        count: u32,

        /// Message ttl
        #[arg(long, default_value_t = 10)]
        ttl: u32,

        /// Message id
        #[arg(long, default_value_t=String::from("unlabelled"))]
        msg_id: String,
    },
    /// Injects a Bill message
    InjectBill {
        #[command(flatten)]
        kafka: KafkaConfig,

        /// Output topic
        #[arg(long)]
        output_topic: String,

        /// Number of customers
        #[arg(long, default_value_t = 1)]
        num_customers: u32,

        /// Bills per customer
        #[arg(long, default_value_t = 1)]
        bills_per_customer: u32,

        /// Customer prefix
        #[arg(long, default_value = "cust")]
        customer_prefix: String,

        /// Starting customer index
        #[arg(long, default_value_t = 0)]
        start_customer_index: u32,

        /// Starting bill index
        #[arg(long, default_value_t = 0)]
        start_bill_index: u32,

        /// Amount in cents
        #[arg(long)]
        amount_cents: i64,
    },
    /// Injects a PaymentRequest message
    InjectPaymentRequest {
        #[command(flatten)]
        kafka: KafkaConfig,

        /// Output topic
        #[arg(long)]
        output_topic: String,

        /// Message count
        #[arg(long, default_value_t = 1)]
        count: u32,

        /// Bill ID
        #[arg(long)]
        bill_id: String,

        /// Customer ID
        #[arg(long)]
        customer_id: String,

        /// Amount in cents
        #[arg(long)]
        amount_cents: i64,
    },
    /// Injects a PaymentFailed message
    InjectPaymentFailed {
        #[command(flatten)]
        kafka: KafkaConfig,

        /// Output topic
        #[arg(long)]
        output_topic: String,

        /// Message count
        #[arg(long, default_value_t = 1)]
        count: u32,

        /// Payment ID
        #[arg(long)]
        payment_id: String,

        /// Bill ID
        #[arg(long)]
        bill_id: String,

        /// Customer ID
        #[arg(long)]
        customer_id: String,

        /// Failure reason
        #[arg(long)]
        reason: String,
    },
    /// Run the processing loop
    Run {
        /// Sets a custom config file
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,
        /// Sets a custom secrets directory
        #[arg(short, long, value_name = "DIR", default_value = "secrets")]
        secrets: PathBuf,
    },
    /// Trying another leg of clap
    Me(KafkaConfig),
}

#[derive(Debug, Args)]
struct KafkaConfig {
    /// Broker list in kafka format
    #[arg(long, default_value_t = String::from("localhost:9092"))]
    brokers: String,

    /// Schema server in host:port format
    #[arg(long, default_value_t = String::from("http://localhost:8081"))]
    registry: String,
}

fn main() -> Result<ExitCode, MyError> {
    let env = EnvFilter::builder()
        .with_default_directive(LevelFilter::WARN.into())
        .with_env_var("CAPTURE_LOG")
        .from_env()?;

    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_env_filter(env)
        .init();

    let args = Cli::parse();

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{version_n:08x}, {version_s}");

    let min_runtime_config = ThreadRuntime::default();

    match args.command {
        Commands::Inject {
            kafka,
            output_topic,
            count,
            ttl,
            msg_id,
        } => {
            println!("Inject with {kafka:?} to {output_topic} x {count}");

            run_in_tokio(&min_runtime_config, async {
                let (schema_id, schema) = get_schema_id::<Chaser>(&kafka.registry, &output_topic)
                    .await
                    .expect("valid schema");

                produce(
                    &kafka.brokers,
                    &output_topic,
                    &schema,
                    schema_id,
                    count,
                    ttl,
                    &msg_id,
                )
                .await;
                Ok(())
            })?;

            println!("Inject complete");
        }
        Commands::InjectBill {
            kafka,
            output_topic,
            num_customers,
            bills_per_customer,
            customer_prefix,
            start_customer_index,
            start_bill_index,
            amount_cents,
        } => {
            info!("Inject Bill with {kafka:?} to {output_topic}");
            run_in_tokio(&min_runtime_config, async {
                let (schema_id, schema) = get_schema_id::<Bill>(&kafka.registry, &output_topic)
                    .await
                    .expect("valid schema");

                produce_bill(
                    &kafka.brokers,
                    &output_topic,
                    &schema,
                    schema_id,
                    &customer_prefix,
                    num_customers,
                    bills_per_customer,
                    start_customer_index,
                    start_bill_index,
                    amount_cents,
                )
                .await;
                Ok(())
            })?
        }

        Commands::InjectPaymentRequest {
            kafka,
            output_topic,
            count,
            bill_id,
            customer_id,
            amount_cents,
        } => {
            info!("Inject PaymentRequest with {kafka:?} to {output_topic}");

            run_in_tokio(&min_runtime_config, async {
                let (schema_id, schema) =
                    get_schema_id::<PaymentRequest>(&kafka.registry, &output_topic)
                        .await
                        .expect("valid schema");

                produce_payment_request(
                    &kafka.brokers,
                    &output_topic,
                    &schema,
                    schema_id,
                    count,
                    &bill_id,
                    &customer_id,
                    amount_cents,
                )
                .await;
                Ok(())
            })?
        }
        Commands::InjectPaymentFailed {
            kafka,
            output_topic,
            count,
            payment_id,
            bill_id,
            customer_id,
            reason,
        } => {
            info!("Inject PaymentFailed with {kafka:?} to {output_topic}");

            run_in_tokio(&min_runtime_config, async {
                let (schema_id, schema) =
                    get_schema_id::<PaymentFailed>(&kafka.registry, &output_topic)
                        .await
                        .expect("valid schema");

                produce_payment_failed(
                    &kafka.brokers,
                    &output_topic,
                    &schema,
                    schema_id,
                    count,
                    &payment_id,
                    &bill_id,
                    &customer_id,
                    &reason,
                )
                .await;
                Ok(())
            })?
        }
        Commands::Run { config, secrets } => {
            info!("Starting {NAME}:{VERSION}");

            hams_logger_init(log_param()).unwrap();

            let config_yaml = match std::fs::read_to_string(config.clone()) {
                Ok(content) => content,
                Err(e) => {
                    error!("Failed to read config file {:?}: {}", config, e);
                    return Err(MyError::Io(e));
                }
            };

            let config: MyConfig = MyConfig::figment(&config_yaml, secrets)
                .extract()
                .unwrap_or_else(|err| {
                    error!("Config file {config:?} failed with error \n{err:#?}");
                    panic!("Config failed to load");
                });

            debug!("Loaded config {:?}", config);

            chaser_start(&config)?
        }
        Commands::Me(service) => {
            error!("Me called with {service:?}");
        }
    }
    Ok(ExitCode::SUCCESS)
}
