use apache_avro::AvroSchema;
use clap::{Args, Parser, Subcommand};
use env_logger::Env;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use log::error;
use log::info;
use rdkafka::util::get_rdkafka_version;
use std::collections::HashMap;

mod chase;
mod error;
mod schemas;

use chase::get_schema_id;
use chase::run_async_processor;

mod produce;
use crate::produce::{produce, produce_bill, produce_payment_failed, produce_payment_request};

use crate::schemas::billing::{Bill, PaymentFailed, PaymentRequest};
use crate::schemas::chaser::Chaser;

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
        kafka: KafkaService,

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
        kafka: KafkaService,

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
        kafka: KafkaService,

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
        kafka: KafkaService,

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
        #[command(flatten)]
        kafka: KafkaService,

        /// Number of workers
        #[arg(long = "num-workers", short, default_value_t = 1)]
        num_workers: i32,

        /// Input topic
        #[arg(long = "input-topic")]
        input_topic: String,

        /// Output topic
        #[arg(long = "output-topic")]
        output_topic: String,

        /// Group id
        #[arg(long)]
        group_id: String,
    },
    /// Trying another leg of clap
    Me(KafkaService),
}

#[derive(Debug, Args)]
struct KafkaService {
    /// Broker list in kafka format
    #[arg(long, default_value_t = String::from("localhost:9092"))]
    brokers: String,

    /// Schema server in host:port format
    #[arg(long, default_value_t = String::from("http://localhost:8081"))]
    registry: String,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    // Start HERE

    #[derive(Debug, AvroSchema, Clone, PartialEq, Eq)]
    enum MyEnum {
        Foo,
        Bar,
        Baz,
    }

    #[derive(Debug, AvroSchema, Clone, PartialEq)]
    struct TestBasicStructWithDefaultValues {
        #[avro(default = "123")]
        a: i32,
        #[avro(default = r#""The default value for 'b'""#)]
        b: String,
        #[avro(default = "true")]
        condition: bool,
        // no default value for 'c'
        c: f64,
        #[avro(default = r#"{"a": 1, "b": 2}"#)]
        map: HashMap<String, i32>,

        #[avro(default = "[1, 2, 3]")]
        array: Vec<i32>,

        #[avro(default = r#""Foo""#)]
        myenum: MyEnum,

        #[avro(default = "null")]
        previous: Option<i64>,
    }
    println!("{:?}", TestBasicStructWithDefaultValues::get_schema());
    println!(
        "Schema is {}",
        TestBasicStructWithDefaultValues::get_schema().canonical_form()
    );
    println!(
        "Schema is {}",
        TestBasicStructWithDefaultValues::get_schema().canonical_form()
    );

    // END here

    let log_level = Env::default().default_filter_or("info");
    env_logger::Builder::from_env(log_level).init();

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{version_n:08x}, {version_s}");

    match args.command {
        Commands::Inject {
            kafka,
            output_topic,
            count,
            ttl,
            msg_id,
        } => {
            info!("Inject with {kafka:?} to {output_topic}");

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
            .await
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
            .await
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
            .await
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
            .await
        }
        Commands::Run {
            kafka,
            num_workers,
            input_topic,
            output_topic,
            group_id,
        } => {
            let (schema_id, schema) = get_schema_id::<Chaser>(&kafka.registry, &output_topic)
                .await
                .expect("valid schema");

            let (bill_schema_id, bill_schema) =
                get_schema_id::<Bill>(&kafka.registry, &output_topic)
                    .await
                    .expect("valid schema");

            let (pr_schema_id, pr_schema) =
                get_schema_id::<PaymentRequest>(&kafka.registry, &output_topic)
                    .await
                    .expect("valid schema");

            let (pf_schema_id, pf_schema) =
                get_schema_id::<PaymentFailed>(&kafka.registry, &output_topic)
                    .await
                    .expect("valid schema");

            let mut schemas = HashMap::new();

            schemas.insert(schema_id, schema);
            schemas.insert(bill_schema_id, bill_schema);
            schemas.insert(pr_schema_id, pr_schema);
            schemas.insert(pf_schema_id, pf_schema);

            (0..num_workers)
                .map(|_| {
                    tokio::spawn(run_async_processor(
                        kafka.brokers.to_owned(),
                        group_id.to_owned(),
                        schemas.clone(),
                        input_topic.to_owned(),
                        output_topic.to_owned(),
                    ))
                })
                .collect::<FuturesUnordered<_>>()
                .for_each(|_| async {})
                .await
        }
        Commands::Me(service) => {
            error!("Me called with {service:?}");
        }
    }
}
