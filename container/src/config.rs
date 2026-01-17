use figment::{
    providers::{Env, Format, Yaml},
    Figment,
};
use figment_file_provider_adapter::FileAdapter;
use hamsrs::hams::config::HamsConfig;
use serde::Deserialize;
use std::path::Path;

use crate::tokio_tools::ThreadRuntime;

#[derive(Deserialize, Debug, Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    pub registry: url::Url,
    pub num_workers: i32,
    pub input_topic: String,
    pub output_topic: String,
    pub group_id: String,
}

fn default_name() -> String {
    crate::NAME.to_string()
}

fn default_version() -> String {
    crate::VERSION.to_string()
}

#[derive(Deserialize, Debug, Clone)]
pub struct MyConfig {
    #[serde(default = "default_name")]
    pub name: String,
    #[serde(default = "default_version")]
    pub version: String,
    /// Config of my web service
    pub hams: HamsConfig,
    pub runtime: ThreadRuntime,
    pub kafka: KafkaConfig,
}

impl MyConfig {
    // Note the `nested` option on both `file` providers. This makes each
    // top-level dictionary act as a profile.
    pub fn figment<P: AsRef<Path> + Clone>(yaml_string: &str, secrets: P) -> Figment {
        Figment::new()
            .merge(FileAdapter::wrap(Yaml::string(yaml_string)).relative_to_dir(secrets))
            .merge(Env::prefixed("APP_").split("__"))
    }
}
