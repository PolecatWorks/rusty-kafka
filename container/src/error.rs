use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MyError {
    #[error("Service cancelled and shutting down")]
    Cancelled,
    #[error("A compiled generic error wtih message: `{0}`")]
    Message(&'static str),
    #[error("A dynamic generic error wtih message: `{0}`")]
    DynMessage(String),
    #[error("unknown data store error")]
    Unknown,
    #[error("Avro error: `{0}`")]
    AvroError(#[from] apache_avro::Error),

    #[error("data store disconnected")]
    Io(#[from] io::Error),
    #[error("Env filter error: `{0}`")]
    EnvFilterError(#[from] tracing_subscriber::filter::FromEnvError),
}

impl From<()> for MyError {
    fn from(_: ()) -> Self {
        MyError::Unknown
    }
}
