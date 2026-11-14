use arrow_flight::error::FlightError;
use thiserror::Error;
use tonic::Status;

pub type Result<T> = std::result::Result<T, ProxyError>;

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Authentication failed: {0}")]
    Unauthenticated(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Schema / conversion error: {0}")]
    Conversion(String),

    #[error("Arrow Flight protocol error: {0}")]
    Flight(#[from] FlightError),

    #[error("Aerospike error: {0}")]
    Aerospike(#[from] aerospike::Error),

    #[error("Write failed after {attempts} attempts: {message}")]
    WriteFailed { attempts: u32, message: String },

    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<ProxyError> for Status {
    fn from(err: ProxyError) -> Self {
        match err {
            ProxyError::Config(msg) => Status::internal(format!("Config error: {msg}")),
            ProxyError::Unauthenticated(msg) => Status::unauthenticated(msg),
            ProxyError::InvalidArgument(msg) => Status::invalid_argument(msg),
            ProxyError::Conversion(msg) => {
                Status::invalid_argument(format!("Data conversion error: {msg}"))
            }
            ProxyError::Flight(flight_err) => match flight_err {
                FlightError::Tonic(status) => *status,
                other => Status::invalid_argument(format!("Arrow Flight error: {other}")),
            },
            ProxyError::Aerospike(as_err) => {
                Status::unavailable(format!("Aerospike error: {as_err}"))
            }
            ProxyError::WriteFailed { attempts, message } => Status::aborted(format!(
                "Aerospike write terminated after {attempts} retries: {message}"
            )),
            ProxyError::Internal(msg) => Status::internal(msg),
        }
    }
}
