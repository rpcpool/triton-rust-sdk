use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("account-sync endpoint must use http or https")]
    EndpointScheme,
    #[error("invalid account-sync endpoint URL")]
    EndpointUrl(#[source] url::ParseError),
    #[error("invalid account-sync endpoint path: expected no path or /<token>")]
    EndpointPath,
    #[error("account-sync token is not valid gRPC metadata")]
    EndpointToken,
    #[error("invalid account-sync endpoint")]
    Endpoint(#[source] tonic::transport::Error),
    #[error("account-sync setting must be nonzero: {0}")]
    Zero(&'static str),
    #[error("reconnect minimum delay exceeds maximum delay")]
    ReconnectRange,
}

#[derive(Debug, Error)]
pub enum AccountSyncError {
    #[error("invalid account-sync configuration")]
    Config(#[from] ConfigError),
    #[error("account-sync runtime is closed")]
    Closed,
    #[error("account-sync operation requires a Tokio runtime")]
    NoRuntime,
    #[error("account-sync command channel closed")]
    ChannelClosed,
    #[error("account-sync transport failed")]
    Transport(#[from] tonic::transport::Error),
    #[error("account-sync stream failed")]
    Stream(#[from] tonic::Status),
    #[error("account-sync shutdown timed out")]
    CloseTimeout,
    #[error("account-sync task failed")]
    TaskJoin(#[from] tokio::task::JoinError),
}
