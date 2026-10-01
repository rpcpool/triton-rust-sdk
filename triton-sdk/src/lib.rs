//! A Solana RPC client with optional account-sync subscriptions.

#![deny(unsafe_code)]

mod account_sync;
pub mod config;
pub mod error;
pub mod nonblocking;

pub use config::AccountSyncConfig;
pub use error::{AccountSyncError, ConfigError};
pub use nonblocking::rpc_client::{Configured, Plain, RpcClient};
pub use solana_account::Account;
pub use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
pub use solana_pubkey::Pubkey;
pub use solana_rpc_client_api::{client_error::Error as ClientError, response};
