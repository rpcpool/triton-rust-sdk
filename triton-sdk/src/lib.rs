//! A Solana RPC client with optional account-sync subscriptions.
//!
//! ```
//! use triton_sdk::nonblocking::rpc_client::RpcClient;
//! let client = RpcClient::new("http://localhost:8899".to_owned());
//! assert_eq!(client.inner().commitment(), client.commitment());
//! ```

#![deny(unsafe_code)]

mod account_sync;
pub mod config;
pub mod error;
pub mod nonblocking;
