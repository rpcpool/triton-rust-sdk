use solana_client::nonblocking::rpc_client::RpcClient as SolanaRpcClient;
use solana_pubkey::Pubkey;
use triton_sdk::{config::AccountSyncConfig, nonblocking::rpc_client::RpcClient};

#[derive(Debug, thiserror::Error)]
enum ExampleError {
    #[error(transparent)]
    Config(#[from] triton_sdk::error::ConfigError),
    #[error(transparent)]
    Client(#[from] solana_rpc_client_api::client_error::Error),
    #[error(transparent)]
    AccountSync(#[from] triton_sdk::error::AccountSyncError),
}

#[tokio::main]
async fn main() -> Result<(), ExampleError> {
    let key = Pubkey::new_from_array([1; 32]);
    let mut config = AccountSyncConfig {
        endpoint: "http://localhost:10000".into(),
        ..Default::default()
    };
    config.pinned_accounts.insert(key);
    let client =
        RpcClient::with_account_sync(SolanaRpcClient::new("http://localhost:8899".into()), config)?;
    let account = client
        .get_account_with_commitment(&key, client.commitment())
        .await?;
    println!(
        "account at slot {}: {:?}",
        account.context.slot, account.value
    );
    client.close().await?;
    Ok(())
}
