use std::{env, error::Error};

use triton_sdk::{AccountSyncConfig, CommitmentConfig, Pubkey, RpcClient};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let key: Pubkey = "So11111111111111111111111111111111111111112".parse()?;
    let client =
        RpcClient::new_with_commitment(env::var("RPC_URL")?, CommitmentConfig::confirmed())
            .with_account_sync(AccountSyncConfig {
                endpoint: env::var("ACCOUNT_SYNC_URL")?,
                pinned_accounts: [key].into(),
                ..Default::default()
            })?;

    let result = client
        .get_account_with_commitment(&key, client.commitment())
        .await;
    let closed = client.close().await;
    let response = result?;
    closed?;
    println!(
        "account={key} slot={} present={}",
        response.context.slot,
        response.value.is_some()
    );
    Ok(())
}
