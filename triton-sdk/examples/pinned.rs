use std::{env, error::Error};

use triton_sdk::{AccountSyncConfig, CommitmentConfig, Pubkey, RpcClient};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let key: Pubkey = "So11111111111111111111111111111111111111112".parse()?;
    let client =
        RpcClient::new_with_commitment(required_url("RPC_URL")?, CommitmentConfig::confirmed())
            .with_account_sync(AccountSyncConfig {
                endpoint: required_url("ACCOUNT_SYNC_URL")?,
                pinned_accounts: [key].into(),
                ..Default::default()
            })
            .map_err(|error| format!("pinned: invalid account-sync configuration: {error}"))?;

    let result = client
        .get_account_with_commitment(&key, client.commitment())
        .await
        .map_err(|_| "pinned: get_account_with_commitment failed");
    let closed = client
        .close()
        .await
        .map_err(|error| format!("pinned: close failed: {error}"));
    if result.is_err()
        && let Err(error) = &closed
    {
        eprintln!("{error}");
    }
    let response = result?;
    closed?;
    if response.value.is_none() {
        return Err(format!("pinned: expected account {key} is missing").into());
    }
    println!(
        "account={key} slot={} present={}",
        response.context.slot,
        response.value.is_some()
    );
    Ok(())
}

fn required_url(name: &str) -> Result<String, Box<dyn Error>> {
    let value = env::var(name).map_err(|_| format!("{name} must be set"))?;
    if value.trim().is_empty() {
        return Err(format!("{name} must not be empty").into());
    }
    Ok(value)
}
