use std::{env, error::Error};

use triton_sdk::{AccountSyncConfig, CommitmentConfig, Configured, Pubkey, RpcClient};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let key: Pubkey = "So11111111111111111111111111111111111111112".parse()?;
    let replacement: Pubkey = "SysvarC1ock11111111111111111111111111111111".parse()?;
    let client =
        RpcClient::new_with_commitment(required_url("RPC_URL")?, CommitmentConfig::confirmed())
            .with_account_sync(AccountSyncConfig {
                endpoint: required_url("ACCOUNT_SYNC_URL")?,
                automatic_subscriptions: false,
                ..Default::default()
            })
            .map_err(|error| format!("lifecycle: invalid account-sync configuration: {error}"))?;

    let result = run_lifecycle(&client, key, replacement).await;
    let closed = client
        .close()
        .await
        .map_err(|error| format!("lifecycle: close failed: {error}"));
    if result.is_err()
        && let Err(error) = &closed
    {
        eprintln!("{error}");
    }
    result?;
    closed?;
    Ok(())
}

async fn run_lifecycle(
    client: &RpcClient<Configured>,
    key: Pubkey,
    replacement: Pubkey,
) -> Result<(), Box<dyn Error>> {
    client
        .add_accounts([key])
        .await
        .map_err(|error| format!("lifecycle: add_accounts failed: {error}"))?;
    if client.account_sync_config().pinned_accounts != [key].into() {
        return Err("lifecycle: unexpected pinned set after add_accounts".into());
    }
    println!("added: {:?}", client.account_sync_config().pinned_accounts);
    let response = client
        .get_account_with_commitment(&key, client.commitment())
        .await
        .map_err(|_| "lifecycle: initial get_account_with_commitment failed")?;
    if response.value.is_none() {
        return Err(format!("lifecycle: expected account {key} is missing").into());
    }
    println!("slot={} present=true", response.context.slot);
    client
        .remove_accounts([key])
        .await
        .map_err(|error| format!("lifecycle: remove_accounts failed: {error}"))?;
    if !client.account_sync_config().pinned_accounts.is_empty() {
        return Err("lifecycle: pinned set is not empty after remove_accounts".into());
    }
    println!(
        "removed: {:?}",
        client.account_sync_config().pinned_accounts
    );
    client
        .add_accounts([key])
        .await
        .map_err(|error| format!("lifecycle: re-add_accounts failed: {error}"))?;
    if client.account_sync_config().pinned_accounts != [key].into() {
        return Err("lifecycle: unexpected pinned set after re-add_accounts".into());
    }
    println!(
        "re-added: {:?}",
        client.account_sync_config().pinned_accounts
    );
    client
        .replace_accounts([replacement])
        .await
        .map_err(|error| format!("lifecycle: replace_accounts failed: {error}"))?;
    if client.account_sync_config().pinned_accounts != [replacement].into() {
        return Err("lifecycle: unexpected pinned set after replace_accounts".into());
    }
    println!(
        "replaced: {:?}",
        client.account_sync_config().pinned_accounts
    );
    let response = client
        .get_account_with_commitment(&replacement, client.commitment())
        .await
        .map_err(|_| "lifecycle: replacement get_account_with_commitment failed")?;
    if response.value.is_none() {
        return Err(
            format!("lifecycle: expected replacement account {replacement} is missing").into(),
        );
    }
    println!(
        "replacement={replacement} slot={} present=true",
        response.context.slot
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
