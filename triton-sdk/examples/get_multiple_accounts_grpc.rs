use std::{env, error::Error};

use triton_sdk::{Account, AccountSyncConfig, CommitmentConfig, Pubkey, RpcClient};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let keys: [Pubkey; 2] = [
        "So11111111111111111111111111111111111111112".parse()?,
        "SysvarC1ock11111111111111111111111111111111".parse()?,
    ];
    let client =
        RpcClient::new_with_commitment(required_url("RPC_URL")?, CommitmentConfig::confirmed())
            .with_account_sync(AccountSyncConfig {
                endpoint: required_url("ACCOUNT_SYNC_URL")?,
                pinned_accounts: keys.into(),
                ..Default::default()
            })
            .map_err(|error| {
                format!("multiple accounts: invalid account-sync configuration: {error}")
            })?;

    let requested = [keys[0], keys[1], keys[0]];
    let result = client
        .get_multiple_accounts_with_commitment(&requested, client.commitment())
        .await
        .map_err(|_| "multiple accounts: get_multiple_accounts_with_commitment failed");
    let closed = client
        .close()
        .await
        .map_err(|error| format!("multiple accounts: close failed: {error}"));
    if result.is_err()
        && let Err(error) = &closed
    {
        eprintln!("{error}");
    }
    let response = result?;
    closed?;
    println!("slot={}", response.context.slot);
    check_accounts(&requested, &response.value)
}

fn check_accounts(
    requested: &[Pubkey; 3],
    accounts: &[Option<Account>],
) -> Result<(), Box<dyn Error>> {
    if accounts.len() != requested.len() {
        return Err(format!(
            "multiple accounts: expected {} results, got {}",
            requested.len(),
            accounts.len()
        )
        .into());
    }
    if accounts[0] != accounts[2] {
        return Err("multiple accounts: duplicate key returned different accounts".into());
    }
    for (index, (key, account)) in requested.iter().zip(accounts).enumerate() {
        if account.is_none() {
            return Err(format!(
                "multiple accounts: expected account {key} is missing at index {index}"
            )
            .into());
        }
        println!("index={index} account={key} present={}", account.is_some());
    }
    Ok(())
}

fn required_url(name: &str) -> Result<String, Box<dyn Error>> {
    let value = env::var(name).map_err(|_| format!("{name} must be set"))?;
    if value.trim().is_empty() {
        return Err(format!("{name} must not be empty").into());
    }
    Ok(value)
}
