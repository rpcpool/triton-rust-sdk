use std::{env, error::Error, time::Duration};

use triton_sdk::{AccountSyncConfig, CommitmentConfig, Configured, Pubkey, RpcClient};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let key: Pubkey = "So11111111111111111111111111111111111111112".parse()?;
    let config = AccountSyncConfig {
        endpoint: required_url("ACCOUNT_SYNC_URL")?,
        dynamic_subscription_lifetime: Duration::from_secs(2),
        ..Default::default()
    };
    let wait = config.dynamic_subscription_lifetime
        + config.subscription_refresh
        + Duration::from_millis(100);
    let client =
        RpcClient::new_with_commitment(required_url("RPC_URL")?, CommitmentConfig::confirmed())
            .with_account_sync(config)
            .map_err(|error| format!("dynamic: invalid account-sync configuration: {error}"))?;

    let result = read_twice(&client, &key, wait).await;
    let closed = client
        .close()
        .await
        .map_err(|error| format!("dynamic: close failed: {error}"));
    if result.is_err()
        && let Err(error) = &closed
    {
        eprintln!("{error}");
    }
    result?;
    closed?;
    Ok(())
}

async fn read_twice(
    client: &RpcClient<Configured>,
    key: &Pubkey,
    wait: Duration,
) -> Result<(), Box<dyn Error>> {
    for read in 1..=2 {
        let response = client
            .get_account_with_commitment(key, client.commitment())
            .await
            .map_err(|_| format!("dynamic: get_account_with_commitment failed on read {read}"))?;
        if response.value.is_none() {
            return Err(
                format!("dynamic: expected account {key} is missing on read {read}").into(),
            );
        }
        println!("read={read} slot={} present=true", response.context.slot);
        if read == 1 {
            println!("waiting {wait:?} before renewing the temporary subscription");
            tokio::time::sleep(wait).await;
        }
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
