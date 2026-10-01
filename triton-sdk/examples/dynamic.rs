use std::{env, error::Error, time::Duration};

use triton_sdk::{AccountSyncConfig, ClientError, CommitmentConfig, Pubkey, RpcClient};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let key: Pubkey = "So11111111111111111111111111111111111111112".parse()?;
    let config = AccountSyncConfig {
        endpoint: env::var("ACCOUNT_SYNC_URL")?,
        dynamic_subscription_lifetime: Duration::from_secs(2),
        ..Default::default()
    };
    let wait = config.dynamic_subscription_lifetime
        + config.subscription_refresh
        + Duration::from_millis(100);
    let client =
        RpcClient::new_with_commitment(env::var("RPC_URL")?, CommitmentConfig::confirmed())
            .with_account_sync(config)?;

    let result = async {
        for read in 1..=2 {
            let response = client
                .get_account_with_commitment(&key, client.commitment())
                .await?;
            println!(
                "read={read} slot={} present={}",
                response.context.slot,
                response.value.is_some()
            );
            if read == 1 {
                println!("waiting {wait:?} before renewing the temporary subscription");
                tokio::time::sleep(wait).await;
            }
        }
        Ok::<_, ClientError>(())
    }
    .await;
    let closed = client.close().await;
    result?;
    closed?;
    Ok(())
}
