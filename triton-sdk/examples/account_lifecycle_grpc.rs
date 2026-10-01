use std::{env, error::Error};

use triton_sdk::{AccountSyncConfig, CommitmentConfig, Pubkey, RpcClient};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let key: Pubkey = "So11111111111111111111111111111111111111112".parse()?;
    let replacement: Pubkey = "SysvarC1ock11111111111111111111111111111111".parse()?;
    let client =
        RpcClient::new_with_commitment(env::var("RPC_URL")?, CommitmentConfig::confirmed())
            .with_account_sync(AccountSyncConfig {
                endpoint: env::var("ACCOUNT_SYNC_URL")?,
                automatic_subscriptions: false,
                ..Default::default()
            })?;

    let result = async {
        client.add_accounts([key]).await?;
        println!("added: {:?}", client.account_sync_config().pinned_accounts);
        let response = client
            .get_account_with_commitment(&key, client.commitment())
            .await?;
        println!(
            "slot={} present={}",
            response.context.slot,
            response.value.is_some()
        );
        client.remove_accounts([key]).await?;
        println!(
            "removed: {:?}",
            client.account_sync_config().pinned_accounts
        );
        client.add_accounts([key]).await?;
        println!(
            "re-added: {:?}",
            client.account_sync_config().pinned_accounts
        );
        client.replace_accounts([replacement]).await?;
        println!(
            "replaced: {:?}",
            client.account_sync_config().pinned_accounts
        );
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    let closed = client.close().await;
    result?;
    closed?;
    Ok(())
}
