use std::{env, error::Error};

use triton_sdk::{AccountSyncConfig, CommitmentConfig, Pubkey, RpcClient};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let keys: [Pubkey; 2] = [
        "So11111111111111111111111111111111111111112".parse()?,
        "SysvarC1ock11111111111111111111111111111111".parse()?,
    ];
    let client =
        RpcClient::new_with_commitment(env::var("RPC_URL")?, CommitmentConfig::confirmed())
            .with_account_sync(AccountSyncConfig {
                endpoint: env::var("ACCOUNT_SYNC_URL")?,
                pinned_accounts: keys.into(),
                ..Default::default()
            })?;

    let requested = [keys[0], keys[1], keys[0]];
    let result = client
        .get_multiple_accounts_with_commitment(&requested, client.commitment())
        .await;
    let closed = client.close().await;
    let response = result?;
    closed?;
    println!("slot={}", response.context.slot);
    for (index, (key, account)) in requested.iter().zip(&response.value).enumerate() {
        println!("index={index} account={key} present={}", account.is_some());
    }
    Ok(())
}
