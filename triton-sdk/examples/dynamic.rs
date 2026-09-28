use triton_sdk::{
    AccountSyncConfig, AccountSyncError, ClientError, ConfigError, Pubkey, RpcClient,
};

#[derive(Debug, thiserror::Error)]
enum ExampleError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error(transparent)]
    AccountSync(#[from] AccountSyncError),
}

#[tokio::main]
async fn main() -> Result<(), ExampleError> {
    let client =
        RpcClient::new("http://localhost:8899".into()).with_account_sync(AccountSyncConfig {
            endpoint: "http://localhost:10000".into(),
            ..Default::default()
        })?;
    let key = Pubkey::new_from_array([2; 32]);
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
