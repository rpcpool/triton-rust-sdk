use std::{ops::Deref, sync::Arc, time::Duration};

use solana_account::Account;
use solana_client::nonblocking::rpc_client::RpcClient as SolanaRpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::{
    client_error::Result as ClientResult,
    request::RpcError,
    response::{Response, RpcResponseContext, RpcResult},
};

use crate::account_sync::runtime::PreparedRead;
use crate::account_sync::{cache::CachedAccount, lane::Command};
use crate::{
    account_sync::AccountSyncRuntime,
    config::AccountSyncConfig,
    error::{AccountSyncError, ConfigError},
};

/// A Solana RPC client
#[derive(Clone)]
pub struct RpcClient<Mode = Plain> {
    inner: Arc<SolanaRpcClient>,
    mode: Mode,
}

/// Uses Solana RPC directly and owns no account-sync runtime.
#[derive(Clone, Debug)]
pub struct Plain;

/// Enables cached raw account reads and account-sync controls.
#[derive(Clone)]
pub struct Configured {
    account_sync: Arc<AccountSyncRuntime>,
}

impl RpcClient<Plain> {
    pub fn new(url: String) -> Self {
        Self::from_inner(SolanaRpcClient::new(url))
    }

    pub fn new_with_commitment(url: String, commitment: CommitmentConfig) -> Self {
        Self::from_inner(SolanaRpcClient::new_with_commitment(url, commitment))
    }

    pub fn new_with_timeout(url: String, timeout: Duration) -> Self {
        Self::from_inner(SolanaRpcClient::new_with_timeout(url, timeout))
    }

    pub fn new_with_timeout_and_commitment(
        url: String,
        timeout: Duration,
        commitment: CommitmentConfig,
    ) -> Self {
        Self::from_inner(SolanaRpcClient::new_with_timeout_and_commitment(
            url, timeout, commitment,
        ))
    }

    pub fn from_inner(inner: SolanaRpcClient) -> Self {
        Self {
            inner: Arc::new(inner),
            mode: Plain,
        }
    }

    /// Configures account sync without starting a background task.
    pub fn with_account_sync(
        self,
        config: AccountSyncConfig,
    ) -> Result<RpcClient<Configured>, ConfigError> {
        config.validate()?;
        let account_sync = Arc::new(AccountSyncRuntime::new(config, self.inner.clone()));
        Ok(RpcClient {
            inner: self.inner,
            mode: Configured { account_sync },
        })
    }
}

impl<Mode> RpcClient<Mode> {
    pub fn inner(&self) -> &SolanaRpcClient {
        &self.inner
    }
}

impl RpcClient<Configured> {
    /// Returns a snapshot including the current pinned account set.
    pub fn account_sync_config(&self) -> AccountSyncConfig {
        self.mode.account_sync.config()
    }

    pub async fn add_accounts(
        &self,
        keys: impl IntoIterator<Item = Pubkey>,
    ) -> Result<(), AccountSyncError> {
        self.mode.account_sync.add_accounts(keys).await
    }

    pub async fn remove_accounts(
        &self,
        keys: impl IntoIterator<Item = Pubkey>,
    ) -> Result<(), AccountSyncError> {
        self.mode.account_sync.remove_accounts(keys).await
    }

    pub async fn replace_accounts(
        &self,
        keys: impl IntoIterator<Item = Pubkey>,
    ) -> Result<(), AccountSyncError> {
        self.mode.account_sync.replace_accounts(keys).await
    }

    pub async fn close(&self) -> Result<(), AccountSyncError> {
        self.mode.account_sync.close().await
    }

    pub async fn get_account(&self, pubkey: &Pubkey) -> ClientResult<Account> {
        self.get_account_with_commitment(pubkey, self.inner.commitment())
            .await?
            .value
            .ok_or_else(|| RpcError::ForUser(format!("AccountNotFound: pubkey={pubkey}")).into())
    }

    pub async fn get_account_data(&self, pubkey: &Pubkey) -> ClientResult<Vec<u8>> {
        Ok(self.get_account(pubkey).await?.data)
    }

    pub async fn get_account_with_commitment(
        &self,
        pubkey: &Pubkey,
        commitment: CommitmentConfig,
    ) -> RpcResult<Option<Account>> {
        let Ok(prepared) = self
            .mode
            .account_sync
            .prepare(commitment, vec![*pubkey])
            .await
        else {
            return self
                .inner
                .get_account_with_commitment(pubkey, commitment)
                .await;
        };
        let PreparedRead { buffered, commands } = prepared;
        let rpc = self.inner.get_account_with_commitment(pubkey, commitment);
        tokio::pin!(rpc);
        let wait = self.mode.account_sync.cache_miss_wait();
        tokio::select! {
            biased;
            result = tokio::time::timeout(wait, buffered) => {
                if let Ok(Ok(mut values)) = result
                    && let Some(value) = values.pop()
                {
                    return Ok(Response { context: RpcResponseContext::new(value.slot), value: value.account });
                }
                let response = rpc.await;
                if let Ok(response) = &response {
                    record(&commands, *pubkey, response.value.clone(), response.context.slot).await;
                }
                response
            }
            response = &mut rpc => {
                if let Ok(response) = &response {
                    record(&commands, *pubkey, response.value.clone(), response.context.slot).await;
                }
                response
            }
        }
    }

    pub async fn get_multiple_accounts(
        &self,
        pubkeys: &[Pubkey],
    ) -> ClientResult<Vec<Option<Account>>> {
        Ok(self
            .get_multiple_accounts_with_commitment(pubkeys, self.inner.commitment())
            .await?
            .value)
    }

    pub async fn get_multiple_accounts_with_commitment(
        &self,
        pubkeys: &[Pubkey],
        commitment: CommitmentConfig,
    ) -> RpcResult<Vec<Option<Account>>> {
        if pubkeys.is_empty() || pubkeys.len() > 100 {
            return self
                .inner
                .get_multiple_accounts_with_commitment(pubkeys, commitment)
                .await;
        }
        let Ok(prepared) = self
            .mode
            .account_sync
            .prepare(commitment, pubkeys.to_vec())
            .await
        else {
            return self
                .inner
                .get_multiple_accounts_with_commitment(pubkeys, commitment)
                .await;
        };
        let PreparedRead { buffered, commands } = prepared;
        let rpc = self
            .inner
            .get_multiple_accounts_with_commitment(pubkeys, commitment);
        tokio::pin!(rpc);
        let wait = self.mode.account_sync.cache_miss_wait();
        tokio::select! {
            biased;
            result = tokio::time::timeout(wait, buffered) => {
                if let Ok(Ok(values)) = result
                    && let Some(response) = buffered_multiple(values)
                {
                    return Ok(response);
                }
                let response = rpc.await;
                record_multiple(&commands, pubkeys, &response).await;
                response
            }
            response = &mut rpc => {
                record_multiple(&commands, pubkeys, &response).await;
                response
            }
        }
    }
}

fn buffered_multiple(values: Vec<CachedAccount>) -> Option<Response<Vec<Option<Account>>>> {
    let slot = values.iter().map(|entry| entry.slot).min()?;
    Some(Response {
        context: RpcResponseContext::new(slot),
        value: values.into_iter().map(|entry| entry.account).collect(),
    })
}

async fn record(
    commands: &tokio::sync::mpsc::Sender<Command>,
    key: Pubkey,
    account: Option<Account>,
    slot: u64,
) {
    let _ = commands.send(Command::Rpc { key, account, slot }).await;
}

async fn record_multiple(
    commands: &tokio::sync::mpsc::Sender<Command>,
    keys: &[Pubkey],
    response: &RpcResult<Vec<Option<Account>>>,
) {
    let Ok(response) = response else {
        return;
    };
    if response.value.len() != keys.len() {
        return;
    }
    for (key, account) in keys.iter().zip(&response.value) {
        record(commands, *key, account.clone(), response.context.slot).await;
    }
}

impl<Mode> Deref for RpcClient<Mode> {
    type Target = SolanaRpcClient;
    fn deref(&self) -> &Self::Target {
        self.inner()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configured(keys: &[Pubkey]) -> RpcClient<Configured> {
        RpcClient::from_inner(SolanaRpcClient::new_mock("succeeds".into()))
            .with_account_sync(AccountSyncConfig {
                endpoint: "http://127.0.0.1:9".into(),
                pinned_accounts: keys.iter().copied().collect(),
                automatic_subscriptions: false,
                ..Default::default()
            })
            .unwrap()
    }

    async fn seed(
        client: &RpcClient<Configured>,
        key: Pubkey,
        account: Option<Account>,
        slot: u64,
    ) {
        let prepared = client
            .mode
            .account_sync
            .prepare(CommitmentConfig::finalized(), vec![key])
            .await
            .unwrap();
        record(&prepared.commands, key, account, slot).await;
    }

    #[test]
    fn constructors_and_configuration_need_no_tokio_runtime() {
        let commitment = CommitmentConfig::confirmed();
        let timeout = Duration::from_secs(3);
        let url = "http://localhost:8899";
        let clients: [RpcClient<Plain>; 4] = [
            RpcClient::new(url.into()),
            RpcClient::new_with_commitment(url.into(), commitment),
            RpcClient::new_with_timeout(url.into(), timeout),
            RpcClient::new_with_timeout_and_commitment(url.into(), timeout, commitment),
        ];
        assert_eq!(clients[1].commitment(), commitment);
        assert_eq!(clients[3].commitment(), commitment);
        for plain in clients {
            assert_eq!(plain.url(), url);
            assert_eq!(plain.inner().commitment(), plain.commitment());
            let configured: RpcClient<Configured> = plain
                .with_account_sync(AccountSyncConfig {
                    endpoint: "http://localhost:10000".into(),
                    ..Default::default()
                })
                .unwrap();
            assert_eq!(configured.url(), url);
        }
        assert!(matches!(
            RpcClient::new(url.into()).with_account_sync(AccountSyncConfig::default()),
            Err(ConfigError::EndpointUrl(_))
        ));
    }

    #[tokio::test]
    async fn plain_account_methods_use_solana_rpc() {
        let client: RpcClient = RpcClient::from_inner(SolanaRpcClient::new_mock("succeeds".into()));
        let key = Pubkey::new_unique();
        let response = client
            .get_account_with_commitment(&key, CommitmentConfig::finalized())
            .await
            .unwrap();
        assert_eq!(response.context.slot, 1);
        assert!(response.value.is_none());
        assert_eq!(
            client.get_multiple_accounts(&[key, key]).await.unwrap(),
            vec![None, None]
        );
    }

    #[tokio::test]
    async fn cache_hits_take_priority_over_ready_rpc_and_respect_commitment() {
        let key = Pubkey::new_unique();
        let client = configured(&[key]);
        let account = Account {
            lamports: 42,
            data: vec![1, 2],
            ..Default::default()
        };
        seed(&client, key, Some(account.clone()), 12).await;
        assert_eq!(client.get_account(&key).await.unwrap(), account);
        assert_eq!(client.get_account_data(&key).await.unwrap(), vec![1, 2]);
        let confirmed = client
            .get_account_with_commitment(&key, CommitmentConfig::confirmed())
            .await
            .unwrap();
        assert_eq!(confirmed.context.slot, 1);
        assert!(confirmed.value.is_none());
        client.close().await.unwrap();
        assert!(
            client
                .get_account_with_commitment(&key, CommitmentConfig::finalized())
                .await
                .unwrap()
                .value
                .is_none()
        );
    }

    #[tokio::test]
    async fn multiple_reads_use_a_complete_source() {
        let keys = [Pubkey::new_unique(), Pubkey::new_unique()];
        let client = configured(&keys);
        let account = Account {
            lamports: 42,
            ..Default::default()
        };
        seed(&client, keys[0], Some(account.clone()), 12).await;
        let requested = [keys[0], keys[1], keys[0]];
        let response = client
            .get_multiple_accounts_with_commitment(&keys, CommitmentConfig::finalized())
            .await
            .unwrap();
        assert_eq!(response.context.slot, 1);
        assert_eq!(response.value, vec![None, None]);
        seed(&client, keys[1], None, 7).await;
        let response = client
            .get_multiple_accounts_with_commitment(&requested, CommitmentConfig::finalized())
            .await
            .unwrap();
        assert_eq!(response.context.slot, 7);
        assert_eq!(
            response.value,
            vec![Some(account.clone()), None, Some(account)]
        );
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn rpc_results_are_returned_when_the_account_is_not_wanted() {
        let client = configured(&[]);
        let key = Pubkey::new_unique();
        let response = client
            .get_account_with_commitment(&key, CommitmentConfig::finalized())
            .await
            .unwrap();
        assert_eq!(response.context.slot, 1);
        assert!(response.value.is_none());
        let mut prepared = client
            .mode
            .account_sync
            .prepare(CommitmentConfig::finalized(), vec![key])
            .await
            .unwrap();
        assert!(matches!(
            prepared.buffered.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn cloned_clients_share_controls_and_config_snapshots_are_owned() {
        let first = Pubkey::new_unique();
        let second = Pubkey::new_unique();
        let client = configured(&[first]);
        let clone = client.clone();
        let snapshot = client.account_sync_config();
        clone.add_accounts([second, second]).await.unwrap();
        assert_eq!(client.account_sync_config().pinned_accounts.len(), 2);
        clone.remove_accounts([first]).await.unwrap();
        assert_eq!(
            client.account_sync_config().pinned_accounts,
            [second].into()
        );
        assert_eq!(snapshot.pinned_accounts, [first].into());
        clone.replace_accounts([first]).await.unwrap();
        assert_eq!(client.account_sync_config().pinned_accounts, [first].into());
        clone.close().await.unwrap();
        assert!(matches!(
            client.add_accounts([second]).await,
            Err(AccountSyncError::Closed)
        ));
    }

    #[tokio::test]
    async fn account_changes_reach_existing_and_new_lanes() {
        let first = Pubkey::new_unique();
        let second = Pubkey::new_unique();
        let client = configured(&[first]);
        let account = Account {
            lamports: 42,
            ..Default::default()
        };
        seed(&client, first, Some(account.clone()), 12).await;
        client.remove_accounts([first]).await.unwrap();
        assert!(client.get_account(&first).await.is_err());
        client.add_accounts([second]).await.unwrap();
        seed(&client, second, Some(account.clone()), 12).await;
        assert_eq!(client.get_account(&second).await.unwrap(), account);
        let commitment = CommitmentConfig::confirmed();
        let prepared = client
            .mode
            .account_sync
            .prepare(commitment, vec![second])
            .await
            .unwrap();
        record(&prepared.commands, second, Some(account.clone()), 13).await;
        assert_eq!(
            client
                .get_account_with_commitment(&second, commitment)
                .await
                .unwrap()
                .value,
            Some(account.clone())
        );
        client.replace_accounts([first]).await.unwrap();
        assert!(client.get_account(&second).await.is_err());
        seed(&client, first, Some(account.clone()), 14).await;
        assert_eq!(client.get_account(&first).await.unwrap(), account);
        client.close().await.unwrap();
    }

    #[test]
    fn buffered_multiple_keeps_positions_and_lowest_slot() {
        let account = Account {
            lamports: 42,
            ..Default::default()
        };
        let values = vec![
            CachedAccount {
                account: Some(account.clone()),
                slot: 12,
                version: 2,
                stream: true,
            },
            CachedAccount {
                account: None,
                slot: 7,
                version: 0,
                stream: false,
            },
            CachedAccount {
                account: Some(account),
                slot: 12,
                version: 2,
                stream: true,
            },
        ];
        let response = buffered_multiple(values).unwrap();
        assert_eq!(response.context.slot, 7);
        assert_eq!(response.value.len(), 3);
        assert_eq!(response.value[0], response.value[2]);
        assert!(response.value[1].is_none());
    }

    #[tokio::test]
    async fn unavailable_stream_keeps_native_rpc_results() {
        let config = AccountSyncConfig {
            endpoint: "http://127.0.0.1:9".into(),
            ..Default::default()
        };
        let client = RpcClient::from_inner(SolanaRpcClient::new_mock("succeeds".into()))
            .with_account_sync(config)
            .unwrap();
        let keys = [
            Pubkey::new_from_array([1; 32]),
            Pubkey::new_from_array([2; 32]),
        ];
        let response = client
            .get_multiple_accounts_with_commitment(&keys, CommitmentConfig::finalized())
            .await
            .unwrap();
        assert_eq!(response.context.slot, 1);
        assert_eq!(response.value, vec![None, None]);
        let account = client
            .get_account_with_commitment(&keys[0], CommitmentConfig::finalized())
            .await
            .unwrap();
        assert_eq!(account.context.slot, 1);
        assert!(account.value.is_none());
        assert!(client.get_account(&keys[0]).await.is_err());
        client.close().await.unwrap();
    }
}
