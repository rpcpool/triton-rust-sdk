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

/// A Solana RPC client with optional cached raw account reads.
///
/// UI account methods and all other Solana methods pass through to [`SolanaRpcClient`].
/// A buffered multiple-account result may contain observations from different slots.
#[derive(Clone)]
pub struct RpcClient {
    inner: Arc<SolanaRpcClient>,
    account_sync: Arc<AccountSyncRuntime>,
}

impl RpcClient {
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
        let inner = Arc::new(inner);
        let account_sync = Arc::new(AccountSyncRuntime::new(None, inner.clone()));
        Self {
            inner,
            account_sync,
        }
    }

    /// Configures account sync without starting a background task.
    pub fn with_account_sync(
        inner: SolanaRpcClient,
        config: AccountSyncConfig,
    ) -> Result<Self, ConfigError> {
        config.validate()?;
        let inner = Arc::new(inner);
        let account_sync = Arc::new(AccountSyncRuntime::new(Some(config), inner.clone()));
        Ok(Self {
            inner,
            account_sync,
        })
    }

    pub fn inner(&self) -> &SolanaRpcClient {
        &self.inner
    }

    pub fn account_sync_config(&self) -> Option<&AccountSyncConfig> {
        self.account_sync.config()
    }

    pub async fn add_pinned_accounts(
        &self,
        keys: impl IntoIterator<Item = Pubkey>,
    ) -> Result<(), AccountSyncError> {
        self.account_sync.add_pinned(keys).await
    }

    pub async fn remove_pinned_accounts(
        &self,
        keys: impl IntoIterator<Item = Pubkey>,
    ) -> Result<(), AccountSyncError> {
        self.account_sync.remove_pinned(keys).await
    }

    pub async fn replace_pinned_accounts(
        &self,
        keys: impl IntoIterator<Item = Pubkey>,
    ) -> Result<(), AccountSyncError> {
        self.account_sync.replace_pinned(keys).await
    }

    pub async fn close(&self) -> Result<(), AccountSyncError> {
        self.account_sync.close().await
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
        let Ok(Some(prepared)) = self.account_sync.prepare(commitment, vec![*pubkey]).await else {
            return self
                .inner
                .get_account_with_commitment(pubkey, commitment)
                .await;
        };
        let PreparedRead {
            generations,
            session,
            buffered,
            commands,
        } = prepared;
        let rpc = self.inner.get_account_with_commitment(pubkey, commitment);
        tokio::pin!(rpc);
        let wait = self
            .account_sync
            .config()
            .map(|config| config.cache_miss_wait)
            .unwrap_or_default();
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
                    record(&commands, *pubkey, generations[0], session, response.value.clone(), response.context.slot).await;
                }
                response
            }
            response = &mut rpc => {
                if let Ok(response) = &response {
                    record(&commands, *pubkey, generations[0], session, response.value.clone(), response.context.slot).await;
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
        let Ok(Some(prepared)) = self
            .account_sync
            .prepare(commitment, pubkeys.to_vec())
            .await
        else {
            return self
                .inner
                .get_multiple_accounts_with_commitment(pubkeys, commitment)
                .await;
        };
        let PreparedRead {
            generations,
            session,
            buffered,
            commands,
        } = prepared;
        let rpc = self
            .inner
            .get_multiple_accounts_with_commitment(pubkeys, commitment);
        tokio::pin!(rpc);
        let wait = self
            .account_sync
            .config()
            .map(|config| config.cache_miss_wait)
            .unwrap_or_default();
        tokio::select! {
            biased;
            result = tokio::time::timeout(wait, buffered) => {
                if let Ok(Ok(values)) = result
                    && let Some(response) = buffered_multiple(values)
                {
                    return Ok(response);
                }
                let response = rpc.await;
                record_multiple(&commands, pubkeys, &generations, session, &response).await;
                response
            }
            response = &mut rpc => {
                record_multiple(&commands, pubkeys, &generations, session, &response).await;
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
    generation: u64,
    session: u64,
    account: Option<Account>,
    slot: u64,
) {
    let _ = commands
        .send(Command::Rpc {
            key,
            generation,
            session,
            account,
            slot,
        })
        .await;
}

async fn record_multiple(
    commands: &tokio::sync::mpsc::Sender<Command>,
    keys: &[Pubkey],
    generations: &[u64],
    session: u64,
    response: &RpcResult<Vec<Option<Account>>>,
) {
    let Ok(response) = response else {
        return;
    };
    if response.value.len() != keys.len() {
        return;
    }
    for ((key, generation), account) in keys.iter().zip(generations).zip(&response.value) {
        record(
            commands,
            *key,
            *generation,
            session,
            account.clone(),
            response.context.slot,
        )
        .await;
    }
}

impl Deref for RpcClient {
    type Target = SolanaRpcClient;
    fn deref(&self) -> &Self::Target {
        self.inner()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                generation: 1,
                stream: true,
            },
            CachedAccount {
                account: None,
                slot: 7,
                version: 0,
                generation: 2,
                stream: false,
            },
            CachedAccount {
                account: Some(account),
                slot: 12,
                version: 2,
                generation: 1,
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
        let client =
            RpcClient::with_account_sync(SolanaRpcClient::new_mock("succeeds".into()), config)
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
