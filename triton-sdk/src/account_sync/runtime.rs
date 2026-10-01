use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex as ConfigMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use solana_client::nonblocking::rpc_client::RpcClient as SolanaRpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_pubkey::Pubkey;
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use super::{
    cache::CachedAccount,
    lane::{self, Command},
};
use crate::{config::AccountSyncConfig, error::AccountSyncError};

struct LaneHandle {
    commands: mpsc::Sender<Command>,
    task: JoinHandle<()>,
}

pub(crate) struct AccountSyncRuntime {
    config: ConfigMutex<AccountSyncConfig>,
    inner: Arc<SolanaRpcClient>,
    lanes: Mutex<HashMap<CommitmentConfig, LaneHandle>>,
    cancel: CancellationToken,
    closed: AtomicBool,
}

pub(crate) struct PreparedRead {
    pub buffered: oneshot::Receiver<Vec<CachedAccount>>,
    pub commands: mpsc::Sender<Command>,
}

impl AccountSyncRuntime {
    pub fn new(config: AccountSyncConfig, inner: Arc<SolanaRpcClient>) -> Self {
        Self {
            config: ConfigMutex::new(config),
            inner,
            lanes: Mutex::new(HashMap::new()),
            cancel: CancellationToken::new(),
            closed: AtomicBool::new(false),
        }
    }

    pub fn config(&self) -> AccountSyncConfig {
        self.config
            .lock()
            .expect("account-sync config poisoned")
            .clone()
    }

    pub fn cache_miss_wait(&self) -> Duration {
        self.config
            .lock()
            .expect("account-sync config poisoned")
            .cache_miss_wait
    }

    pub async fn prepare(
        &self,
        commitment: CommitmentConfig,
        keys: Vec<Pubkey>,
    ) -> Result<PreparedRead, AccountSyncError> {
        let commands = self.lane(commitment).await?;
        let (ready_tx, ready_rx) = oneshot::channel();
        let (result_tx, result_rx) = oneshot::channel();
        tokio::select! {
            _ = self.cancel.cancelled() => return Err(AccountSyncError::Closed),
            result = commands.send(Command::Read { keys, ready: ready_tx, result: result_tx }) => result.map_err(|_| AccountSyncError::ChannelClosed)?,
        }
        ready_rx
            .await
            .map_err(|_| AccountSyncError::ChannelClosed)?;
        Ok(PreparedRead {
            buffered: result_rx,
            commands,
        })
    }

    async fn lane(
        &self,
        commitment: CommitmentConfig,
    ) -> Result<mpsc::Sender<Command>, AccountSyncError> {
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(AccountSyncError::NoRuntime);
        }
        let mut lanes = self.lanes.lock().await;
        if self.closed.load(Ordering::Acquire) {
            return Err(AccountSyncError::Closed);
        }
        if let Some(lane) = lanes.get(&commitment) {
            return Ok(lane.commands.clone());
        }
        let (commands, task) = lane::spawn(
            self.config(),
            commitment,
            self.inner.clone(),
            self.cancel.child_token(),
        );
        lanes.insert(
            commitment,
            LaneHandle {
                commands: commands.clone(),
                task,
            },
        );
        Ok(commands)
    }

    pub async fn add_accounts(
        &self,
        keys: impl IntoIterator<Item = Pubkey>,
    ) -> Result<(), AccountSyncError> {
        self.change_pinned(|pinned| pinned.extend(keys)).await
    }

    pub async fn remove_accounts(
        &self,
        keys: impl IntoIterator<Item = Pubkey>,
    ) -> Result<(), AccountSyncError> {
        self.change_pinned(|pinned| {
            for key in keys {
                pinned.remove(&key);
            }
        })
        .await
    }

    pub async fn replace_accounts(
        &self,
        keys: impl IntoIterator<Item = Pubkey>,
    ) -> Result<(), AccountSyncError> {
        self.change_pinned(|pinned| *pinned = keys.into_iter().collect())
            .await
    }

    async fn change_pinned(
        &self,
        change: impl FnOnce(&mut HashSet<Pubkey>),
    ) -> Result<(), AccountSyncError> {
        let lanes = self.lanes.lock().await;
        if self.closed.load(Ordering::Acquire) {
            return Err(AccountSyncError::Closed);
        }
        let keys = {
            let mut config = self.config.lock().expect("account-sync config poisoned");
            change(&mut config.pinned_accounts);
            config.pinned_accounts.clone()
        };
        for lane in lanes.values() {
            tokio::select! {
                _ = self.cancel.cancelled() => return Err(AccountSyncError::Closed),
                result = lane.commands.send(Command::Pinned(keys.clone())) => result.map_err(|_| AccountSyncError::ChannelClosed)?,
            }
        }
        Ok(())
    }

    pub async fn close(&self) -> Result<(), AccountSyncError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.cancel.cancel();
        let tasks: Vec<_> = self
            .lanes
            .lock()
            .await
            .drain()
            .map(|(_, lane)| lane.task)
            .collect();
        let timeout = self.config().close_timeout;
        tokio::time::timeout(timeout, async {
            for task in tasks {
                task.await?;
            }
            Ok(())
        })
        .await
        .map_err(|_| AccountSyncError::CloseTimeout)?
    }
}

impl Drop for AccountSyncRuntime {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn close_before_start_is_idempotent() {
        let config = AccountSyncConfig {
            endpoint: "http://localhost:10000".into(),
            ..Default::default()
        };
        let inner = Arc::new(SolanaRpcClient::new_mock("succeeds".into()));
        let runtime = AccountSyncRuntime::new(config, inner);
        assert!(runtime.close().await.is_ok());
        assert!(runtime.close().await.is_ok());
        assert!(matches!(
            runtime.add_accounts([Pubkey::new_unique()]).await,
            Err(AccountSyncError::Closed)
        ));
    }
}
