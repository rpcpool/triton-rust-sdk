use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
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
    config: Option<AccountSyncConfig>,
    inner: Arc<SolanaRpcClient>,
    lanes: Mutex<HashMap<CommitmentConfig, LaneHandle>>,
    pinned: Mutex<HashSet<Pubkey>>,
    cancel: CancellationToken,
    closed: AtomicBool,
}

pub(crate) struct PreparedRead {
    pub generations: Vec<u64>,
    pub session: u64,
    pub buffered: oneshot::Receiver<Vec<CachedAccount>>,
    pub commands: mpsc::Sender<Command>,
}

impl AccountSyncRuntime {
    pub fn new(config: Option<AccountSyncConfig>, inner: Arc<SolanaRpcClient>) -> Self {
        let pinned = config
            .as_ref()
            .map(|config| config.pinned_accounts.clone())
            .unwrap_or_default();
        Self {
            config,
            inner,
            lanes: Mutex::new(HashMap::new()),
            pinned: Mutex::new(pinned),
            cancel: CancellationToken::new(),
            closed: AtomicBool::new(false),
        }
    }

    pub fn config(&self) -> Option<&AccountSyncConfig> {
        self.config.as_ref()
    }

    pub async fn prepare(
        &self,
        commitment: CommitmentConfig,
        keys: Vec<Pubkey>,
    ) -> Result<Option<PreparedRead>, AccountSyncError> {
        let Some(commands) = self.lane(commitment).await? else {
            return Ok(None);
        };
        let (generation_tx, generation_rx) = oneshot::channel();
        let (result_tx, result_rx) = oneshot::channel();
        tokio::select! {
            _ = self.cancel.cancelled() => return Err(AccountSyncError::Closed),
            result = commands.send(Command::Read { keys, generations: generation_tx, result: result_tx }) => result.map_err(|_| AccountSyncError::ChannelClosed)?,
        }
        let (generations, session) = generation_rx
            .await
            .map_err(|_| AccountSyncError::ChannelClosed)?;
        Ok(Some(PreparedRead {
            generations,
            session,
            buffered: result_rx,
            commands,
        }))
    }

    async fn lane(
        &self,
        commitment: CommitmentConfig,
    ) -> Result<Option<mpsc::Sender<Command>>, AccountSyncError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(AccountSyncError::Closed);
        }
        let Some(config) = &self.config else {
            return Ok(None);
        };
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(AccountSyncError::NoRuntime);
        }
        let mut lanes = self.lanes.lock().await;
        if let Some(lane) = lanes.get(&commitment) {
            return Ok(Some(lane.commands.clone()));
        }
        let mut config = config.clone();
        config.pinned_accounts = self.pinned.lock().await.clone();
        let (commands, task) = lane::spawn(
            config,
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
        Ok(Some(commands))
    }

    pub async fn add_pinned(
        &self,
        keys: impl IntoIterator<Item = Pubkey>,
    ) -> Result<(), AccountSyncError> {
        self.change_pinned(|pinned| pinned.extend(keys)).await
    }

    pub async fn remove_pinned(
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

    pub async fn replace_pinned(
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
        if self.closed.load(Ordering::Acquire) {
            return Err(AccountSyncError::Closed);
        }
        if self.config.is_none() {
            return Err(AccountSyncError::NotConfigured);
        }
        let mut pinned = self.pinned.lock().await;
        change(&mut pinned);
        let keys = pinned.clone();
        drop(pinned);
        let senders: Vec<_> = self
            .lanes
            .lock()
            .await
            .values()
            .map(|lane| lane.commands.clone())
            .collect();
        for sender in senders {
            tokio::select! {
                _ = self.cancel.cancelled() => return Err(AccountSyncError::Closed),
                result = sender.send(Command::Pinned(keys.clone())) => result.map_err(|_| AccountSyncError::ChannelClosed)?,
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
        let timeout = self
            .config
            .as_ref()
            .map(|config| config.close_timeout)
            .unwrap_or_default();
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
        let runtime = AccountSyncRuntime::new(Some(config), inner);
        assert!(runtime.close().await.is_ok());
        assert!(runtime.close().await.is_ok());
        assert!(matches!(
            runtime.add_pinned([Pubkey::new_unique()]).await,
            Err(AccountSyncError::Closed)
        ));
    }
}
