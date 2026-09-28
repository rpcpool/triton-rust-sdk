use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use solana_account::Account;
use solana_client::nonblocking::rpc_client::RpcClient as SolanaRpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_pubkey::Pubkey;
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Instant, interval},
};
use tokio_util::sync::CancellationToken;

use super::{
    cache::{Cache, CachedAccount},
    transport::{self, StreamAccount},
};
use crate::config::AccountSyncConfig;

pub(crate) enum Command {
    Read {
        keys: Vec<Pubkey>,
        ready: oneshot::Sender<()>,
        result: oneshot::Sender<Vec<CachedAccount>>,
    },
    Rpc {
        key: Pubkey,
        account: Option<Account>,
        slot: u64,
    },
    Snapshot {
        key: Pubkey,
        entry: Option<CachedAccount>,
    },
    Session(u64),
    Stream {
        session: u64,
        account: StreamAccount,
    },
    Pinned(HashSet<Pubkey>),
}

struct Waiter {
    keys: Vec<Pubkey>,
    result: oneshot::Sender<Vec<CachedAccount>>,
}

struct Lane {
    config: AccountSyncConfig,
    commitment: CommitmentConfig,
    inner: Arc<SolanaRpcClient>,
    commands: mpsc::Sender<Command>,
    cache: Cache,
    pinned: HashSet<Pubkey>,
    dynamic: HashMap<Pubkey, Instant>,
    snapshots: HashSet<Pubkey>,
    waiters: Vec<Waiter>,
    desired: watch::Sender<HashSet<Pubkey>>,
    session: u64,
    cancel: CancellationToken,
}

pub(super) fn spawn(
    config: AccountSyncConfig,
    commitment: CommitmentConfig,
    inner: Arc<SolanaRpcClient>,
    cancel: CancellationToken,
) -> (mpsc::Sender<Command>, JoinHandle<()>) {
    let (commands, receiver) = mpsc::channel(256);
    let (desired, desired_rx) = watch::channel(HashSet::new());
    let lane = Lane {
        pinned: config.pinned_accounts.clone(),
        config: config.clone(),
        commitment,
        inner,
        commands: commands.clone(),
        cache: Cache::default(),
        dynamic: HashMap::new(),
        snapshots: HashSet::new(),
        waiters: Vec::new(),
        desired,
        session: 0,
        cancel: cancel.clone(),
    };
    let transport_cancel = cancel.child_token();
    let transport_commands = commands.clone();
    let transport_task = tokio::spawn(transport::run(
        config,
        commitment,
        desired_rx,
        transport_commands,
        transport_cancel,
    ));
    let task = tokio::spawn(async move {
        lane.run(receiver, cancel).await;
        transport_task.abort();
        let _ = transport_task.await;
    });
    (commands, task)
}

impl Lane {
    async fn run(mut self, mut receiver: mpsc::Receiver<Command>, cancel: CancellationToken) {
        self.refresh_desired();
        let mut refresh = interval(self.config.subscription_refresh);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = refresh.tick() => self.expire_dynamic(),
                command = receiver.recv() => match command {
                    Some(command) => self.handle(command),
                    None => break,
                },
            }
        }
    }

    fn handle(&mut self, command: Command) {
        match command {
            Command::Read {
                keys,
                ready,
                result,
            } => self.read(keys, ready, result),
            Command::Rpc { key, account, slot } => self.write_rpc(key, account, slot),
            Command::Snapshot { key, entry } => {
                self.snapshots.remove(&key);
                if let Some(entry) = entry {
                    self.write(key, entry);
                }
            }
            Command::Session(session) => self.set_session(session),
            Command::Stream { session, account } if session != 0 && session == self.session => {
                self.stream(account);
            }
            Command::Stream { .. } => {}
            Command::Pinned(keys) => self.set_pinned(keys),
        }
    }

    fn read(
        &mut self,
        keys: Vec<Pubkey>,
        ready: oneshot::Sender<()>,
        result: oneshot::Sender<Vec<CachedAccount>>,
    ) {
        let mut changed = false;
        for key in &keys {
            if self.config.automatic_subscriptions && !self.pinned.contains(key) {
                let expiry = Instant::now() + self.config.dynamic_subscription_lifetime;
                changed |= self.dynamic.insert(*key, expiry).is_none();
            }
        }
        if changed {
            self.refresh_desired();
        }
        if self.session != 0 {
            for key in &keys {
                if self.wanted(key) {
                    self.start_snapshot(*key);
                }
            }
        }
        if let Some(accounts) = self.complete(&keys) {
            let _ = result.send(accounts);
        } else {
            self.waiters.push(Waiter { keys, result });
        }
        let _ = ready.send(());
    }

    fn start_snapshot(&mut self, key: Pubkey) {
        if self.session == 0 {
            return;
        }
        if self.cache.get(&key).is_some() || !self.snapshots.insert(key) {
            return;
        }
        let inner = self.inner.clone();
        let commands = self.commands.clone();
        let commitment = self.commitment;
        let cancel = self.cancel.clone();
        tokio::spawn(async move {
            let response = tokio::select! {
                _ = cancel.cancelled() => return,
                response = inner.get_account_with_commitment(&key, commitment) => response,
            };
            let entry = response.ok().map(|response| CachedAccount {
                account: response.value,
                slot: response.context.slot,
                version: 0,
                stream: false,
            });
            let _ = commands.send(Command::Snapshot { key, entry }).await;
        });
    }

    fn stream(&mut self, update: StreamAccount) {
        let key = update.key;
        let entry = CachedAccount {
            account: Some(update.account),
            slot: update.slot,
            version: update.version,
            stream: true,
        };
        self.write(key, entry);
    }

    fn write_rpc(&mut self, key: Pubkey, account: Option<Account>, slot: u64) {
        let entry = CachedAccount {
            account,
            slot,
            version: 0,
            stream: false,
        };
        self.write(key, entry);
    }

    fn write(&mut self, key: Pubkey, entry: CachedAccount) {
        if !self.wanted(&key) || !self.cache.write(key, entry) {
            return;
        }
        let waiters = std::mem::take(&mut self.waiters);
        for waiter in waiters {
            if waiter.result.is_closed() {
                continue;
            }
            if let Some(values) = self.complete(&waiter.keys) {
                let _ = waiter.result.send(values);
            } else {
                self.waiters.push(waiter);
            }
        }
    }

    fn complete(&self, keys: &[Pubkey]) -> Option<Vec<CachedAccount>> {
        keys.iter()
            .map(|key| self.cache.get(key).cloned())
            .collect()
    }

    fn wanted(&self, key: &Pubkey) -> bool {
        self.pinned.contains(key)
            || self
                .dynamic
                .get(key)
                .is_some_and(|expiry| *expiry > Instant::now())
    }

    fn set_pinned(&mut self, keys: HashSet<Pubkey>) {
        self.pinned = keys;
        self.discard_unwanted();
        self.refresh_desired();
    }

    fn expire_dynamic(&mut self) {
        let now = Instant::now();
        self.waiters.retain(|waiter| !waiter.result.is_closed());
        self.dynamic.retain(|_, expiry| *expiry > now);
        self.discard_unwanted();
        let wanted: Vec<_> = self
            .pinned
            .iter()
            .chain(self.dynamic.keys())
            .copied()
            .collect();
        if self.session != 0 {
            for key in wanted {
                self.start_snapshot(key);
            }
        }
        self.refresh_desired();
    }

    fn discard_unwanted(&mut self) {
        let stale: Vec<_> = self
            .cache
            .keys()
            .filter(|key| !self.wanted(key))
            .copied()
            .collect();
        for key in stale {
            self.cache.remove(&key);
        }
    }

    fn refresh_desired(&mut self) {
        let keys: HashSet<_> = self
            .pinned
            .iter()
            .chain(self.dynamic.keys())
            .copied()
            .collect();
        if *self.desired.borrow() != keys {
            self.session = 0;
            self.cache.clear_entries();
            self.desired.send_replace(keys);
        }
    }

    fn set_session(&mut self, session: u64) {
        if session == self.session {
            return;
        }
        self.session = session;
        self.cache.clear_entries();
        if session == 0 {
            return;
        }
        let wanted: Vec<_> = self
            .pinned
            .iter()
            .chain(self.dynamic.keys())
            .copied()
            .collect();
        for key in wanted {
            self.start_snapshot(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lane() -> Lane {
        let config = AccountSyncConfig {
            endpoint: "http://localhost:10000".into(),
            ..Default::default()
        };
        let (commands, _receiver) = mpsc::channel(256);
        let (desired, _watcher) = watch::channel(HashSet::new());
        Lane {
            config,
            commitment: CommitmentConfig::finalized(),
            inner: Arc::new(SolanaRpcClient::new_mock("succeeds".into())),
            commands,
            cache: Cache::default(),
            pinned: HashSet::new(),
            dynamic: HashMap::new(),
            snapshots: HashSet::new(),
            waiters: Vec::new(),
            desired,
            session: 0,
            cancel: CancellationToken::new(),
        }
    }

    #[tokio::test]
    async fn duplicates_renew_one_dynamic_subscription() {
        let mut lane = lane();
        let key = Pubkey::new_unique();
        let (ready_tx, ready_rx) = oneshot::channel();
        let (result_tx, _result_rx) = oneshot::channel();
        lane.read(vec![key, key], ready_tx, result_tx);
        ready_rx.await.unwrap();
        assert_eq!(lane.desired.borrow().len(), 1);
        lane.write_rpc(key, None, 1);
        assert!(lane.cache.get(&key).is_some());
        lane.dynamic
            .insert(key, Instant::now() - std::time::Duration::from_secs(1));
        lane.expire_dynamic();
        assert!(lane.desired.borrow().is_empty());
        assert!(lane.cache.get(&key).is_none());
        lane.write_rpc(key, None, 2);
        assert!(lane.cache.get(&key).is_none());
    }

    #[tokio::test]
    async fn pinned_account_survives_dynamic_expiry_and_removal_drops_cache() {
        let mut lane = lane();
        let key = Pubkey::new_unique();
        lane.set_pinned(HashSet::from([key]));
        lane.write_rpc(key, None, 1);
        lane.dynamic
            .insert(key, Instant::now() - std::time::Duration::from_secs(1));
        lane.expire_dynamic();
        assert!(lane.desired.borrow().contains(&key));
        assert!(lane.cache.get(&key).is_some());
        lane.set_pinned(HashSet::new());
        assert!(lane.desired.borrow().is_empty());
        assert!(lane.cache.get(&key).is_none());
        lane.write_rpc(key, None, 2);
        assert!(lane.cache.get(&key).is_none());
        lane.set_pinned(HashSet::from([key]));
        lane.write_rpc(key, None, 2);
        assert_eq!(lane.cache.get(&key).unwrap().slot, 2);
    }

    #[tokio::test]
    async fn stream_is_available_before_snapshot_and_rejects_old_session() {
        let mut lane = lane();
        let key = Pubkey::new_unique();
        lane.set_pinned(HashSet::from([key]));
        lane.set_session(1);
        let account = Account {
            lamports: 10,
            ..Default::default()
        };
        lane.handle(Command::Stream {
            session: 1,
            account: StreamAccount {
                key,
                account: account.clone(),
                slot: 12,
                version: 2,
            },
        });
        assert_eq!(lane.cache.get(&key).unwrap().slot, 12);
        assert!(lane.snapshots.contains(&key));
        lane.handle(Command::Snapshot {
            key,
            entry: Some(CachedAccount {
                account: None,
                slot: 11,
                version: 0,
                stream: false,
            }),
        });
        assert!(!lane.snapshots.contains(&key));
        assert_eq!(lane.cache.get(&key).map(|entry| entry.slot), Some(12));
        assert_eq!(
            lane.cache
                .get(&key)
                .and_then(|entry| entry.account.as_ref())
                .map(|account| account.lamports),
            Some(10)
        );

        lane.set_pinned(HashSet::new());
        lane.set_pinned(HashSet::from([key]));
        lane.set_session(2);
        lane.handle(Command::Stream {
            session: 1,
            account: StreamAccount {
                key,
                account,
                slot: 20,
                version: 1,
            },
        });
        assert!(lane.cache.get(&key).is_none());
        assert!(lane.snapshots.contains(&key));
    }

    #[tokio::test]
    async fn rpc_observation_is_available_before_snapshot() {
        let mut lane = lane();
        let key = Pubkey::new_unique();
        lane.set_pinned(HashSet::from([key]));
        lane.set_session(1);
        lane.handle(Command::Rpc {
            key,
            account: Some(Account {
                lamports: 11,
                ..Default::default()
            }),
            slot: 12,
        });
        assert_eq!(lane.cache.get(&key).unwrap().slot, 12);
        lane.handle(Command::Snapshot {
            key,
            entry: Some(CachedAccount {
                account: None,
                slot: 11,
                version: 0,
                stream: false,
            }),
        });
        assert_eq!(
            lane.cache
                .get(&key)
                .and_then(|entry| entry.account.as_ref())
                .map(|account| account.lamports),
            Some(11)
        );
    }

    #[tokio::test]
    async fn snapshots_retry_after_failure_without_duplicate_calls() {
        let mut lane = lane();
        let (commands, mut receiver) = mpsc::channel(256);
        lane.commands = commands;
        lane.inner = Arc::new(SolanaRpcClient::new_mock("fails".into()));
        let key = Pubkey::new_unique();
        lane.set_pinned(HashSet::from([key]));
        lane.set_session(1);
        lane.expire_dynamic();
        lane.start_snapshot(key);
        let failed = receiver.recv().await.unwrap();
        assert!(matches!(&failed, Command::Snapshot { entry: None, .. }));
        assert!(receiver.try_recv().is_err());
        assert!(lane.snapshots.contains(&key));
        lane.handle(failed);
        assert!(!lane.snapshots.contains(&key));
        lane.inner = Arc::new(SolanaRpcClient::new_mock("succeeds".into()));
        lane.expire_dynamic();
        assert!(lane.snapshots.contains(&key));
        lane.handle(receiver.recv().await.unwrap());
        assert!(!lane.snapshots.contains(&key));
        assert!(lane.cache.get(&key).unwrap().account.is_none());
    }

    #[tokio::test]
    async fn multiple_read_waits_for_every_key_and_preserves_duplicates() {
        let mut lane = lane();
        let first = Pubkey::new_unique();
        let second = Pubkey::new_unique();
        lane.set_pinned(HashSet::from([first, second]));
        let account = Account {
            lamports: 42,
            ..Default::default()
        };
        lane.write_rpc(first, Some(account.clone()), 12);
        let (ready, _) = oneshot::channel();
        let (result, mut response) = oneshot::channel();
        lane.read(vec![first, second, first], ready, result);
        assert!(matches!(
            response.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        lane.write_rpc(second, None, 7);
        let values = response.await.unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0].account, Some(account.clone()));
        assert!(values[1].account.is_none());
        assert_eq!(values[2].account, Some(account));
        let (ready, _) = oneshot::channel();
        let (result, mut response) = oneshot::channel();
        lane.read(vec![second], ready, result);
        assert!(response.try_recv().unwrap()[0].account.is_none());
    }

    #[tokio::test]
    async fn removing_pinned_keeps_active_temporary_subscription() {
        let mut lane = lane();
        let key = Pubkey::new_unique();
        lane.set_pinned(HashSet::from([key]));
        lane.dynamic
            .insert(key, Instant::now() + std::time::Duration::from_secs(60));
        lane.write_rpc(key, None, 1);
        lane.set_pinned(HashSet::new());
        assert!(lane.cache.get(&key).is_some());
        assert!(lane.desired.borrow().contains(&key));
    }

    #[test]
    fn expired_or_unsubscribed_accounts_reject_all_writes() {
        let mut lane = lane();
        let key = Pubkey::new_unique();
        lane.dynamic
            .insert(key, Instant::now() - std::time::Duration::from_secs(1));
        lane.write_rpc(key, None, 1);
        lane.stream(StreamAccount {
            key,
            account: Account::default(),
            slot: 2,
            version: 1,
        });
        lane.snapshots.insert(key);
        lane.handle(Command::Snapshot {
            key,
            entry: Some(CachedAccount {
                account: None,
                slot: 3,
                version: 0,
                stream: false,
            }),
        });
        assert!(lane.cache.get(&key).is_none());
        assert!(!lane.snapshots.contains(&key));
    }
}
