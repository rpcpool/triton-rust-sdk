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
        generations: oneshot::Sender<(Vec<u64>, u64)>,
        result: oneshot::Sender<Vec<CachedAccount>>,
    },
    Rpc {
        key: Pubkey,
        generation: u64,
        session: u64,
        account: Option<Account>,
        slot: u64,
    },
    Snapshot {
        key: Pubkey,
        generation: u64,
        session: u64,
        account: Option<Account>,
        slot: u64,
    },
    SnapshotFailed {
        key: Pubkey,
        generation: u64,
        session: u64,
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
    generations: Vec<u64>,
    result: oneshot::Sender<Vec<CachedAccount>>,
}

struct PendingSnapshot {
    generation: u64,
    session: u64,
    rpc: Option<CachedAccount>,
    staged: Option<CachedAccount>,
}

struct Lane {
    config: AccountSyncConfig,
    commitment: CommitmentConfig,
    inner: Arc<SolanaRpcClient>,
    commands: mpsc::Sender<Command>,
    cache: Cache,
    pinned: HashSet<Pubkey>,
    dynamic: HashMap<Pubkey, Instant>,
    pending: HashMap<Pubkey, PendingSnapshot>,
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
        pending: HashMap::new(),
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
                generations,
                result,
            } => self.read(keys, generations, result),
            Command::Rpc {
                key,
                generation,
                session,
                account,
                slot,
            } => {
                if session != 0 && session == self.session {
                    self.write_rpc(key, generation, account, slot);
                }
            }
            Command::Snapshot {
                key,
                generation,
                session,
                account,
                slot,
            } => self.snapshot(key, generation, session, account, slot),
            Command::SnapshotFailed {
                key,
                generation,
                session,
            } => self.snapshot_failed(key, generation, session),
            Command::Session(session) => self.set_session(session),
            Command::Stream { session, account } if session == self.session => self.stream(account),
            Command::Stream { .. } => {}
            Command::Pinned(keys) => self.set_pinned(keys),
        }
    }

    fn read(
        &mut self,
        keys: Vec<Pubkey>,
        generations: oneshot::Sender<(Vec<u64>, u64)>,
        result: oneshot::Sender<Vec<CachedAccount>>,
    ) {
        let mut ids = Vec::with_capacity(keys.len());
        let mut changed = false;
        for key in &keys {
            let generation = self.cache.generation(*key);
            ids.push(generation);
            if self.config.automatic_subscriptions && !self.pinned.contains(key) {
                let expiry = Instant::now() + self.config.dynamic_subscription_lifetime;
                changed |= self.dynamic.insert(*key, expiry).is_none();
            }
        }
        if changed {
            self.refresh_desired();
        }
        if self.session != 0 {
            for (key, generation) in keys.iter().zip(&ids) {
                if self.pinned.contains(key) || self.dynamic.contains_key(key) {
                    self.start_snapshot(*key, *generation);
                }
            }
        }
        if let Some(accounts) = self.complete(&keys, &ids) {
            let _ = result.send(accounts);
        } else {
            self.waiters.push(Waiter {
                keys,
                generations: ids.clone(),
                result,
            });
        }
        let _ = generations.send((ids, self.session));
    }

    fn start_snapshot(&mut self, key: Pubkey, generation: u64) {
        if self.session == 0 {
            return;
        }
        if self.cache.get(&key).is_some() || self.pending.contains_key(&key) {
            return;
        }
        self.pending.insert(
            key,
            PendingSnapshot {
                generation,
                session: self.session,
                rpc: None,
                staged: None,
            },
        );
        let inner = self.inner.clone();
        let commands = self.commands.clone();
        let commitment = self.commitment;
        let session = self.session;
        let cancel = self.cancel.clone();
        tokio::spawn(async move {
            let response = tokio::select! {
                _ = cancel.cancelled() => return,
                response = inner.get_account_with_commitment(&key, commitment) => response,
            };
            let command = match response {
                Ok(response) => Command::Snapshot {
                    key,
                    generation,
                    session,
                    account: response.value,
                    slot: response.context.slot,
                },
                Err(_) => Command::SnapshotFailed {
                    key,
                    generation,
                    session,
                },
            };
            let _ = commands.send(command).await;
        });
    }

    fn snapshot(
        &mut self,
        key: Pubkey,
        generation: u64,
        session: u64,
        account: Option<Account>,
        slot: u64,
    ) {
        if self.session != session
            || !self.pending.get(&key).is_some_and(|pending| {
                pending.generation == generation && pending.session == session
            })
        {
            return;
        }
        let Some(pending) = self.pending.remove(&key) else {
            return;
        };
        self.write_rpc(key, generation, account, slot);
        if let Some(rpc) = pending.rpc {
            self.write(key, rpc);
        }
        if let Some(staged) = pending.staged {
            self.write(key, staged);
        }
    }

    fn snapshot_failed(&mut self, key: Pubkey, generation: u64, session: u64) {
        if self.session != session
            || !self.pending.get(&key).is_some_and(|pending| {
                pending.generation == generation && pending.session == session
            })
        {
            return;
        }
        let Some(pending) = self.pending.remove(&key) else {
            return;
        };
        if let Some(rpc) = pending.rpc {
            self.write(key, rpc);
        }
        if let Some(staged) = pending.staged {
            self.write(key, staged);
        }
    }

    fn stream(&mut self, update: StreamAccount) {
        let key = update.key;
        if !self.pinned.contains(&key) && !self.dynamic.contains_key(&key) {
            return;
        }
        let generation = self.cache.generation(key);
        let entry = CachedAccount {
            account: Some(update.account),
            slot: update.slot,
            version: update.version,
            generation,
            stream: true,
        };
        if let Some(pending) = self.pending.get_mut(&key)
            && pending.generation == generation
        {
            if pending
                .staged
                .as_ref()
                .is_none_or(|old| (entry.slot, entry.version) > (old.slot, old.version))
            {
                pending.staged = Some(entry);
            }
            return;
        }
        self.write(key, entry);
    }

    fn write_rpc(&mut self, key: Pubkey, generation: u64, account: Option<Account>, slot: u64) {
        if !self.pinned.contains(&key) && !self.dynamic.contains_key(&key) {
            return;
        }
        let entry = CachedAccount {
            account,
            slot,
            version: 0,
            generation,
            stream: false,
        };
        if let Some(pending) = self.pending.get_mut(&key)
            && pending.generation == generation
            && pending.session == self.session
        {
            if pending.rpc.as_ref().is_none_or(|old| entry.slot > old.slot) {
                pending.rpc = Some(entry);
            }
            return;
        }
        self.write(key, entry);
    }

    fn write(&mut self, key: Pubkey, entry: CachedAccount) {
        if !self.cache.write(key, entry) {
            return;
        }
        let waiters = std::mem::take(&mut self.waiters);
        for waiter in waiters {
            if waiter.result.is_closed() {
                continue;
            }
            if let Some(values) = self.complete(&waiter.keys, &waiter.generations) {
                let _ = waiter.result.send(values);
            } else {
                self.waiters.push(waiter);
            }
        }
    }

    fn complete(&self, keys: &[Pubkey], generations: &[u64]) -> Option<Vec<CachedAccount>> {
        keys.iter()
            .zip(generations)
            .map(|(key, generation)| {
                self.cache
                    .get(key)
                    .filter(|entry| entry.generation == *generation)
                    .cloned()
            })
            .collect()
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
                let generation = self.cache.generation(key);
                self.start_snapshot(key, generation);
            }
        }
        self.refresh_desired();
    }

    fn discard_unwanted(&mut self) {
        let stale: Vec<_> = self
            .cache
            .keys()
            .filter(|key| !self.pinned.contains(key) && !self.dynamic.contains_key(key))
            .copied()
            .collect();
        for key in stale {
            self.cache.remove(&key);
            self.pending.remove(&key);
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
            self.pending.clear();
            self.desired.send_replace(keys);
        }
    }

    fn set_session(&mut self, session: u64) {
        if session == self.session {
            return;
        }
        self.session = session;
        self.cache.clear_entries();
        self.pending.clear();
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
            let generation = self.cache.generation(key);
            self.start_snapshot(key, generation);
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
            pending: HashMap::new(),
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
        let (gen_tx, gen_rx) = oneshot::channel();
        let (result_tx, _result_rx) = oneshot::channel();
        lane.read(vec![key, key], gen_tx, result_tx);
        let (generations, _) = gen_rx.await.unwrap();
        assert_eq!(generations, vec![generations[0]; 2]);
        assert_eq!(lane.desired.borrow().len(), 1);
        lane.dynamic
            .insert(key, Instant::now() - std::time::Duration::from_secs(1));
        lane.expire_dynamic();
        assert!(lane.desired.borrow().is_empty());
        assert_ne!(lane.cache.generation(key), generations[0]);
    }

    #[tokio::test]
    async fn pinned_account_survives_dynamic_expiry_and_readd_gets_new_generation() {
        let mut lane = lane();
        let key = Pubkey::new_unique();
        lane.set_pinned(HashSet::from([key]));
        let first = lane.cache.generation(key);
        lane.dynamic
            .insert(key, Instant::now() - std::time::Duration::from_secs(1));
        lane.expire_dynamic();
        assert!(lane.desired.borrow().contains(&key));
        lane.set_pinned(HashSet::new());
        assert!(lane.desired.borrow().is_empty());
        lane.set_pinned(HashSet::from([key]));
        assert_ne!(lane.cache.generation(key), first);
    }

    #[tokio::test]
    async fn stages_stream_update_after_snapshot_and_rejects_old_session() {
        let mut lane = lane();
        let key = Pubkey::new_unique();
        lane.set_pinned(HashSet::from([key]));
        let first = lane.cache.generation(key);
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
        assert!(lane.cache.get(&key).is_none());
        lane.snapshot(
            key,
            first,
            1,
            Some(Account {
                lamports: 5,
                ..Default::default()
            }),
            11,
        );
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
        let second = lane.cache.generation(key);
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
        lane.snapshot(key, first, 1, None, 21);
        assert_ne!(first, second);
        assert!(lane.cache.get(&key).is_none());
        assert_eq!(
            lane.pending.get(&key).map(|pending| pending.generation),
            Some(second)
        );
    }

    #[tokio::test]
    async fn rpc_observation_waits_for_snapshot() {
        let mut lane = lane();
        let key = Pubkey::new_unique();
        lane.set_pinned(HashSet::from([key]));
        let generation = lane.cache.generation(key);
        lane.set_session(1);
        lane.handle(Command::Rpc {
            key,
            generation,
            session: 1,
            account: Some(Account {
                lamports: 11,
                ..Default::default()
            }),
            slot: 12,
        });
        assert!(lane.cache.get(&key).is_none());
        lane.snapshot(
            key,
            generation,
            1,
            Some(Account {
                lamports: 5,
                ..Default::default()
            }),
            11,
        );
        assert_eq!(
            lane.cache
                .get(&key)
                .and_then(|entry| entry.account.as_ref())
                .map(|account| account.lamports),
            Some(11)
        );
    }
}
