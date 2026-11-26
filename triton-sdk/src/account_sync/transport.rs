use std::collections::HashSet;

use solana_account::Account;
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_pubkey::Pubkey;
use tokio::{
    sync::{mpsc, watch},
    time::{Instant, MissedTickBehavior, interval_at, sleep},
};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use yellowstone_account_sync_proto::{
    account_sync::yellowstone_account_sync_grpc_service_client::YellowstoneAccountSyncGrpcServiceClient,
    geyser::{self, subscribe_update::UpdateOneof},
};

use super::{endpoint::GrpcEndpoint, lane::Command};
use crate::{config::AccountSyncConfig, error::AccountSyncError};

pub(crate) struct StreamAccount {
    pub key: Pubkey,
    pub account: Account,
    pub slot: u64,
    pub version: u64,
}

pub(super) async fn run(
    config: AccountSyncConfig,
    commitment: CommitmentConfig,
    mut desired: watch::Receiver<HashSet<Pubkey>>,
    commands: mpsc::Sender<Command>,
    cancel: CancellationToken,
) {
    let mut delay = config.reconnect_min_delay;
    let mut session = 0;
    loop {
        if cancel.is_cancelled() {
            return;
        }
        if desired.borrow().is_empty() {
            tokio::select! {
                _ = cancel.cancelled() => return,
                result = desired.changed() => if result.is_err() { return; },
            }
            continue;
        }
        session += 1;
        let started = tokio::time::Instant::now();
        let result = tokio::select! {
            _ = cancel.cancelled() => return,
            result = stream_once(&config, commitment, session, &mut desired, &commands, &cancel) => result,
        };
        if commands.send(Command::Session(0)).await.is_err() {
            return;
        }
        if started.elapsed() >= config.reconnect_max_delay {
            delay = config.reconnect_min_delay;
        }
        if result.is_ok() {
            delay = config.reconnect_min_delay;
            continue;
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = sleep(delay) => {},
        }
        delay = delay.saturating_mul(2).min(config.reconnect_max_delay);
    }
}

async fn stream_once(
    config: &AccountSyncConfig,
    commitment: CommitmentConfig,
    session: u64,
    desired: &mut watch::Receiver<HashSet<Pubkey>>,
    commands: &mpsc::Sender<Command>,
    cancel: &CancellationToken,
) -> Result<(), AccountSyncError> {
    let parsed = GrpcEndpoint::parse(&config.endpoint)?;
    let endpoint = parsed
        .endpoint
        .connect_timeout(config.connect_timeout)
        .initial_connection_window_size(config.http2_window_size)
        .initial_stream_window_size(config.http2_window_size)
        .http2_keep_alive_interval(config.keepalive_interval)
        .keep_alive_timeout(config.keepalive_timeout)
        .keep_alive_while_idle(config.keepalive_while_idle);
    let channel: Channel = tokio::select! {
        _ = cancel.cancelled() => return Ok(()),
        result = endpoint.connect() => result?,
    };
    let mut client = YellowstoneAccountSyncGrpcServiceClient::new(channel)
        .max_decoding_message_size(config.max_decoded_message_size)
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd);
    let (requests, request_rx) = mpsc::channel(8);
    let request = make_request(&desired.borrow_and_update(), commitment);
    requests
        .send(request)
        .await
        .map_err(|_| AccountSyncError::ChannelClosed)?;
    let mut request = tonic::Request::new(tokio_stream::wrappers::ReceiverStream::new(request_rx));
    if let Some(token) = parsed.token {
        request.metadata_mut().insert("x-token", token);
    }
    let response = tokio::select! {
        _ = cancel.cancelled() => return Ok(()),
        result = client.subscribe(request) => result?,
    };
    commands
        .send(Command::Session(session))
        .await
        .map_err(|_| AccountSyncError::ChannelClosed)?;
    let stream = response.into_inner();
    tokio::select! {
        _ = cancel.cancelled() => Ok(()),
        result = drive_stream(config, commitment, session, desired, commands, &requests, stream) => result,
    }
}

async fn drive_stream(
    config: &AccountSyncConfig,
    commitment: CommitmentConfig,
    session: u64,
    desired: &mut watch::Receiver<HashSet<Pubkey>>,
    commands: &mpsc::Sender<Command>,
    requests: &mpsc::Sender<geyser::SubscribeRequest>,
    mut stream: tonic::Streaming<geyser::SubscribeUpdate>,
) -> Result<(), AccountSyncError> {
    let period = config.keepalive_interval;
    let mut ping = interval_at(Instant::now() + period, period);
    ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut ping_id: i32 = 0;
    loop {
        tokio::select! {
            changed = desired.changed() => {
                if changed.is_err() || !send_desired(desired, commitment, requests).await? {
                    return Ok(());
                }
            }
            _ = ping.tick() => {
                ping_id = ping_id.wrapping_add(1);
                send(requests, make_ping(ping_id)).await?;
            }
            update = stream.message() => {
                let Some(update) = update? else { return Err(tonic::Status::unavailable("account-sync stream ended").into()); };
                if let Some(account) = decode_account(update) {
                    commands.send(Command::Stream { session, account }).await.map_err(|_| AccountSyncError::ChannelClosed)?;
                }
            }
        }
    }
}

// Sends the full key set on the open stream. Returns false when the set is empty.
async fn send_desired(
    desired: &mut watch::Receiver<HashSet<Pubkey>>,
    commitment: CommitmentConfig,
    requests: &mpsc::Sender<geyser::SubscribeRequest>,
) -> Result<bool, AccountSyncError> {
    let request = {
        let keys = desired.borrow_and_update();
        if keys.is_empty() {
            return Ok(false);
        }
        make_request(&keys, commitment)
    };
    send(requests, request).await?;
    Ok(true)
}

async fn send(
    requests: &mpsc::Sender<geyser::SubscribeRequest>,
    request: geyser::SubscribeRequest,
) -> Result<(), AccountSyncError> {
    requests
        .send(request)
        .await
        .map_err(|_| AccountSyncError::ChannelClosed)
}

fn make_ping(id: i32) -> geyser::SubscribeRequest {
    geyser::SubscribeRequest {
        ping: Some(geyser::SubscribeRequestPing { id }),
        ..Default::default()
    }
}

fn make_request(keys: &HashSet<Pubkey>, commitment: CommitmentConfig) -> geyser::SubscribeRequest {
    let mut request = geyser::SubscribeRequest::default();
    let accounts = geyser::SubscribeRequestFilterAccounts {
        account: keys.iter().map(ToString::to_string).collect(),
        ..Default::default()
    };
    request.accounts.insert("accounts".into(), accounts);
    request.commitment = Some(match commitment.commitment {
        CommitmentLevel::Processed => geyser::CommitmentLevel::Processed as i32,
        CommitmentLevel::Confirmed => geyser::CommitmentLevel::Confirmed as i32,
        CommitmentLevel::Finalized => geyser::CommitmentLevel::Finalized as i32,
    });
    request
}

fn decode_account(update: geyser::SubscribeUpdate) -> Option<StreamAccount> {
    let UpdateOneof::Account(update) = update.update_oneof? else {
        return None;
    };
    let info = update.account?;
    let key = Pubkey::try_from(info.pubkey.as_slice()).ok()?;
    let owner = Pubkey::try_from(info.owner.as_slice()).ok()?;
    Some(StreamAccount {
        key,
        account: Account {
            lamports: info.lamports,
            data: info.data,
            owner,
            executable: info.executable,
            rent_epoch: info.rent_epoch,
        },
        slot: update.slot,
        version: info.write_version,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use tokio_stream::wrappers::TcpListenerStream;
    use yellowstone_account_sync_proto::account_sync::yellowstone_account_sync_grpc_service_server::{
        YellowstoneAccountSyncGrpcService, YellowstoneAccountSyncGrpcServiceServer,
    };

    use super::*;

    struct TestService {
        received: mpsc::Sender<(tonic::metadata::MetadataMap, geyser::SubscribeRequest)>,
    }

    #[tonic::async_trait]
    impl YellowstoneAccountSyncGrpcService for TestService {
        type SubscribeStream = tokio_stream::Empty<Result<geyser::SubscribeUpdate, tonic::Status>>;

        async fn subscribe(
            &self,
            request: tonic::Request<tonic::Streaming<geyser::SubscribeRequest>>,
        ) -> Result<tonic::Response<Self::SubscribeStream>, tonic::Status> {
            let metadata = request.metadata().clone();
            let first = request.into_inner().message().await?.unwrap();
            self.received.send((metadata, first)).await.unwrap();
            // An empty response stream forces the client to reconnect.
            Ok(tonic::Response::new(tokio_stream::empty()))
        }
    }

    async fn check_subscription_metadata(path: &str, token: Option<&str>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (received_tx, mut received) = mpsc::channel(8);
        let cancel = CancellationToken::new();
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(YellowstoneAccountSyncGrpcServiceServer::new(TestService {
                    received: received_tx,
                }))
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    cancel.clone().cancelled_owned(),
                ),
        );
        let config = AccountSyncConfig {
            endpoint: format!("{address}{path}"),
            reconnect_min_delay: Duration::from_millis(1),
            ..Default::default()
        };
        config.validate().unwrap();
        let key = Pubkey::new_from_array([1; 32]);
        let (_desired_tx, desired) = watch::channel(HashSet::from([key]));
        let (commands, mut command_rx) = mpsc::channel(8);
        let client = tokio::spawn(run(
            config,
            CommitmentConfig::confirmed(),
            desired,
            commands,
            cancel.clone(),
        ));
        let outcome = tokio::time::timeout(Duration::from_secs(5), async {
            for session in 1..=2 {
                let (metadata, request) = received.recv().await.unwrap();
                assert_eq!(
                    metadata.get("x-token").map(|value| value.to_str().unwrap()),
                    token
                );
                assert_eq!(request.accounts["accounts"].account, [key.to_string()]);
                assert_eq!(
                    request.commitment,
                    Some(geyser::CommitmentLevel::Confirmed as i32)
                );
                assert!(
                    matches!(command_rx.recv().await, Some(Command::Session(id)) if id == session)
                );
                assert!(matches!(command_rx.recv().await, Some(Command::Session(0))));
            }
        })
        .await;
        cancel.cancel();
        client.await.unwrap();
        server.await.unwrap().unwrap();
        outcome.expect("subscription or reconnect timed out");
    }

    #[tokio::test]
    async fn sends_path_token_on_initial_subscription_and_reconnect() {
        check_subscription_metadata(
            "//test%2Ftoken///?ignored=yes#fragment",
            Some("test%2Ftoken"),
        )
        .await;
    }

    #[tokio::test]
    async fn omits_token_on_initial_subscription_and_reconnect_without_path() {
        check_subscription_metadata("/?token=ignored#fragment", None).await;
    }

    type Received = (usize, Option<geyser::SubscribeRequest>);

    // Keeps every stream open and records each request with its stream number.
    // `None` marks the end of a request stream.
    struct OpenService {
        calls: Arc<AtomicUsize>,
        received: mpsc::UnboundedSender<Received>,
    }

    #[tonic::async_trait]
    impl YellowstoneAccountSyncGrpcService for OpenService {
        type SubscribeStream =
            tokio_stream::Pending<Result<geyser::SubscribeUpdate, tonic::Status>>;

        async fn subscribe(
            &self,
            request: tonic::Request<tonic::Streaming<geyser::SubscribeRequest>>,
        ) -> Result<tonic::Response<Self::SubscribeStream>, tonic::Status> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            let mut inbound = request.into_inner();
            let received = self.received.clone();
            tokio::spawn(async move {
                while let Ok(Some(message)) = inbound.message().await {
                    let _ = received.send((call, Some(message)));
                }
                let _ = received.send((call, None));
            });
            Ok(tonic::Response::new(tokio_stream::pending()))
        }
    }

    struct Open {
        desired: watch::Sender<HashSet<Pubkey>>,
        received: mpsc::UnboundedReceiver<Received>,
        commands: mpsc::Receiver<Command>,
        calls: Arc<AtomicUsize>,
        cancel: CancellationToken,
        client: tokio::task::JoinHandle<()>,
        server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    }

    impl Open {
        async fn start(keepalive_interval: Duration, keys: HashSet<Pubkey>) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (received_tx, received) = mpsc::unbounded_channel();
            let calls = Arc::new(AtomicUsize::new(0));
            let cancel = CancellationToken::new();
            let service = OpenService {
                calls: calls.clone(),
                received: received_tx,
            };
            let server = tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(YellowstoneAccountSyncGrpcServiceServer::new(service))
                    .serve_with_incoming_shutdown(
                        TcpListenerStream::new(listener),
                        cancel.clone().cancelled_owned(),
                    ),
            );
            let config = AccountSyncConfig {
                endpoint: format!("{address}"),
                keepalive_interval,
                ..Default::default()
            };
            let (desired, desired_rx) = watch::channel(keys);
            let (commands_tx, commands) = mpsc::channel(8);
            let client = tokio::spawn(run(
                config,
                CommitmentConfig::confirmed(),
                desired_rx,
                commands_tx,
                cancel.clone(),
            ));
            Self {
                desired,
                received,
                commands,
                calls,
                cancel,
                client,
                server,
            }
        }

        async fn next(&mut self) -> Received {
            self.received.recv().await.unwrap()
        }

        async fn stop(self) {
            self.cancel.cancel();
            self.client.await.unwrap();
            self.server.await.unwrap().unwrap();
        }
    }

    fn accounts(request: &geyser::SubscribeRequest) -> HashSet<String> {
        request
            .accounts
            .values()
            .flat_map(|filter| filter.account.iter().cloned())
            .collect()
    }

    #[tokio::test]
    async fn account_list_change_reuses_open_stream() {
        let key = Pubkey::new_from_array([1; 32]);
        let other = Pubkey::new_from_array([2; 32]);
        let mut open = Open::start(Duration::from_secs(60), HashSet::from([key])).await;
        let outcome = tokio::time::timeout(Duration::from_secs(5), async {
            let (call, first) = open.next().await;
            assert_eq!(call, 1);
            assert_eq!(accounts(&first.unwrap()), HashSet::from([key.to_string()]));
            assert!(matches!(
                open.commands.recv().await,
                Some(Command::Session(1))
            ));
            open.desired.send_replace(HashSet::from([key, other]));
            let (call, next) = open.next().await;
            assert_eq!(call, 1);
            assert_eq!(
                accounts(&next.unwrap()),
                HashSet::from([key.to_string(), other.to_string()])
            );
            assert_eq!(open.calls.load(Ordering::SeqCst), 1);
            assert!(open.commands.try_recv().is_err());
        })
        .await;
        open.stop().await;
        outcome.expect("list change timed out");
    }

    #[tokio::test]
    async fn empty_set_closes_stream_without_empty_request() {
        let key = Pubkey::new_from_array([1; 32]);
        let mut open = Open::start(Duration::from_secs(60), HashSet::from([key])).await;
        let outcome = tokio::time::timeout(Duration::from_secs(5), async {
            assert!(open.next().await.1.is_some());
            assert!(matches!(
                open.commands.recv().await,
                Some(Command::Session(1))
            ));
            open.desired.send_replace(HashSet::new());
            assert!(matches!(open.next().await, (1, None)));
            assert!(matches!(
                open.commands.recv().await,
                Some(Command::Session(0))
            ));
            assert_eq!(open.calls.load(Ordering::SeqCst), 1);
        })
        .await;
        open.stop().await;
        outcome.expect("stream close timed out");
    }

    #[tokio::test]
    async fn ping_on_quiet_stream_keeps_account_list() {
        let key = Pubkey::new_from_array([1; 32]);
        let mut open = Open::start(Duration::from_millis(50), HashSet::from([key])).await;
        let outcome = tokio::time::timeout(Duration::from_secs(5), async {
            let first = open.next().await.1.unwrap();
            assert_eq!(accounts(&first), HashSet::from([key.to_string()]));
            let (call, ping) = open.next().await;
            let ping = ping.unwrap();
            assert_eq!(call, 1);
            // A ping-only request leaves the server's account list as is.
            assert!(ping.ping.is_some());
            assert!(ping.accounts.is_empty());
            assert_eq!(ping.commitment, None);
            assert_eq!(open.calls.load(Ordering::SeqCst), 1);
        })
        .await;
        open.stop().await;
        outcome.expect("ping timed out");
    }

    #[test]
    fn request_uses_full_account_set_and_commitment() {
        let keys = HashSet::from([
            Pubkey::new_from_array([1; 32]),
            Pubkey::new_from_array([2; 32]),
        ]);
        let request = make_request(&keys, CommitmentConfig::confirmed());
        assert_eq!(
            request.commitment,
            Some(geyser::CommitmentLevel::Confirmed as i32)
        );
        let accounts = &request.accounts["accounts"].account;
        assert_eq!(accounts.len(), 2);
        assert!(keys.iter().all(|key| accounts.contains(&key.to_string())));
    }

    #[test]
    fn account_update_maps_fields_without_policy_checks() {
        let key = Pubkey::new_from_array([3; 32]);
        let owner = Pubkey::new_from_array([4; 32]);
        let update = geyser::SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Account(geyser::SubscribeUpdateAccount {
                account: Some(geyser::SubscribeUpdateAccountInfo {
                    pubkey: key.to_bytes().to_vec(),
                    owner: owner.to_bytes().to_vec(),
                    lamports: 0,
                    data: vec![5],
                    executable: true,
                    rent_epoch: 6,
                    write_version: 7,
                    txn_signature: None,
                }),
                slot: 8,
                is_startup: false,
                bank_id: Some(9),
            })),
            ..Default::default()
        };
        let account = decode_account(update).unwrap();
        assert_eq!(account.key, key);
        assert_eq!(account.slot, 8);
        assert_eq!(account.version, 7);
        assert_eq!(account.account.lamports, 0);
        assert_eq!(account.account.owner, owner);
        assert_eq!(account.account.data, vec![5]);
    }
}
