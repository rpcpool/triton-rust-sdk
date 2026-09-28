use std::collections::HashSet;

use solana_account::Account;
use solana_commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_pubkey::Pubkey;
use tokio::{
    sync::{mpsc, watch},
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};
use yellowstone_account_sync_proto::{
    account_sync::yellowstone_account_sync_grpc_service_client::YellowstoneAccountSyncGrpcServiceClient,
    geyser::{self, subscribe_update::UpdateOneof},
};

use super::lane::Command;
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
    let endpoint = Endpoint::from_shared(config.endpoint.clone())?
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
    let response = tokio::select! {
        _ = cancel.cancelled() => return Ok(()),
        result = client.subscribe(tokio_stream::wrappers::ReceiverStream::new(request_rx)) => result?,
    };
    let mut stream = response.into_inner();
    commands
        .send(Command::Session(session))
        .await
        .map_err(|_| AccountSyncError::ChannelClosed)?;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            changed = desired.changed() => {
                if changed.is_err() { return Ok(()); }
                return Ok(());
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
    use super::*;

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
