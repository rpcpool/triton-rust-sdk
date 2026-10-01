use std::{
    collections::{HashSet, VecDeque},
    env,
    error::Error,
    future::Future,
    time::{Duration, Instant},
};

use sha2::{Digest, Sha256};
use triton_sdk::{
    Account, AccountSyncConfig, ClientError, CommitmentConfig, Configured, Pubkey, RpcClient,
};

const ACCOUNT_COUNT: usize = 100;
const MAX_PRINTED_STATES: usize = 10_000;
type StateHash = [u8; 32];

struct TimedBatchRead {
    hash: StateHash,
    duration: Duration,
}

#[derive(Debug)]
struct FailedRead {
    name: &'static str,
    duration: Duration,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let (url, commitment, poll_interval) = settings()?;
    let csv = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/accounts.csv"
    ))?;
    let keys = load_public_keys(&csv, ACCOUNT_COUNT)?;
    let plain = RpcClient::new_with_commitment(url.clone(), commitment);
    let endpoint = env::var("ACCOUNT_SYNC_URL").unwrap_or_else(|_| url.clone());
    let configured =
        RpcClient::new_with_commitment(url, commitment).with_account_sync(AccountSyncConfig {
            endpoint,
            pinned_accounts: keys.iter().copied().collect(),
            ..Default::default()
        })?;

    println!(
        "comparing get_multiple_accounts: accounts={} commitment={:?} poll_interval_ms={}",
        keys.len(),
        commitment.commitment,
        poll_interval.as_millis()
    );
    println!("waiting for account states returned by both clients; stop with Ctrl+C");
    let result = compare(&plain, &configured, &keys, poll_interval).await;
    let closed = configured.close().await;
    result?;
    closed?;
    Ok(())
}

fn settings() -> Result<(String, CommitmentConfig, Duration), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let mut endpoint = None;
    let mut positional = Vec::new();
    while let Some(arg) = args.next() {
        let value = if arg == "--rpc-endpoint" {
            Some(args.next().ok_or("missing --rpc-endpoint value")?)
        } else if let Some(value) = arg.strip_prefix("--rpc-endpoint=") {
            Some(value.to_owned())
        } else if arg.starts_with("--") {
            return Err(format!("unknown option: {arg}").into());
        } else {
            positional.push(arg);
            None
        };
        if let Some(value) = value {
            if value.trim().is_empty() || value.starts_with("--") || endpoint.is_some() {
                return Err("provide --rpc-endpoint once with a nonempty URL".into());
            }
            endpoint = Some(value.trim().to_owned());
        }
    }
    if positional.len() > 2 {
        return Err(
            "usage: [--rpc-endpoint URL] [processed|confirmed|finalized] [poll_interval_ms]".into(),
        );
    }
    let commitment = match positional
        .first()
        .map(String::as_str)
        .unwrap_or("confirmed")
    {
        "processed" => CommitmentConfig::processed(),
        "confirmed" => CommitmentConfig::confirmed(),
        "finalized" => CommitmentConfig::finalized(),
        _ => return Err("commitment must be processed, confirmed, or finalized".into()),
    };
    let poll_ms = positional
        .get(1)
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(250);
    if poll_ms == 0 {
        return Err("poll_interval_ms must be a positive integer".into());
    }
    let url = endpoint
        .or_else(|| env::var("RPC_URL").ok())
        .ok_or("set RPC_URL or pass --rpc-endpoint URL")?;
    Ok((url, commitment, Duration::from_millis(poll_ms)))
}

fn load_public_keys(csv: &str, count: usize) -> Result<Vec<Pubkey>, Box<dyn Error>> {
    let mut keys = Vec::new();
    let mut seen = HashSet::new();
    for (index, line) in csv.lines().enumerate() {
        let value = line.trim();
        if value.is_empty() {
            continue;
        }
        let key = value
            .parse::<Pubkey>()
            .map_err(|_| format!("invalid public key in accounts.csv at line {}", index + 1))?;
        if seen.insert(key) {
            keys.push(key);
            if keys.len() == count {
                return Ok(keys);
            }
        }
    }
    Err(format!(
        "accounts.csv contains {} valid unique accounts; {count} are required",
        keys.len()
    )
    .into())
}

async fn compare(
    plain: &RpcClient,
    configured: &RpcClient<Configured>,
    keys: &[Pubkey],
    poll_interval: Duration,
) -> Result<(), Box<dyn Error>> {
    let mut stop = tokio::spawn(stop_signal());
    let mut printed = HashSet::new();
    let mut order = VecDeque::new();
    let (mut last_triton_error, mut last_rpc_error) = (None, None);
    let mut update = 0_u64;
    loop {
        if stop.is_finished() {
            stop.await??;
            return Ok(());
        }
        let started = Instant::now();
        let (triton, rpc) = tokio::join!(
            measure_batch_read(configured.get_multiple_accounts(keys), keys.len()),
            measure_batch_read(plain.get_multiple_accounts(keys), keys.len()),
        );
        report_read_error("triton", &triton, &mut last_triton_error);
        report_read_error("solana", &rpc, &mut last_rpc_error);
        if let (Ok(triton), Ok(rpc)) = (triton, rpc)
            && triton.hash == rpc.hash
            && remember_printed_state(&mut printed, &mut order, triton.hash)
        {
            update += 1;
            print_matched_state(update, &triton, &rpc);
        }
        tokio::select! {
            result = &mut stop => { result??; return Ok(()); }
            _ = tokio::time::sleep(poll_interval.saturating_sub(started.elapsed())) => {}
        }
    }
}

async fn stop_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}

async fn measure_batch_read(
    read: impl Future<Output = Result<Vec<Option<Account>>, ClientError>>,
    expected_count: usize,
) -> Result<TimedBatchRead, FailedRead> {
    let started = Instant::now();
    let accounts = read.await.map_err(|_| FailedRead {
        name: "ClientError",
        duration: started.elapsed(),
    })?;
    if accounts.len() != expected_count {
        return Err(FailedRead {
            name: "AccountCountError",
            duration: started.elapsed(),
        });
    }
    let duration = started.elapsed();
    Ok(TimedBatchRead {
        hash: hash_accounts(&accounts),
        duration,
    })
}

fn hash_accounts(accounts: &[Option<Account>]) -> StateHash {
    let mut hash = Sha256::new();
    hash.update(accounts.len().to_string());
    for account in accounts {
        hash.update(b"|");
        match account {
            Some(account) => hash.update(fingerprint_account(account)),
            None => hash.update(b"missing"),
        }
    }
    hash.finalize().into()
}

fn fingerprint_account(account: &Account) -> String {
    let mut hash = Sha256::new();
    hash.update(account.lamports.to_string());
    hash.update(b"|");
    hash.update(account.owner.to_bytes());
    hash.update(if account.executable { b"|1|" } else { b"|0|" });
    hash.update(account.rent_epoch.to_string());
    hash.update(b"|");
    hash.update(&account.data);
    format!("{:x}", hash.finalize())
}

fn report_read_error(
    source: &str,
    read: &Result<TimedBatchRead, FailedRead>,
    last_error: &mut Option<&'static str>,
) {
    match read {
        Ok(_) => *last_error = None,
        Err(error) if *last_error != Some(error.name) => {
            *last_error = Some(error.name);
            eprintln!(
                "{source} get_multiple_accounts failed after {:.3}ms ({})",
                error.duration.as_secs_f64() * 1000.0,
                error.name
            );
        }
        Err(_) => {}
    }
}

fn remember_printed_state(
    printed: &mut HashSet<StateHash>,
    order: &mut VecDeque<StateHash>,
    hash: StateHash,
) -> bool {
    if !printed.insert(hash) {
        return false;
    }
    order.push_back(hash);
    if order.len() > MAX_PRINTED_STATES
        && let Some(oldest) = order.pop_front()
    {
        printed.remove(&oldest);
    }
    true
}

fn print_matched_state(update: u64, triton: &TimedBatchRead, rpc: &TimedBatchRead) {
    let triton_ms = (triton.duration.as_secs_f64() * 1_000_000.0).round() / 1000.0;
    let rpc_ms = (rpc.duration.as_secs_f64() * 1_000_000.0).round() / 1000.0;
    let winner = if triton_ms == rpc_ms {
        "tie"
    } else if triton_ms < rpc_ms {
        "triton-account-sync"
    } else {
        "solana-rpc"
    };
    println!("update={update}");
    println!("solana-rpc durationMs={rpc_ms:.3}");
    println!("triton durationMs={triton_ms:.3}");
    println!(
        "winner={winner} fasterByMs={:.3}",
        (rpc_ms - triton_ms).abs()
    );
}
