# Rust SDK examples

These examples show SDK features for Yellowstone Account Sync. They default to `confirmed` commitment. The basic examples use fixed account keys near the top of each file. The comparison loads its keys from `accounts.csv`. All client and Solana types come from `triton_sdk`.

## Setup

Set the Solana JSON-RPC and account-sync gRPC endpoints for the same cluster. Replace these local addresses with your service URLs if needed:

```bash
export RPC_URL='http://localhost:8899'
export ACCOUNT_SYNC_URL='http://localhost:10000'
```

Run the commands below from the repository root.

## Examples

| Example | Main SDK functionality | Command |
| --- | --- | --- |
| [Pinned account](pinned.rs) | Set `pinned_accounts` and read one account with its context slot. | `cargo run -p triton-sdk --example pinned` |
| [Temporary subscription](dynamic.rs) | Read without pinning, wait past a two-second subscription lifetime and a refresh interval, then read again. | `cargo run -p triton-sdk --example dynamic` |
| [Multiple accounts](get_multiple_accounts_grpc.rs) | Read two keys plus a duplicate and print the results in input order. | `cargo run -p triton-sdk --example get_multiple_accounts_grpc` |
| [Account lifecycle](account_lifecycle_grpc.rs) | Add, read, remove, re-add, and replace pinned accounts. Inspect the current set with `account_sync_config()`. | `cargo run -p triton-sdk --example account_lifecycle_grpc` |
| [Read timings](compare_multiple_accounts_latency.rs) | Read the same 100 accounts as the JS comparison. Print timings for new matching results until interrupted. | `cargo run -p triton-sdk --example compare_multiple_accounts_latency` |

## Reading the output

Configured reads can return cached accounts or fall back to RPC. A successful read does not prove that the stream is connected or that the cache supplied the result. A buffered batch preserves order and duplicates; its context slot is the lowest cached slot in that batch.

The temporary subscription example shows how to renew an idle subscription with another read. Its output does not expose or prove cache eviction. Pinned accounts stay subscribed until removed. Removing a pin can leave an existing temporary subscription active; the lifecycle example disables automatic subscriptions to keep this distinction simple. A rapid remove-and-re-add can accept a result from an earlier RPC request if it passes the cache slot rules.

## Read timing comparison

`accounts.csv` contains the first 100 unique keys from the JS example's account list, in the same order. The loader skips blank lines and duplicates, and requires 100 valid keys.

The comparison follows the JS loop:

1. Start both 100-account reads concurrently, with no warm-up call.
2. Measure each full request and validate the result count.
3. Hash each result with the JS SHA-256 field encoding, including account order, missing accounts, and every account field.
4. Print durations, the winner, and the time difference only when the hashes match in the same round and the state has not already been printed.
5. Remember at most 10,000 printed states, removing the oldest first.
6. Wait for the remainder of the 250 ms polling interval and repeat. Report read errors once per error type and source until a successful read resets that source.

The loop runs until Ctrl+C or SIGTERM on Unix. It finishes the active pair of reads and closes the configured client. There is no three-update limit. As in the JS example, unchanged results or mismatched results produce no timing output.

The comparison also accepts the JS-style endpoint flag, commitment, and polling interval:

```bash
cargo run -p triton-sdk --example compare_multiple_accounts_latency -- \
  --rpc-endpoint "$RPC_URL" confirmed 250
```

`--rpc-endpoint` overrides `RPC_URL`. `ACCOUNT_SYNC_URL` selects the streaming endpoint; if unset, this example uses the RPC endpoint for streaming too. Other examples require both environment variables.

Timings measure full batch request duration, not stream delivery latency. Matching account values do not prove that both clients observed the same slot or that the configured result came from the stream.
