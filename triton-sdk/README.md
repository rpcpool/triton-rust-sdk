# Triton Rust SDK

`triton_sdk::nonblocking::rpc_client::RpcClient` wraps Solana's nonblocking RPC client. Construct it with `RpcClient::with_account_sync(inner, config)` to enable account-sync reads. Set `AccountSyncConfig::endpoint` to the account-sync gRPC URL. The normal Solana-style constructors work without account sync.

The raw `get_account` and `get_multiple_accounts` methods use buffered account observations when available. A miss starts a temporary subscription by default and races the stream with a full Solana RPC request. Pinned subscriptions stay active until removed or the client closes. Call `close().await` for graceful shutdown.

The Solana client version is pinned to 4.3.0. Its raw account methods are `get_account`, `get_account_with_commitment`, `get_account_data`, `get_multiple_accounts`, and `get_multiple_accounts_with_commitment`. The config-based account methods in this version return UI account types and remain on the inner client.

Other Solana methods remain available through `Deref` and `inner()`. UI account methods use Solana RPC directly. A buffered multiple-account response keeps input order and duplicates, but its accounts may come from different observation slots. Its context slot is the lowest slot among them.

The stream account message has no explicit deletion marker. Missing accounts enter the cache through a Solana RPC observation. Stream ordering uses slot and write version. RPC observations have a slot but no write version; at the same slot, a stream observation wins.

Run `cargo run -p triton-sdk --example pinned` or `cargo run -p triton-sdk --example dynamic` after setting the example URLs to reachable services.

## Direct dependencies

`solana-client` supplies the wrapped RPC client. `solana-account`, `solana-pubkey`, `solana-commitment-config`, and `solana-rpc-client-api` supply its public account method types. Solana 4.3 uses these split crates, so this SDK does not need a direct `solana-sdk` dependency. `yellowstone-account-sync-proto` supplies the local generated stream client and messages. `tonic` configures the transport. `tokio`, `tokio-stream`, and `tokio-util` run the lane actors, request stream, and cancellation. `thiserror` defines the public setup and control errors.
