use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    // SAFETY: build scripts run single-threaded here before code generation.
    unsafe {
        env::set_var("PROTOC", protoc_path);
    }

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let local_proto_dir = manifest_dir.join("proto");
    let yellowstone_proto_dir = manifest_dir
        .join("proto")
        .join("yellowstone-grpc")
        .join("yellowstone-grpc-proto")
        .join("proto");

    println!(
        "cargo:rerun-if-changed={}",
        local_proto_dir.join("account_sync.proto").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        yellowstone_proto_dir.join("geyser.proto").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        yellowstone_proto_dir.join("solana-storage.proto").display()
    );

    tonic_prost_build::configure()
        .emit_package(false)
        .compile_protos(
            &[local_proto_dir.join("account_sync.proto")],
            &[local_proto_dir, yellowstone_proto_dir],
        )?;

    Ok(())
}
