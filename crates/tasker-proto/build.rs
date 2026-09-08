//! Compiles the `.proto` files with the installed `protoc`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        "proto/tasker/v1/common.proto",
        "proto/tasker/v1/control.proto",
        "proto/tasker/v1/worker.proto",
        "proto/externalscaler/externalscaler.proto",
    ];
    for p in &protos {
        // Re-run only when a .proto changes, not on every build.
        println!("cargo:rerun-if-changed={p}");
    }
    let mut config = prost_build::Config::new();
    // `bytes` fields become `bytes::Bytes` instead of `Vec<u8>`: zero-copy off the wire.
    config.bytes(["."]);
    tonic_prost_build::configure().compile_with_config(config, &protos, &["proto"])?;
    Ok(())
}
