fn main() -> Result<(), Box<dyn std::error::Error>> {
    // protox compiles the schema in pure Rust, so neither builders nor
    // docs.rs need a system `protoc`.
    println!("cargo:rerun-if-changed=proto/krpc.proto");
    let file_descriptors = protox::compile(["proto/krpc.proto"], ["proto/"])?;
    prost_build::Config::new().compile_fds(file_descriptors)?;
    Ok(())
}
