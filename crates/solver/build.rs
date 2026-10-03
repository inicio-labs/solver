fn main() {
    // embed_migrations! cannot track new migration directories on its own.
    println!("cargo:rerun-if-changed=migrations_postgres");

    // Maker gateway protocol (ADR 0003), compiled without a system protoc.
    println!("cargo:rerun-if-changed=proto");
    let descriptors = protox::compile(["maker/v1/gateway.proto"], ["proto"])
        .expect("maker gateway proto does not compile");
    tonic_prost_build::configure()
        .build_transport(false)
        .compile_fds(descriptors)
        .expect("maker gateway code generation failed");
}
