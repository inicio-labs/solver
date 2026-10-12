// Generates the maker gateway client from the repository's gateway.proto.
// protox compiles the proto in Rust, so no system `protoc` is needed.
fn main() {
    println!("cargo:rerun-if-changed=../../crates/solver/proto");
    let descriptors = protox::compile(["maker/v1/gateway.proto"], ["../../crates/solver/proto"])
        .expect("gateway.proto does not compile");
    tonic_prost_build::configure()
        .build_server(false)
        .compile_fds(descriptors)
        .expect("gateway client code generation failed");
}
