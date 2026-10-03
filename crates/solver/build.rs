fn main() {
    // embed_migrations! cannot track new migration directories on its own.
    println!("cargo:rerun-if-changed=migrations_postgres");
}
