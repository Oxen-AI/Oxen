fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/merkle_tree.capnp")
        .run()
        .expect("schema compiler command");
}
