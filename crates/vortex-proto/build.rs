use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

fn main() {
    let path = Path::new(&env::var("OUT_DIR").unwrap()).join("command_table.rs");
    let mut file = BufWriter::new(File::create(&path).unwrap());

    // Generate a perfect hash table for command dispatch.
    // Phase 0 stubs: PING, SET, GET, DEL, EXISTS, TTL, EXPIRE, COMMAND, INFO, QUIT.
    let mut map = phf_codegen::Map::new();

    map.entry(
        "PING",
        "&CommandMeta { name: \"PING\", arity: -1, flags: CommandFlags::FAST }",
    );
    map.entry(
        "SET",
        "&CommandMeta { name: \"SET\", arity: -3, flags: CommandFlags::WRITE }",
    );
    map.entry(
        "GET",
        "&CommandMeta { name: \"GET\", arity: 2, flags: CommandFlags::READ }",
    );
    map.entry(
        "DEL",
        "&CommandMeta { name: \"DEL\", arity: -2, flags: CommandFlags::WRITE }",
    );
    map.entry(
        "EXISTS",
        "&CommandMeta { name: \"EXISTS\", arity: -2, flags: CommandFlags::READ }",
    );
    map.entry(
        "TTL",
        "&CommandMeta { name: \"TTL\", arity: 2, flags: CommandFlags::READ }",
    );
    map.entry(
        "EXPIRE",
        "&CommandMeta { name: \"EXPIRE\", arity: 3, flags: CommandFlags::WRITE }",
    );
    map.entry(
        "COMMAND",
        "&CommandMeta { name: \"COMMAND\", arity: -1, flags: CommandFlags::SLOW }",
    );
    map.entry(
        "INFO",
        "&CommandMeta { name: \"INFO\", arity: -1, flags: CommandFlags::SLOW }",
    );
    map.entry(
        "QUIT",
        "&CommandMeta { name: \"QUIT\", arity: 1, flags: CommandFlags::FAST }",
    );

    writeln!(
        &mut file,
        "/// Compile-time perfect hash table for O(1) command dispatch.\n\
         static COMMAND_TABLE: phf::Map<&'static str, &'static CommandMeta> = {};",
        map.build()
    )
    .unwrap();
}
