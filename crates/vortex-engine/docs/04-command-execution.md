# Engine Commands and Execution

The vortex engine removes heavy runtime parsing overhead globally. Rather than dynamically traversing argument types using boxed traits like `Box<dyn Command>`, `vortex-engine` defines a fully static resolution loop over hardcoded matching paths, leading to ultra-fast inline compiler optimizations.

## Command Routing 

When an `io_uring` connection reactor builds its arguments off the network bytes, the execution calls into `src/commands/mod.rs`: `execute_command(...)`.

```rust
pub fn execute_command(
    keyspace: &ConcurrentKeyspace,
    name: &[u8],
    frame: &FrameRef<'_>,
    now_nanos: u64,
) -> Option<ExecutedCommand> {
    match name {
        b"GET" => Some(string::cmd_get(keyspace, frame, now_nanos).into()),
        b"SET" => Some(string::cmd_set(keyspace, frame, now_nanos).into()),
        // ... statically routed.
        _ => None,
    }
}
```

This structure is intentional:
1. **Zero-allocation frames:** `execute_command` accepts `FrameRef`, keeping `std::String` allocation out of the dispatch phase.
2. **Nanosecond Timing Engine:** The engine commands rely on `now_nanos` provided as an argument from the caller rather than generating system timestamps repeatedly. It guarantees TTL checking maintains identical timestamps for batched commands during one reactor loop.

## CmdResult & Static Returns

The `CmdResult` enum restricts the amount of memory requested when formatting responses for the end-user:
```rust
pub enum CmdResult {
    Static(&'static [u8]),
    Inline(InlineResp),
    Resp(RespFrame),
}
```

A significant portion of commands execute and then must inform the user `+OK\r\n`. By creating a `CmdResult::Static` variant referencing a static lifetime `&[u8]`, sending typical Redis protocol OK/ERR messages forces `0` dynamic byte array constructions. Instead, the reference address is fed to the wire protocols instantly.

When values must dynamically answer users, but are exceedingly short, `CmdResult::Inline` acts similarly by preventing heap manipulation.

## Category Split

Inside `vortex-engine/src/commands/`, functions are categorized cleanly for scaling constraints.
- `string.rs`: Basic manipulation `SET, GET, INCR, GETDEL, APPEND, INCRBYFLOAT`.
- `key.rs`: TTL modifications and database checks `TTL, PTTL, EXISTS, DEL, RENAME, TYPE, SCAN`.
- `server.rs`: Administrative metrics or bulk alterations `PING, FLUSHALL, DBSIZE, MULTI, EXEC, INFO`.

Since commands execute securely enclosed within their locks requested from the keyspace, the command dispatchers never leak locking primitives back out onto the application reactor layer, keeping abstractions perfectly isolated.
