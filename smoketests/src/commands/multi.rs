use std::io::{Read, Write};
use std::net::TcpStream;

use anyhow::{Result, ensure};
use redis::FromRedisValue;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn assert_queued(ctx: &mut SmokeContext, args: &[&str]) -> Result<()> {
    let queued: String = ctx.exec(args)?;
    assert_eq!(queued, "QUEUED");
    Ok(())
}

fn assert_redis_error_contains(err: &redis::RedisError, needle: &str) {
    let found = err.code().is_some_and(|code| code.contains(needle))
        || err.detail().is_some_and(|detail| detail.contains(needle))
        || err.to_string().contains(needle);
    assert!(
        found,
        "redis error `{err}` did not contain `{needle}`; code={:?}, detail={:?}",
        err.code(),
        err.detail()
    );
}

fn raw_roundtrip(ctx: &SmokeContext, request: &[u8], expected: &[u8]) -> Result<()> {
    let addr = ctx
        .server_url()
        .strip_prefix("redis://")
        .and_then(|rest| rest.split('/').next())
        .ok_or_else(|| anyhow::anyhow!("unsupported smoke URL: {}", ctx.server_url()))?;
    let mut stream = TcpStream::connect(addr)?;
    stream.write_all(request)?;

    let mut actual = vec![0u8; expected.len()];
    let mut read = 0usize;
    while read < actual.len() {
        let n = stream.read(&mut actual[read..])?;
        ensure!(n != 0, "server closed raw smoke connection early");
        read += n;
    }
    ensure!(
        actual == expected,
        "raw response mismatch\nexpected: {:?}\nactual:   {:?}",
        String::from_utf8_lossy(expected),
        String::from_utf8_lossy(&actual)
    );
    Ok(())
}

fn multi_queues_and_executes(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["MULTI"])?;

    assert_queued(ctx, &["SET", "tx:key", "value"])?;
    assert_queued(ctx, &["GET", "tx:key"])?;

    let replies: Vec<redis::Value> = ctx.exec(&["EXEC"])?;
    assert_eq!(replies.len(), 2);
    let set_reply = String::from_redis_value(&replies[0])?;
    let get_reply = String::from_redis_value(&replies[1])?;
    assert_eq!(set_reply, "OK");
    assert_eq!(get_reply, "value");
    assert_eq!(ctx.get("tx:key")?, Some("value".to_string()));
    Ok(())
}

fn multi_empty_exec(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["MULTI"])?;
    let replies: Vec<redis::Value> = ctx.exec(&["EXEC"])?;
    assert!(replies.is_empty(), "empty transaction returned {replies:?}");
    Ok(())
}

fn multi_wrong_arity_does_not_enter_transaction(ctx: &mut SmokeContext) -> Result<()> {
    let err = ctx.exec_error(&["MULTI", "extra"])?;
    assert!(err.to_string().contains("wrong number of arguments"));

    ctx.assert_ok(&["SET", "tx:arity", "immediate"])?;
    assert_eq!(ctx.get("tx:arity")?, Some("immediate".to_string()));

    let err = ctx.exec_error(&["EXEC"])?;
    assert!(err.to_string().contains("EXEC without MULTI"));
    Ok(())
}

fn multi_queue_time_error_aborts(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["MULTI"])?;
    assert_queued(ctx, &["SET", "tx:abort", "value"])?;

    let err = ctx.exec_error(&["INCR", "tx:abort", "extra"])?;
    assert!(err.to_string().contains("wrong number of arguments"));

    let err = ctx.exec_error(&["EXEC"])?;
    assert_redis_error_contains(&err, "EXECABORT");
    assert_eq!(ctx.get("tx:abort")?, None);
    Ok(())
}

fn multi_unknown_command_aborts(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["MULTI"])?;

    let err = ctx.exec_error(&["VORTEX_UNKNOWN_FOR_SMOKE"])?;
    assert!(err.to_string().contains("unknown command"));

    let err = ctx.exec_error(&["EXEC"])?;
    assert_redis_error_contains(&err, "EXECABORT");
    Ok(())
}

fn multi_nested_marks_dirty(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["MULTI"])?;

    let err = ctx.exec_error(&["MULTI"])?;
    assert!(err.to_string().contains("MULTI calls can not be nested"));

    let err = ctx.exec_error(&["EXEC"])?;
    assert_redis_error_contains(&err, "EXECABORT");
    Ok(())
}

fn multi_runtime_error_does_not_abort_later_commands(ctx: &mut SmokeContext) -> Result<()> {
    let request = b"*1\r\n$5\r\nMULTI\r\n\
*3\r\n$3\r\nSET\r\n$10\r\ntx:raw:num\r\n$5\r\nnoint\r\n\
*2\r\n$4\r\nINCR\r\n$10\r\ntx:raw:num\r\n\
*3\r\n$3\r\nSET\r\n$12\r\ntx:raw:after\r\n$2\r\nok\r\n\
*1\r\n$4\r\nEXEC\r\n\
*2\r\n$3\r\nGET\r\n$12\r\ntx:raw:after\r\n";
    let expected = b"+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n\
*3\r\n+OK\r\n-ERR value is not an integer or out of range\r\n+OK\r\n\
$2\r\nok\r\n";
    raw_roundtrip(ctx, request, expected)?;
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("MULTI", CommandGroup::Transaction, SupportLevel::Supported)
        .summary("Starts a transaction and queues commands until EXEC.")
        .syntax(&["MULTI"])
        .tested(&[
            "Queued command replies",
            "Empty EXEC",
            "Wrong arity before MULTI state",
            "Queue-time error causes EXECABORT",
            "Unknown command causes EXECABORT",
            "Nested MULTI causes EXECABORT",
            "Runtime errors stay in EXEC reply array",
        ])
        .case(CaseDef::new(
            "queues and executes transaction",
            "MULTI should queue commands and EXEC should return their replies.",
            multi_queues_and_executes,
        ))
        .case(CaseDef::new(
            "empty transaction",
            "EXEC should return an empty array for an empty transaction.",
            multi_empty_exec,
        ))
        .case(CaseDef::new(
            "wrong arity does not enter transaction",
            "MULTI with arguments should fail and leave the connection outside MULTI mode.",
            multi_wrong_arity_does_not_enter_transaction,
        ))
        .case(CaseDef::new(
            "queue-time error aborts exec",
            "A queue-time command error should make EXEC return EXECABORT and discard queued work.",
            multi_queue_time_error_aborts,
        ))
        .case(CaseDef::new(
            "unknown command aborts exec",
            "An unknown command inside MULTI should make EXEC return EXECABORT.",
            multi_unknown_command_aborts,
        ))
        .case(CaseDef::new(
            "nested multi aborts exec",
            "Nested MULTI should dirty the transaction and make EXEC return EXECABORT.",
            multi_nested_marks_dirty,
        ))
        .case(CaseDef::new(
            "runtime error remains in exec array",
            "A runtime command error should be returned inside the EXEC array without rolling back later commands.",
            multi_runtime_error_does_not_abort_later_commands,
        ))
}
