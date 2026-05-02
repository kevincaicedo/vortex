use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

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

fn watch_aborts_on_conflict(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("watched:key", "v1")?;
    ctx.assert_ok(&["WATCH", "watched:key"])?;

    let mut other = SmokeContext::connect(ctx.server_url())?;
    other.set("watched:key", "v2")?;

    ctx.assert_ok(&["MULTI"])?;
    let queued: String = ctx.exec(&["SET", "watched:key", "v3"])?;
    assert_eq!(queued, "QUEUED");
    let replies: Option<Vec<redis::Value>> = ctx.exec(&["EXEC"])?;
    assert!(replies.is_none());
    assert_eq!(ctx.get("watched:key")?, Some("v2".to_string()));
    Ok(())
}

fn watch_allows_exec_without_conflict(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("watch:clean", "v1")?;
    ctx.assert_ok(&["WATCH", "watch:clean"])?;

    ctx.assert_ok(&["MULTI"])?;
    let queued: String = ctx.exec(&["SET", "watch:clean", "v2"])?;
    assert_eq!(queued, "QUEUED");

    let replies: Vec<redis::Value> = ctx.exec(&["EXEC"])?;
    assert_eq!(replies.len(), 1);
    assert_eq!(ctx.get("watch:clean")?, Some("v2".to_string()));
    Ok(())
}

fn watch_aborts_on_same_connection_write(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("watch:self", "v1")?;
    ctx.assert_ok(&["WATCH", "watch:self"])?;
    ctx.set("watch:self", "v2")?;

    ctx.assert_ok(&["MULTI"])?;
    let queued: String = ctx.exec(&["SET", "watch:self", "v3"])?;
    assert_eq!(queued, "QUEUED");

    let replies: Option<Vec<redis::Value>> = ctx.exec(&["EXEC"])?;
    assert!(replies.is_none());
    assert_eq!(ctx.get("watch:self")?, Some("v2".to_string()));
    Ok(())
}

fn watch_multi_key_conflict(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("watch:k1", "v1")?;
    ctx.set("watch:k2", "v1")?;
    ctx.assert_ok(&["WATCH", "watch:k1", "watch:k2"])?;

    let mut other = SmokeContext::connect(ctx.server_url())?;
    other.set("watch:k2", "v2")?;

    ctx.assert_ok(&["MULTI"])?;
    let queued: String = ctx.exec(&["SET", "watch:k1", "v3"])?;
    assert_eq!(queued, "QUEUED");

    let replies: Option<Vec<redis::Value>> = ctx.exec(&["EXEC"])?;
    assert!(replies.is_none());
    assert_eq!(ctx.get("watch:k1")?, Some("v1".to_string()));
    assert_eq!(ctx.get("watch:k2")?, Some("v2".to_string()));
    Ok(())
}

fn watch_missing_key_aborts_after_set_delete(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["WATCH", "watch:missing"])?;

    let mut other = SmokeContext::connect(ctx.server_url())?;
    other.set("watch:missing", "v1")?;
    let deleted: i64 = other.exec(&["DEL", "watch:missing"])?;
    assert_eq!(deleted, 1);

    ctx.assert_ok(&["MULTI"])?;
    let queued: String = ctx.exec(&["SET", "watch:probe", "v2"])?;
    assert_eq!(queued, "QUEUED");

    let replies: Option<Vec<redis::Value>> = ctx.exec(&["EXEC"])?;
    assert!(replies.is_none());
    Ok(())
}

fn watch_aborts_after_flushall(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("watch:flush", "v1")?;
    ctx.assert_ok(&["WATCH", "watch:flush"])?;

    let mut other = SmokeContext::connect(ctx.server_url())?;
    other.assert_ok(&["FLUSHALL"])?;

    ctx.assert_ok(&["MULTI"])?;
    let queued: String = ctx.exec(&["SET", "watch:probe", "v2"])?;
    assert_eq!(queued, "QUEUED");

    let replies: Option<Vec<redis::Value>> = ctx.exec(&["EXEC"])?;
    assert!(replies.is_none());
    Ok(())
}

fn watch_aborts_after_expire_only_mutation(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("watch:ttl", "v1")?;
    ctx.assert_ok(&["WATCH", "watch:ttl"])?;

    let mut other = SmokeContext::connect(ctx.server_url())?;
    let applied: i64 = other.exec(&["PEXPIRE", "watch:ttl", "60000"])?;
    assert_eq!(applied, 1);

    ctx.assert_ok(&["MULTI"])?;
    let queued: String = ctx.exec(&["SET", "watch:probe", "v2"])?;
    assert_eq!(queued, "QUEUED");

    let replies: Option<Vec<redis::Value>> = ctx.exec(&["EXEC"])?;
    assert!(replies.is_none());
    Ok(())
}

fn watch_aborts_after_persist_only_mutation(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["SET", "watch:persist", "v1", "EX", "60"])?;
    ctx.assert_ok(&["WATCH", "watch:persist"])?;

    let mut other = SmokeContext::connect(ctx.server_url())?;
    let persisted: i64 = other.exec(&["PERSIST", "watch:persist"])?;
    assert_eq!(persisted, 1);

    ctx.assert_ok(&["MULTI"])?;
    let queued: String = ctx.exec(&["SET", "watch:probe", "v2"])?;
    assert_eq!(queued, "QUEUED");

    let replies: Option<Vec<redis::Value>> = ctx.exec(&["EXEC"])?;
    assert!(replies.is_none());
    Ok(())
}

fn watch_rejects_wrong_arity(ctx: &mut SmokeContext) -> Result<()> {
    let err = ctx.exec_error(&["WATCH"])?;
    assert!(err.to_string().contains("wrong number of arguments"));
    Ok(())
}

fn watch_inside_multi_aborts_exec(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["MULTI"])?;

    let err = ctx.exec_error(&["WATCH", "watch:inside"])?;
    assert!(err.to_string().contains("WATCH inside MULTI"));

    let err = ctx.exec_error(&["EXEC"])?;
    assert_redis_error_contains(&err, "EXECABORT");
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("WATCH", CommandGroup::Transaction, SupportLevel::Supported)
        .summary("Watches keys and aborts EXEC when another client modifies them.")
        .syntax(&["WATCH key [key ...]"])
        .tested(&[
            "No-conflict EXEC success",
            "External conflict abort",
            "Same-connection pre-MULTI write abort",
            "Multi-key conflict abort",
            "Missing-key set+del conflict abort",
            "FLUSHALL invalidation abort",
            "TTL-only PEXPIRE mutation abort",
            "TTL-only PERSIST mutation abort",
            "Wrong arity",
            "WATCH inside MULTI dirties transaction",
        ])
        .case(CaseDef::new(
            "watch aborts on conflict",
            "WATCH should make EXEC return a nil array after an external write.",
            watch_aborts_on_conflict,
        ))
        .case(CaseDef::new(
            "watch allows clean exec",
            "WATCH should allow EXEC when no watched key changed before EXEC.",
            watch_allows_exec_without_conflict,
        ))
        .case(CaseDef::new(
            "same connection write aborts",
            "A pre-MULTI write by the watching connection should also abort EXEC.",
            watch_aborts_on_same_connection_write,
        ))
        .case(CaseDef::new(
            "multi-key conflict",
            "A change to any watched key should abort EXEC.",
            watch_multi_key_conflict,
        ))
        .case(CaseDef::new(
            "missing key set-delete conflict",
            "WATCH on a missing key should still abort after another client creates and removes it.",
            watch_missing_key_aborts_after_set_delete,
        ))
        .case(CaseDef::new(
            "flushall invalidates watch",
            "FLUSHALL should invalidate the watch epoch and abort EXEC.",
            watch_aborts_after_flushall,
        ))
        .case(CaseDef::new(
            "pexpire mutation aborts watch",
            "Adding only expiry metadata to a watched key should still abort EXEC.",
            watch_aborts_after_expire_only_mutation,
        ))
        .case(CaseDef::new(
            "persist mutation aborts watch",
            "Removing only expiry metadata from a watched key should still abort EXEC.",
            watch_aborts_after_persist_only_mutation,
        ))
        .case(CaseDef::new(
            "watch rejects wrong arity",
            "WATCH should require at least one key.",
            watch_rejects_wrong_arity,
        ))
        .case(CaseDef::new(
            "watch inside multi aborts exec",
            "WATCH inside MULTI should error and dirty the transaction.",
            watch_inside_multi_aborts_exec,
        ))
}
