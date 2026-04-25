use anyhow::Result;
use redis::FromRedisValue;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn multi_queues_and_executes(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["MULTI"])?;

    let queued: String = ctx.exec(&["SET", "tx:key", "value"])?;
    assert_eq!(queued, "QUEUED");
    let queued: String = ctx.exec(&["GET", "tx:key"])?;
    assert_eq!(queued, "QUEUED");

    let replies: Vec<redis::Value> = ctx.exec(&["EXEC"])?;
    assert_eq!(replies.len(), 2);
    let set_reply = String::from_redis_value(&replies[0])?;
    let get_reply = String::from_redis_value(&replies[1])?;
    assert_eq!(set_reply, "OK");
    assert_eq!(get_reply, "value");
    assert_eq!(ctx.get("tx:key")?, Some("value".to_string()));
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("MULTI", CommandGroup::Server, SupportLevel::Supported)
        .summary("Starts a transaction and queues commands until EXEC.")
        .syntax(&["MULTI"])
        .tested(&["Queued command replies", "EXEC applies queued commands"])
        .not_tested(&["Nested transaction errors", "Large queued transactions"])
        .case(CaseDef::new(
            "queues and executes transaction",
            "MULTI should queue commands and EXEC should return their replies.",
            multi_queues_and_executes,
        ))
}
