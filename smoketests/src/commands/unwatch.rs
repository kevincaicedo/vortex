use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn unwatch_is_currently_noop_ok(ctx: &mut SmokeContext) -> Result<()> {
    let reply: String = ctx.exec(&["UNWATCH"])?;
    assert_eq!(reply, "OK");
    Ok(())
}

fn unwatch_rejects_wrong_arity(ctx: &mut SmokeContext) -> Result<()> {
    let err = ctx.exec_error(&["UNWATCH", "extra"])?;
    assert!(err.to_string().contains("wrong number of arguments"));
    Ok(())
}

fn unwatch_clears_watches(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("unwatch:key", "v1")?;
    ctx.assert_ok(&["WATCH", "unwatch:key"])?;

    let reply: String = ctx.exec(&["UNWATCH"])?;
    assert_eq!(reply, "OK");

    let mut other = SmokeContext::connect(ctx.server_url())?;
    other.set("unwatch:key", "v2")?;

    ctx.assert_ok(&["MULTI"])?;
    let queued: String = ctx.exec(&["SET", "unwatch:applied", "yes"])?;
    assert_eq!(queued, "QUEUED");
    let replies: Vec<redis::Value> = ctx.exec(&["EXEC"])?;
    assert_eq!(replies.len(), 1);
    assert_eq!(ctx.get("unwatch:applied")?, Some("yes".to_string()));
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new(
        "UNWATCH",
        CommandGroup::Transaction,
        SupportLevel::Supported,
    )
    .summary("Clears watched keys for the current connection.")
    .syntax(&["UNWATCH"])
    .tested(&[
        "OK without active watches",
        "Wrong arity",
        "Clears watched keys",
    ])
    .case(CaseDef::new(
        "returns ok",
        "UNWATCH should return OK even when no keys are watched.",
        unwatch_is_currently_noop_ok,
    ))
    .case(CaseDef::new(
        "rejects wrong arity",
        "UNWATCH should reject unexpected arguments.",
        unwatch_rejects_wrong_arity,
    ))
    .case(CaseDef::new(
        "clears watches",
        "UNWATCH should prevent a later external write from aborting EXEC.",
        unwatch_clears_watches,
    ))
}
