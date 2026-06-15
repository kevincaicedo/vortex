use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn discard_errors_without_multi(ctx: &mut SmokeContext) -> Result<()> {
    let err = ctx.exec_error(&["DISCARD"])?;
    assert!(err.to_string().contains("DISCARD without MULTI"));
    Ok(())
}

fn discard_rejects_wrong_arity(ctx: &mut SmokeContext) -> Result<()> {
    let err = ctx.exec_error(&["DISCARD", "extra"])?;
    assert!(err.to_string().contains("wrong number of arguments"));
    Ok(())
}

fn discard_clears_queue(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["MULTI"])?;
    let queued: String = ctx.exec(&["SET", "discard:key", "value"])?;
    assert_eq!(queued, "QUEUED");
    ctx.assert_ok(&["DISCARD"])?;

    assert_eq!(ctx.get("discard:key")?, None);
    let err = ctx.exec_error(&["EXEC"])?;
    assert!(err.to_string().contains("EXEC without MULTI"));
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new(
        "DISCARD",
        CommandGroup::Transaction,
        SupportLevel::Supported,
    )
    .summary("Discards a queued transaction and errors when no transaction is active.")
    .syntax(&["DISCARD"])
    .tested(&["Error without MULTI", "Wrong arity", "Queue clearing"])
    .case(CaseDef::new(
        "discard errors without multi",
        "DISCARD should return an error when no transaction is active.",
        discard_errors_without_multi,
    ))
    .case(CaseDef::new(
        "discard rejects wrong arity",
        "DISCARD should reject unexpected arguments.",
        discard_rejects_wrong_arity,
    ))
    .case(CaseDef::new(
        "discard clears queued commands",
        "DISCARD should leave queued writes unapplied and exit MULTI mode.",
        discard_clears_queue,
    ))
}
