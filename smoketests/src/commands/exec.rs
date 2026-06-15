use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn exec_errors_without_multi(ctx: &mut SmokeContext) -> Result<()> {
    let err = ctx.exec_error(&["EXEC"])?;
    assert!(err.to_string().contains("EXEC without MULTI"));
    Ok(())
}

fn exec_rejects_wrong_arity(ctx: &mut SmokeContext) -> Result<()> {
    let err = ctx.exec_error(&["EXEC", "extra"])?;
    assert!(err.to_string().contains("wrong number of arguments"));
    Ok(())
}

fn exec_runs_queued_commands(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["MULTI"])?;
    let queued: String = ctx.exec(&["SET", "exec:key", "value"])?;
    assert_eq!(queued, "QUEUED");
    let queued: String = ctx.exec(&["GET", "exec:key"])?;
    assert_eq!(queued, "QUEUED");

    let replies: Vec<redis::Value> = ctx.exec(&["EXEC"])?;
    assert_eq!(replies.len(), 2);
    assert_eq!(ctx.get("exec:key")?, Some("value".to_string()));
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("EXEC", CommandGroup::Transaction, SupportLevel::Supported)
        .summary("Executes a queued transaction and errors when no transaction is active.")
        .syntax(&["EXEC"])
        .tested(&[
            "Error without MULTI",
            "Wrong arity",
            "Executes queued commands in order",
        ])
        .case(CaseDef::new(
            "exec errors without multi",
            "EXEC should return an error when no transaction is active.",
            exec_errors_without_multi,
        ))
        .case(CaseDef::new(
            "exec rejects wrong arity",
            "EXEC should reject unexpected arguments.",
            exec_rejects_wrong_arity,
        ))
        .case(CaseDef::new(
            "exec runs queued commands",
            "EXEC should execute queued transaction commands and publish their replies.",
            exec_runs_queued_commands,
        ))
}
