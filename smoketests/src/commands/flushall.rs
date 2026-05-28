use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn clears_current_dataset(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("a", "1")?;
    ctx.set("b", "2")?;
    ctx.assert_ok(&["FLUSHALL"])?;
    assert_eq!(ctx.dbsize()?, 0);
    Ok(())
}

fn accepts_sync_and_async_options(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("flushall:sync", "1")?;
    ctx.assert_ok(&["FLUSHALL", "SYNC"])?;
    assert_eq!(ctx.dbsize()?, 0);

    ctx.set("flushall:async", "1")?;
    ctx.assert_ok(&["FLUSHALL", "ASYNC"])?;
    assert_eq!(ctx.dbsize()?, 0);
    Ok(())
}

fn rejects_malformed_options_without_clearing(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("flushall:keep", "1")?;

    let err = ctx.exec_error(&["FLUSHALL", "eventually"])?;
    assert!(err.to_string().contains("syntax"));
    assert_eq!(ctx.dbsize()?, 1);

    let err = ctx.exec_error(&["FLUSHALL", "ASYNC", "extra"])?;
    assert!(err.to_string().contains("syntax"));
    assert_eq!(ctx.dbsize()?, 1);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("FLUSHALL", CommandGroup::Server, SupportLevel::Supported)
        .summary("Clears the currently supported single database.")
        .syntax(&["FLUSHALL [ASYNC | SYNC]"])
        .tested(&[
            "Current synchronous dataset clear",
            "SYNC and ASYNC option acceptance",
            "Malformed options rejected without clearing the dataset",
        ])
        .not_tested(&["ASYNC behavior because Vortex currently executes synchronously"])
        .case(CaseDef::new(
            "clears dataset",
            "FLUSHALL should remove all keys from the current Vortex dataset.",
            clears_current_dataset,
        ))
        .case(CaseDef::new(
            "sync async options",
            "FLUSHALL should accept SYNC and ASYNC option forms.",
            accepts_sync_and_async_options,
        ))
        .case(CaseDef::new(
            "malformed option validation",
            "FLUSHALL should reject malformed options before clearing keys.",
            rejects_malformed_options_without_clearing,
        ))
}
