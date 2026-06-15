use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn clears_current_dataset(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("a", "1")?;
    ctx.set("b", "2")?;
    ctx.assert_ok(&["FLUSHDB"])?;
    assert_eq!(ctx.dbsize()?, 0);
    Ok(())
}

fn accepts_sync_and_async_options(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("flushdb:sync", "1")?;
    ctx.assert_ok(&["FLUSHDB", "SYNC"])?;
    assert_eq!(ctx.dbsize()?, 0);

    ctx.set("flushdb:async", "1")?;
    ctx.assert_ok(&["FLUSHDB", "ASYNC"])?;
    assert_eq!(ctx.dbsize()?, 0);
    Ok(())
}

fn rejects_malformed_options_without_clearing(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("flushdb:keep", "1")?;

    let err = ctx.exec_error(&["FLUSHDB", "later"])?;
    assert!(err.to_string().contains("syntax"));
    assert_eq!(ctx.dbsize()?, 1);

    let err = ctx.exec_error(&["FLUSHDB", "SYNC", "extra"])?;
    assert!(err.to_string().contains("syntax"));
    assert_eq!(ctx.dbsize()?, 1);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("FLUSHDB", CommandGroup::Server, SupportLevel::Supported)
        .summary("Clears the current database.")
        .syntax(&["FLUSHDB [ASYNC | SYNC]"])
        .tested(&[
            "Current synchronous dataset clear",
            "SYNC and ASYNC option acceptance",
            "Malformed options rejected without clearing the dataset",
        ])
        .not_tested(&["ASYNC behavior because Vortex currently executes synchronously"])
        .case(CaseDef::new(
            "clears current dataset",
            "FLUSHDB should remove all keys from DB 0.",
            clears_current_dataset,
        ))
        .case(CaseDef::new(
            "sync async options",
            "FLUSHDB should accept SYNC and ASYNC option forms.",
            accepts_sync_and_async_options,
        ))
        .case(CaseDef::new(
            "malformed option validation",
            "FLUSHDB should reject malformed options before clearing keys.",
            rejects_malformed_options_without_clearing,
        ))
}
