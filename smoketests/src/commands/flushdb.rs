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

pub fn spec() -> CommandSpec {
    CommandSpec::new("FLUSHDB", CommandGroup::Server, SupportLevel::Supported)
        .summary("Clears the current database.")
        .syntax(&["FLUSHDB [ASYNC | SYNC]"])
        .tested(&["Current synchronous dataset clear"])
        .not_tested(&["ASYNC behavior because Vortex currently executes synchronously"])
        .case(CaseDef::new(
            "clears current dataset",
            "FLUSHDB should remove all keys from DB 0.",
            clears_current_dataset,
        ))
}
