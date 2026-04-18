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

pub fn spec() -> CommandSpec {
    CommandSpec::new("FLUSHALL", CommandGroup::Server, SupportLevel::Supported)
        .summary("Clears the currently supported single database.")
        .syntax(&["FLUSHALL [ASYNC | SYNC]"])
        .tested(&["Current synchronous dataset clear"])
        .not_tested(&["ASYNC behavior because Vortex currently executes synchronously"])
        .case(CaseDef::new(
            "clears dataset",
            "FLUSHALL should remove all keys from the current Vortex dataset.",
            clears_current_dataset,
        ))
}
