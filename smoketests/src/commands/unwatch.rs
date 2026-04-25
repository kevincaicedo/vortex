use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn unwatch_is_currently_noop_ok(ctx: &mut SmokeContext) -> Result<()> {
    let reply: String = ctx.exec(&["UNWATCH"])?;
    assert_eq!(reply, "OK");
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("UNWATCH", CommandGroup::Server, SupportLevel::Supported)
        .summary("Clears watched keys for the current connection.")
        .syntax(&["UNWATCH"])
        .tested(&["OK without active watches"])
        .not_tested(&["Watch clearing is covered by reactor unit tests"])
        .case(CaseDef::new(
            "returns ok",
            "UNWATCH should return OK even when no keys are watched.",
            unwatch_is_currently_noop_ok,
        ))
}
