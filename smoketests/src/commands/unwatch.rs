use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn unwatch_is_currently_noop_ok(ctx: &mut SmokeContext) -> Result<()> {
    let reply: String = ctx.exec(&["UNWATCH"])?;
    assert_eq!(reply, "OK");
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("UNWATCH", CommandGroup::Server, SupportLevel::Partial)
        .summary("Currently returns OK even though WATCH state is not implemented yet.")
        .syntax(&["UNWATCH"])
        .tested(&["Current no-op OK behavior"])
        .not_tested(&["Actual watched-key state clearing until WATCH support ships"])
        .case(CaseDef::new(
            "returns ok as no op",
            "UNWATCH should currently behave as a successful no-op.",
            unwatch_is_currently_noop_ok,
        ))
}
