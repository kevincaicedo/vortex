use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn watch_returns_stub_error(ctx: &mut SmokeContext) -> Result<()> {
    let err = ctx.exec_error(&["WATCH", "key"])?;
    assert!(
        err.to_string()
            .contains("MULTI/EXEC is not yet implemented")
    );
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("WATCH", CommandGroup::Server, SupportLevel::Stubbed)
        .summary("Currently behaves as a transaction stub and returns an error.")
        .syntax(&["WATCH key [key ...]"])
        .tested(&["Current stub error semantics"])
        .not_tested(&["Optimistic transaction semantics until WATCH support ships"])
        .case(CaseDef::new(
            "watch returns stub error",
            "WATCH should currently return the documented not-yet-implemented error.",
            watch_returns_stub_error,
        ))
}
