use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn multi_returns_stub_error(ctx: &mut SmokeContext) -> Result<()> {
    let err = ctx.exec_error(&["MULTI"])?;
    assert!(
        err.to_string()
            .contains("MULTI/EXEC is not yet implemented")
    );
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("MULTI", CommandGroup::Server, SupportLevel::Stubbed)
        .summary("Currently behaves as a transaction stub and returns an error.")
        .syntax(&["MULTI"])
        .tested(&["Current stub error semantics"])
        .not_tested(&["Queued transaction semantics until MULTI/EXEC ships"])
        .case(CaseDef::new(
            "multi returns stub error",
            "MULTI should currently return the documented not-yet-implemented error.",
            multi_returns_stub_error,
        ))
}
