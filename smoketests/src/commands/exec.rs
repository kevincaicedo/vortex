use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn exec_errors_without_multi(ctx: &mut SmokeContext) -> Result<()> {
    let err = ctx.exec_error(&["EXEC"])?;
    assert!(err.to_string().contains("EXEC without MULTI"));
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("EXEC", CommandGroup::Server, SupportLevel::Stubbed)
        .summary("Currently behaves as a transaction stub and errors without MULTI.")
        .syntax(&["EXEC"])
        .tested(&["Current stub error semantics"])
        .not_tested(&["Queued transaction execution until MULTI/EXEC ships"])
        .case(CaseDef::new(
            "exec errors without multi",
            "EXEC should currently return the documented stub error.",
            exec_errors_without_multi,
        ))
}
