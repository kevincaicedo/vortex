use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn exec_errors_without_multi(ctx: &mut SmokeContext) -> Result<()> {
    let err = ctx.exec_error(&["EXEC"])?;
    assert!(err.to_string().contains("EXEC without MULTI"));
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("EXEC", CommandGroup::Server, SupportLevel::Supported)
        .summary("Executes a queued transaction and errors when no transaction is active.")
        .syntax(&["EXEC"])
        .tested(&["Error without MULTI"])
        .not_tested(&["Successful EXEC is covered by MULTI smoke and reactor unit tests"])
        .case(CaseDef::new(
            "exec errors without multi",
            "EXEC should return an error when no transaction is active.",
            exec_errors_without_multi,
        ))
}
