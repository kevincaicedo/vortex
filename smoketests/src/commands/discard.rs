use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn discard_errors_without_multi(ctx: &mut SmokeContext) -> Result<()> {
    let err = ctx.exec_error(&["DISCARD"])?;
    assert!(err.to_string().contains("DISCARD without MULTI"));
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("DISCARD", CommandGroup::Server, SupportLevel::Stubbed)
        .summary("Currently behaves as a transaction stub and errors without MULTI.")
        .syntax(&["DISCARD"])
        .tested(&["Current stub error semantics"])
        .not_tested(&["Real queued transaction semantics until MULTI/EXEC ships"])
        .case(CaseDef::new(
            "discard errors without multi",
            "DISCARD should currently return the documented stub error.",
            discard_errors_without_multi,
        ))
}
