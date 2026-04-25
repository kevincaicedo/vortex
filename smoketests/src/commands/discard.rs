use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn discard_errors_without_multi(ctx: &mut SmokeContext) -> Result<()> {
    let err = ctx.exec_error(&["DISCARD"])?;
    assert!(err.to_string().contains("DISCARD without MULTI"));
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("DISCARD", CommandGroup::Server, SupportLevel::Supported)
        .summary("Discards a queued transaction and errors when no transaction is active.")
        .syntax(&["DISCARD"])
        .tested(&["Error without MULTI"])
        .not_tested(&["Queue clearing is covered by reactor unit tests"])
        .case(CaseDef::new(
            "discard errors without multi",
            "DISCARD should return an error when no transaction is active.",
            discard_errors_without_multi,
        ))
}
