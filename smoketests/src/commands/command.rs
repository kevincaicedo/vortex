use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn command_count_is_positive(ctx: &mut SmokeContext) -> Result<()> {
    let count: i64 = ctx.exec(&["COMMAND", "COUNT"])?;
    assert!(count > 0);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("COMMAND", CommandGroup::Server, SupportLevel::Partial)
        .summary("Exposes limited command metadata for currently implemented commands.")
        .syntax(&[
            "COMMAND",
            "COMMAND COUNT",
            "COMMAND INFO command [command ...]",
        ])
        .tested(&["COMMAND COUNT returns a positive number"])
        .not_tested(&[
            "COMMAND DOCS and COMMAND LIST remain placeholder responses",
            "Full Redis metadata parity",
        ])
        .case(CaseDef::new(
            "command count is positive",
            "COMMAND COUNT should return the number of registered commands.",
            command_count_is_positive,
        ))
}
