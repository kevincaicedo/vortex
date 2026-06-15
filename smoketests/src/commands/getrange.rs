use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn returns_requested_slice(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("greeting", "hello world")?;
    let slice: String = ctx.exec(&["GETRANGE", "greeting", "0", "4"])?;
    assert_eq!(slice, "hello");
    Ok(())
}

fn supports_negative_indices(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("greeting", "hello world")?;
    let slice: String = ctx.exec(&["GETRANGE", "greeting", "-5", "-1"])?;
    assert_eq!(slice, "world");
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("GETRANGE", CommandGroup::String, SupportLevel::Supported)
        .summary("Returns a substring of the string stored at a key.")
        .syntax(&["GETRANGE key start end"])
        .tested(&["Positive bounds", "Negative bounds"])
        .not_tested(&["Out-of-range bounds against very large strings"])
        .case(CaseDef::new(
            "positive bounds",
            "GETRANGE should return the requested inclusive slice.",
            returns_requested_slice,
        ))
        .case(CaseDef::new(
            "negative bounds",
            "GETRANGE should support Redis-style negative offsets.",
            supports_negative_indices,
        ))
}
