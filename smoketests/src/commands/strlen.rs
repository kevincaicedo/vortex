use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn returns_length_of_existing_string(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("greeting", "hello")?;
    let len: i64 = ctx.exec(&["STRLEN", "greeting"])?;
    assert_eq!(len, 5);
    Ok(())
}

fn missing_key_has_zero_length(ctx: &mut SmokeContext) -> Result<()> {
    let len: i64 = ctx.exec(&["STRLEN", "missing"])?;
    assert_eq!(len, 0);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("STRLEN", CommandGroup::String, SupportLevel::Supported)
        .summary("Returns the byte length of a string value.")
        .syntax(&["STRLEN key"])
        .tested(&["Existing string length", "Missing key length is zero"])
        .not_tested(&["Wrong-type behavior"])
        .case(CaseDef::new(
            "existing string length",
            "STRLEN should return the current byte length.",
            returns_length_of_existing_string,
        ))
        .case(CaseDef::new(
            "missing key length is zero",
            "STRLEN should return zero for a missing key.",
            missing_key_has_zero_length,
        ))
}
