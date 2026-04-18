use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn increments_missing_key_from_zero(ctx: &mut SmokeContext) -> Result<()> {
    let value: i64 = ctx.exec(&["INCR", "counter"])?;
    assert_eq!(value, 1);
    Ok(())
}

fn increments_existing_integer(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("counter", "7")?;
    let value: i64 = ctx.exec(&["INCR", "counter"])?;
    assert_eq!(value, 8);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("INCR", CommandGroup::String, SupportLevel::Supported)
        .summary("Increments an integer string by one.")
        .syntax(&["INCR key"])
        .tested(&["Missing key starts at zero", "Existing integer increments"])
        .not_tested(&["Overflow behavior"])
        .case(CaseDef::new(
            "missing key starts at zero",
            "INCR should create a missing key with value 1.",
            increments_missing_key_from_zero,
        ))
        .case(CaseDef::new(
            "existing integer increments",
            "INCR should add one to an existing integer string.",
            increments_existing_integer,
        ))
}
