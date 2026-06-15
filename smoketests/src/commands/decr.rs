use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn decrements_missing_key_from_zero(ctx: &mut SmokeContext) -> Result<()> {
    let value: i64 = ctx.exec(&["DECR", "counter"])?;
    assert_eq!(value, -1);
    assert_eq!(ctx.get("counter")?, Some("-1".to_string()));
    Ok(())
}

fn decrements_existing_integer(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("counter", "5")?;
    let value: i64 = ctx.exec(&["DECR", "counter"])?;
    assert_eq!(value, 4);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("DECR", CommandGroup::String, SupportLevel::Supported)
        .summary("Decrements an integer string by one.")
        .syntax(&["DECR key"])
        .tested(&[
            "Missing key initializes to zero",
            "Existing integer decrements",
        ])
        .not_tested(&["Overflow behavior"])
        .case(CaseDef::new(
            "missing key starts at zero",
            "DECR should create a missing key with value -1.",
            decrements_missing_key_from_zero,
        ))
        .case(CaseDef::new(
            "existing integer decrements",
            "DECR should subtract one from an existing integer value.",
            decrements_existing_integer,
        ))
}
