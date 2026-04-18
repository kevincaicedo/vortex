use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn decrements_existing_integer_by_delta(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("counter", "10")?;
    let value: i64 = ctx.exec(&["DECRBY", "counter", "3"])?;
    assert_eq!(value, 7);
    Ok(())
}

fn accepts_negative_delta(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("counter", "7")?;
    let value: i64 = ctx.exec(&["DECRBY", "counter", "-2"])?;
    assert_eq!(value, 9);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("DECRBY", CommandGroup::String, SupportLevel::Supported)
        .summary("Decrements an integer string by an explicit amount.")
        .syntax(&["DECRBY key decrement"])
        .tested(&[
            "Positive decrement",
            "Negative decrement is treated as addition",
        ])
        .not_tested(&["Overflow behavior"])
        .case(CaseDef::new(
            "positive delta",
            "DECRBY should subtract the provided integer delta.",
            decrements_existing_integer_by_delta,
        ))
        .case(CaseDef::new(
            "negative delta",
            "DECRBY should accept signed deltas like Redis.",
            accepts_negative_delta,
        ))
}
