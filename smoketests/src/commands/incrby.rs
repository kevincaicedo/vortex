use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn increments_by_positive_delta(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("counter", "3")?;
    let value: i64 = ctx.exec(&["INCRBY", "counter", "9"])?;
    assert_eq!(value, 12);
    Ok(())
}

fn accepts_negative_delta(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("counter", "12")?;
    let value: i64 = ctx.exec(&["INCRBY", "counter", "-5"])?;
    assert_eq!(value, 7);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("INCRBY", CommandGroup::String, SupportLevel::Supported)
        .summary("Increments an integer string by an explicit amount.")
        .syntax(&["INCRBY key increment"])
        .tested(&["Positive increment", "Negative increment"])
        .not_tested(&["Overflow behavior"])
        .case(CaseDef::new(
            "positive delta",
            "INCRBY should add the provided integer delta.",
            increments_by_positive_delta,
        ))
        .case(CaseDef::new(
            "negative delta",
            "INCRBY should accept signed deltas like Redis.",
            accepts_negative_delta,
        ))
}
