use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn time_returns_two_numeric_strings(ctx: &mut SmokeContext) -> Result<()> {
    let (secs, usecs): (String, String) = ctx.exec(&["TIME"])?;
    assert!(secs.parse::<u64>().is_ok());
    let micros = usecs.parse::<u64>().unwrap();
    assert!(micros < 1_000_000);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("TIME", CommandGroup::Server, SupportLevel::Supported)
        .summary("Returns the current wall-clock time as seconds and microseconds.")
        .syntax(&["TIME"])
        .tested(&["Two-element numeric tuple response"])
        .not_tested(&["Monotonicity across rapid repeated calls"])
        .case(CaseDef::new(
            "returns numeric tuple",
            "TIME should return seconds and microseconds as decimal strings.",
            time_returns_two_numeric_strings,
        ))
}
