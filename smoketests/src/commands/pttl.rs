use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn reports_sentinel_values(ctx: &mut SmokeContext) -> Result<()> {
    assert_eq!(ctx.exec::<i64>(&["PTTL", "missing"])?, -2);
    ctx.set("persistent", "v")?;
    assert_eq!(ctx.exec::<i64>(&["PTTL", "persistent"])?, -1);
    Ok(())
}

fn reports_positive_millisecond_ttl(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["SET", "session", "token", "PX", "60000"])?;
    let pttl = ctx.exec::<i64>(&["PTTL", "session"])?;
    assert!(pttl > 0 && pttl <= 60_000);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("PTTL", CommandGroup::Key, SupportLevel::Supported)
        .summary("Returns the remaining TTL of a key in milliseconds.")
        .syntax(&["PTTL key"])
        .tested(&["Sentinel states", "Positive millisecond TTL"])
        .not_tested(&[])
        .case(CaseDef::new(
            "reports sentinel states",
            "PTTL should return -2 for missing keys and -1 for persistent keys.",
            reports_sentinel_values,
        ))
        .case(CaseDef::new(
            "reports positive ttl",
            "PTTL should return a positive millisecond countdown for expiring keys.",
            reports_positive_millisecond_ttl,
        ))
}
