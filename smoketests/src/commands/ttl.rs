use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn reports_sentinel_values(ctx: &mut SmokeContext) -> Result<()> {
    assert_eq!(ctx.exec::<i64>(&["TTL", "missing"])?, -2);
    ctx.set("persistent", "v")?;
    assert_eq!(ctx.exec::<i64>(&["TTL", "persistent"])?, -1);
    Ok(())
}

fn reports_positive_second_ttl(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["SET", "session", "token", "EX", "60"])?;
    let ttl = ctx.exec::<i64>(&["TTL", "session"])?;
    assert!(ttl > 0 && ttl <= 60);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("TTL", CommandGroup::Key, SupportLevel::Supported)
        .summary("Returns the remaining TTL of a key in seconds.")
        .syntax(&["TTL key"])
        .tested(&["Sentinel states", "Positive TTL for expiring keys"])
        .not_tested(&[])
        .case(CaseDef::new(
            "reports sentinel states",
            "TTL should return -2 for missing keys and -1 for persistent keys.",
            reports_sentinel_values,
        ))
        .case(CaseDef::new(
            "reports positive ttl",
            "TTL should return a positive second countdown for expiring keys.",
            reports_positive_second_ttl,
        ))
}
