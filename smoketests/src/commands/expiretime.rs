use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn reports_missing_and_persistent_states(ctx: &mut SmokeContext) -> Result<()> {
    assert_eq!(ctx.exec::<i64>(&["EXPIRETIME", "missing"])?, -2);
    ctx.set("persistent", "v")?;
    assert_eq!(ctx.exec::<i64>(&["EXPIRETIME", "persistent"])?, -1);
    Ok(())
}

fn reports_absolute_deadline(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("session", "token")?;
    let _: i64 = ctx.exec(&["EXPIREAT", "session", "4102444800"])?;
    assert_eq!(ctx.exec::<i64>(&["EXPIRETIME", "session"])?, 4_102_444_800);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("EXPIRETIME", CommandGroup::Key, SupportLevel::Supported)
        .summary("Returns the absolute second-based expiration deadline for a key.")
        .syntax(&["EXPIRETIME key"])
        .tested(&[
            "Missing and persistent sentinel values",
            "Absolute deadline reporting",
        ])
        .not_tested(&[])
        .case(CaseDef::new(
            "reports sentinel states",
            "EXPIRETIME should return -2 for missing keys and -1 for persistent keys.",
            reports_missing_and_persistent_states,
        ))
        .case(CaseDef::new(
            "reports absolute deadline",
            "EXPIRETIME should return the stored absolute second deadline.",
            reports_absolute_deadline,
        ))
}
