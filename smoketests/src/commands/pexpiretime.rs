use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn reports_missing_and_persistent_states(ctx: &mut SmokeContext) -> Result<()> {
    assert_eq!(ctx.exec::<i64>(&["PEXPIRETIME", "missing"])?, -2);
    ctx.set("persistent", "v")?;
    assert_eq!(ctx.exec::<i64>(&["PEXPIRETIME", "persistent"])?, -1);
    Ok(())
}

fn reports_absolute_deadline(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("session", "token")?;
    let expected = 4_102_444_800_000;
    let _: i64 = ctx.exec(&["PEXPIREAT", "session", "4102444800000"])?;
    let actual = ctx.exec::<i64>(&["PEXPIRETIME", "session"])?;
    assert!(actual == expected || actual == expected - 1);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("PEXPIRETIME", CommandGroup::Key, SupportLevel::Supported)
        .summary("Returns the absolute millisecond expiration deadline for a key.")
        .syntax(&["PEXPIRETIME key"])
        .tested(&[
            "Missing and persistent sentinel values",
            "Absolute deadline reporting",
        ])
        .not_tested(&[])
        .case(CaseDef::new(
            "reports sentinel states",
            "PEXPIRETIME should return -2 for missing keys and -1 for persistent keys.",
            reports_missing_and_persistent_states,
        ))
        .case(CaseDef::new(
            "reports absolute deadline",
            "PEXPIRETIME should return the stored absolute millisecond deadline.",
            reports_absolute_deadline,
        ))
}
