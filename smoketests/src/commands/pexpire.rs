use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn sets_millisecond_expiry(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("session", "token")?;
    let applied: i64 = ctx.exec(&["PEXPIRE", "session", "60000"])?;
    assert_eq!(applied, 1);
    let pttl = ctx.pttl("session")?;
    assert!(pttl > 0 && pttl <= 60_000);
    Ok(())
}

fn option_variants_are_routed(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("pexpire:nx", "token")?;
    assert_eq!(
        ctx.exec::<i64>(&["PEXPIRE", "pexpire:nx", "60000", "NX"])?,
        1
    );
    assert_eq!(
        ctx.exec::<i64>(&["PEXPIRE", "pexpire:nx", "90000", "NX"])?,
        0
    );

    ctx.set("pexpire:xx", "token")?;
    assert_eq!(
        ctx.exec::<i64>(&["PEXPIRE", "pexpire:xx", "60000", "XX"])?,
        0
    );
    assert_eq!(ctx.exec::<i64>(&["PEXPIRE", "pexpire:xx", "60000"])?, 1);
    assert_eq!(
        ctx.exec::<i64>(&["PEXPIRE", "pexpire:xx", "90000", "XX"])?,
        1
    );

    ctx.set("pexpire:gtlt", "token")?;
    assert_eq!(ctx.exec::<i64>(&["PEXPIRE", "pexpire:gtlt", "60000"])?, 1);
    assert_eq!(
        ctx.exec::<i64>(&["PEXPIRE", "pexpire:gtlt", "30000", "GT"])?,
        0
    );
    assert_eq!(
        ctx.exec::<i64>(&["PEXPIRE", "pexpire:gtlt", "90000", "GT"])?,
        1
    );
    assert_eq!(
        ctx.exec::<i64>(&["PEXPIRE", "pexpire:gtlt", "120000", "LT"])?,
        0
    );
    assert_eq!(
        ctx.exec::<i64>(&["PEXPIRE", "pexpire:gtlt", "30000", "LT"])?,
        1
    );
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("PEXPIRE", CommandGroup::Key, SupportLevel::Supported)
        .summary("Sets a millisecond expiration time on an existing key.")
        .syntax(&["PEXPIRE key milliseconds [NX | XX | GT | LT]"])
        .tested(&[
            "Basic PEXPIRE success path",
            "NX / XX / GT / LT option routing",
        ])
        .case(CaseDef::new(
            "sets millisecond ttl",
            "PEXPIRE should assign a positive PTTL to an existing key.",
            sets_millisecond_expiry,
        ))
        .case(CaseDef::new(
            "condition options are routed",
            "PEXPIRE should accept NX, XX, GT, and LT through the normal client dispatch path.",
            option_variants_are_routed,
        ))
}
