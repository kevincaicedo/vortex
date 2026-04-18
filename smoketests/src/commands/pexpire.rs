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

pub fn spec() -> CommandSpec {
    CommandSpec::new("PEXPIRE", CommandGroup::Key, SupportLevel::Supported)
        .summary("Sets a millisecond expiration time on an existing key.")
        .syntax(&["PEXPIRE key milliseconds [NX | XX | GT | LT]"])
        .tested(&["Basic PEXPIRE success path"])
        .not_tested(&["Option variants NX / XX / GT / LT"])
        .case(CaseDef::new(
            "sets millisecond ttl",
            "PEXPIRE should assign a positive PTTL to an existing key.",
            sets_millisecond_expiry,
        ))
}
