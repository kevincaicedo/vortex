use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn clears_existing_expiry(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["SET", "session", "token", "EX", "60"])?;
    let cleared: i64 = ctx.exec(&["PERSIST", "session"])?;
    assert_eq!(cleared, 1);
    assert_eq!(ctx.ttl("session")?, -1);
    Ok(())
}

fn persistent_key_returns_zero(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("session", "token")?;
    let cleared: i64 = ctx.exec(&["PERSIST", "session"])?;
    assert_eq!(cleared, 0);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("PERSIST", CommandGroup::Key, SupportLevel::Supported)
        .summary("Removes the expiration from a key.")
        .syntax(&["PERSIST key"])
        .tested(&["TTL removal", "Persistent key no-op"])
        .not_tested(&[])
        .case(CaseDef::new(
            "clears existing ttl",
            "PERSIST should remove an existing expiration and return 1.",
            clears_existing_expiry,
        ))
        .case(CaseDef::new(
            "persistent key returns zero",
            "PERSIST should return 0 when the key is already persistent.",
            persistent_key_returns_zero,
        ))
}
