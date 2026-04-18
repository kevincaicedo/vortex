use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn sets_expiry_on_existing_key(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("session", "token")?;
    let applied: i64 = ctx.exec(&["EXPIRE", "session", "60"])?;
    assert_eq!(applied, 1);
    let ttl = ctx.ttl("session")?;
    assert!(ttl > 0 && ttl <= 60);
    Ok(())
}

fn missing_key_returns_zero(ctx: &mut SmokeContext) -> Result<()> {
    let applied: i64 = ctx.exec(&["EXPIRE", "missing", "60"])?;
    assert_eq!(applied, 0);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("EXPIRE", CommandGroup::Key, SupportLevel::Supported)
        .summary("Sets a second-based expiration time on an existing key.")
        .syntax(&["EXPIRE key seconds [NX | XX | GT | LT]"])
        .tested(&["Basic EXPIRE success path", "Missing key returns zero"])
        .not_tested(&["NX / XX / GT / LT option variants"])
        .case(CaseDef::new(
            "existing key gets ttl",
            "EXPIRE should assign a positive TTL to an existing key.",
            sets_expiry_on_existing_key,
        ))
        .case(CaseDef::new(
            "missing key returns zero",
            "EXPIRE should return 0 for a missing key.",
            missing_key_returns_zero,
        ))
}
