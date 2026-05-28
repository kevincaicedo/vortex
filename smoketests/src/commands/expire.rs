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

fn option_variants_are_routed(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("expire:nx", "token")?;
    assert_eq!(ctx.exec::<i64>(&["EXPIRE", "expire:nx", "60", "NX"])?, 1);
    assert_eq!(ctx.exec::<i64>(&["EXPIRE", "expire:nx", "90", "NX"])?, 0);

    ctx.set("expire:xx", "token")?;
    assert_eq!(ctx.exec::<i64>(&["EXPIRE", "expire:xx", "60", "XX"])?, 0);
    assert_eq!(ctx.exec::<i64>(&["EXPIRE", "expire:xx", "60"])?, 1);
    assert_eq!(ctx.exec::<i64>(&["EXPIRE", "expire:xx", "90", "XX"])?, 1);

    ctx.set("expire:gtlt", "token")?;
    assert_eq!(ctx.exec::<i64>(&["EXPIRE", "expire:gtlt", "60"])?, 1);
    assert_eq!(ctx.exec::<i64>(&["EXPIRE", "expire:gtlt", "30", "GT"])?, 0);
    assert_eq!(ctx.exec::<i64>(&["EXPIRE", "expire:gtlt", "90", "GT"])?, 1);
    assert_eq!(ctx.exec::<i64>(&["EXPIRE", "expire:gtlt", "120", "LT"])?, 0);
    assert_eq!(ctx.exec::<i64>(&["EXPIRE", "expire:gtlt", "30", "LT"])?, 1);
    Ok(())
}

fn conflicting_options_are_rejected(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("expire:conflict", "token")?;
    let err = ctx.exec_error(&["EXPIRE", "expire:conflict", "60", "NX", "XX"])?;
    assert!(err.to_string().contains("syntax"));
    assert_eq!(ctx.ttl("expire:conflict")?, -1);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("EXPIRE", CommandGroup::Key, SupportLevel::Supported)
        .summary("Sets a second-based expiration time on an existing key.")
        .syntax(&["EXPIRE key seconds [NX | XX | GT | LT]"])
        .tested(&[
            "Basic EXPIRE success path",
            "Missing key returns zero",
            "NX / XX / GT / LT option routing",
            "Conflicting condition options fail closed",
        ])
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
        .case(CaseDef::new(
            "condition options are routed",
            "EXPIRE should accept NX, XX, GT, and LT through the normal client dispatch path.",
            option_variants_are_routed,
        ))
        .case(CaseDef::new(
            "conflicting options are rejected",
            "EXPIRE should reject multiple condition options without changing the key TTL.",
            conflicting_options_are_rejected,
        ))
}
