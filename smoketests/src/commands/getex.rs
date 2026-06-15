use anyhow::{Result, ensure};

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn getex_sets_relative_expiry(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("token", "abc")?;
    let value: Option<String> = ctx.exec(&["GETEX", "token", "EX", "60"])?;
    assert_eq!(value, Some("abc".to_string()));
    let ttl = ctx.ttl("token")?;
    assert!(ttl > 0 && ttl <= 60);
    Ok(())
}

fn getex_persist_clears_expiry(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["SET", "token", "abc", "EX", "60"])?;
    let value: Option<String> = ctx.exec(&["GETEX", "token", "PERSIST"])?;
    assert_eq!(value, Some("abc".to_string()));
    assert_eq!(ctx.ttl("token")?, -1);
    Ok(())
}

fn getex_sets_absolute_expiry(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("token", "abc")?;
    let (now_secs, _): (String, String) = ctx.exec(&["TIME"])?;
    let exat = (now_secs.parse::<u64>()? + 60).to_string();
    let value: Option<String> = ctx.exec(&["GETEX", "token", "EXAT", &exat])?;
    assert_eq!(value, Some("abc".to_string()));
    let ttl = ctx.ttl("token")?;
    ensure!(
        ttl > 0 && ttl <= 60,
        "GETEX EXAT TTL out of range: now_secs={now_secs}, exat={exat}, ttl={ttl}"
    );

    ctx.set("token-ms", "abc")?;
    let (secs, usecs): (String, String) = ctx.exec(&["TIME"])?;
    let pxat = (secs.parse::<u64>()? * 1000) + (usecs.parse::<u64>()? / 1000) + 60_000;
    let pxat_str = pxat.to_string();
    let value: Option<String> = ctx.exec(&["GETEX", "token-ms", "PXAT", &pxat_str])?;
    assert_eq!(value, Some("abc".to_string()));
    let pttl = ctx.pttl("token-ms")?;
    ensure!(
        pttl > 0 && pttl <= 60_000,
        "GETEX PXAT PTTL out of range: secs={secs}, usecs={usecs}, pxat={pxat}, pttl={pttl}"
    );
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("GETEX", CommandGroup::String, SupportLevel::Supported)
        .summary("Returns the string value of a key after updating its expiration.")
        .syntax(&[
            "GETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | PERSIST]",
        ])
        .tested(&[
            "Relative EX expiry",
            "Absolute EXAT / PXAT expiry",
            "PERSIST clears the TTL",
        ])
        .not_tested(&[])
        .case(CaseDef::new(
            "sets relative expiry",
            "GETEX EX should return the value and update TTL.",
            getex_sets_relative_expiry,
        ))
        .case(CaseDef::new(
            "sets absolute expiry",
            "GETEX EXAT/PXAT should return the value and apply a positive absolute TTL.",
            getex_sets_absolute_expiry,
        ))
        .case(CaseDef::new(
            "persist clears ttl",
            "GETEX PERSIST should return the value and remove expiry metadata.",
            getex_persist_clears_expiry,
        ))
}
