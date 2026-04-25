use anyhow::{Result, ensure};

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn plain_set_round_trip(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["SET", "name", "vortex"])?;
    assert_eq!(ctx.get("name")?, Some("vortex".to_string()));
    Ok(())
}

fn set_supports_nx_xx_and_get(ctx: &mut SmokeContext) -> Result<()> {
    let created: Option<String> = ctx.exec(&["SET", "flag", "one", "NX", "GET"])?;
    assert_eq!(created, None);

    let not_set: Option<String> = ctx.exec(&["SET", "flag", "two", "NX", "GET"])?;
    assert_eq!(not_set, Some("one".to_string()));
    assert_eq!(ctx.get("flag")?, Some("one".to_string()));

    let replaced: Option<String> = ctx.exec(&["SET", "flag", "three", "XX", "GET"])?;
    assert_eq!(replaced, Some("one".to_string()));
    assert_eq!(ctx.get("flag")?, Some("three".to_string()));
    Ok(())
}

fn set_supports_expiry_options(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["SET", "expiring", "v", "EX", "60"])?;
    let ttl = ctx.ttl("expiring")?;
    assert!(ttl > 0 && ttl <= 60);
    Ok(())
}

fn set_supports_absolute_expiry(ctx: &mut SmokeContext) -> Result<()> {
    let (now_secs, _): (String, String) = ctx.exec(&["TIME"])?;
    let exat = (now_secs.parse::<u64>()? + 60).to_string();
    ctx.assert_ok(&["SET", "abs-expiring", "v", "EXAT", &exat])?;
    let ttl = ctx.ttl("abs-expiring")?;
    ensure!(
        ttl > 0 && ttl <= 60,
        "EXAT TTL out of range: now_secs={now_secs}, exat={exat}, ttl={ttl}"
    );

    let (secs, usecs): (String, String) = ctx.exec(&["TIME"])?;
    let pxat = (secs.parse::<u64>()? * 1000) + (usecs.parse::<u64>()? / 1000) + 60_000;
    let pxat_str = pxat.to_string();
    ctx.assert_ok(&["SET", "abs-expiring-ms", "v", "PXAT", &pxat_str])?;
    let pttl = ctx.pttl("abs-expiring-ms")?;
    ensure!(
        pttl > 0 && pttl <= 60_000,
        "PXAT PTTL out of range: secs={secs}, usecs={usecs}, pxat={pxat}, pttl={pttl}"
    );
    Ok(())
}

fn set_supports_keepttl(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["SET", "sticky", "v1", "EX", "60"])?;
    let before = ctx.ttl("sticky")?;
    assert!(before > 0 && before <= 60);

    ctx.assert_ok(&["SET", "sticky", "v2", "KEEPTTL"])?;
    assert_eq!(ctx.get("sticky")?, Some("v2".to_string()));
    let after = ctx.ttl("sticky")?;
    assert!(after > 0 && after <= before);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("SET", CommandGroup::String, SupportLevel::Supported)
        .summary("Writes a string value with Redis-compatible option handling.")
        .syntax(&[
            "SET key value [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL] [NX | XX] [GET]",
        ])
        .tested(&[
            "Basic write path",
            "NX / XX / GET option interaction",
            "Relative EX expiry",
            "Absolute EXAT / PXAT expiry",
            "KEEPTTL preserves existing expiry",
        ])
        .not_tested(&[])
        .case(CaseDef::new(
            "plain set round trip",
            "SET should write the provided bytes.",
            plain_set_round_trip,
        ))
        .case(CaseDef::new(
            "supports nx xx get",
            "SET should match Redis semantics for NX/XX/GET.",
            set_supports_nx_xx_and_get,
        ))
        .case(CaseDef::new(
            "supports ex ttl",
            "SET EX should apply a positive TTL.",
            set_supports_expiry_options,
        ))
        .case(CaseDef::new(
            "supports exat and pxat",
            "SET EXAT/PXAT should accept absolute deadlines and apply a positive TTL.",
            set_supports_absolute_expiry,
        ))
        .case(CaseDef::new(
            "supports keepttl",
            "SET KEEPTTL should preserve an existing expiration while replacing the value.",
            set_supports_keepttl,
        ))
}
