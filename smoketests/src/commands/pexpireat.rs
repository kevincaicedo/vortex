use anyhow::{Result, ensure};

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn stores_future_unix_millis_deadline(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("session", "token")?;
    let deadline = 4_102_444_800_000i64;
    let deadline_str = deadline.to_string();
    let applied: i64 = ctx.exec(&["PEXPIREAT", "session", &deadline_str])?;
    assert_eq!(applied, 1);
    let deadline: i64 = ctx.exec(&["PEXPIRETIME", "session"])?;
    assert_eq!(deadline, deadline_str.parse::<i64>()?);
    Ok(())
}

fn stores_dynamic_unix_millis_deadline(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("session", "token")?;
    let (secs, usecs): (String, String) = ctx.exec(&["TIME"])?;
    let deadline = (secs.parse::<i64>()? * 1000) + (usecs.parse::<i64>()? / 1000) + 60_000;
    let deadline_str = deadline.to_string();
    let applied: i64 = ctx.exec(&["PEXPIREAT", "session", &deadline_str])?;
    assert_eq!(applied, 1);

    let pttl = ctx.pttl("session")?;
    ensure!(
        pttl > 0 && pttl <= 60_000,
        "PEXPIREAT PTTL out of range: secs={secs}, usecs={usecs}, deadline={deadline}, pttl={pttl}"
    );

    let reported_deadline: i64 = ctx.exec(&["PEXPIRETIME", "session"])?;
    assert!(reported_deadline == deadline || reported_deadline == deadline - 1);
    Ok(())
}

fn option_variants_are_routed(ctx: &mut SmokeContext) -> Result<()> {
    let (secs, usecs): (String, String) = ctx.exec(&["TIME"])?;
    let now_ms = (secs.parse::<i64>()? * 1000) + (usecs.parse::<i64>()? / 1000);
    let plus_30 = (now_ms + 30_000).to_string();
    let plus_60 = (now_ms + 60_000).to_string();
    let plus_90 = (now_ms + 90_000).to_string();
    let plus_120 = (now_ms + 120_000).to_string();

    ctx.set("pexpireat:nx", "token")?;
    assert_eq!(
        ctx.exec::<i64>(&["PEXPIREAT", "pexpireat:nx", &plus_60, "NX"])?,
        1
    );
    assert_eq!(
        ctx.exec::<i64>(&["PEXPIREAT", "pexpireat:nx", &plus_90, "NX"])?,
        0
    );

    ctx.set("pexpireat:xx", "token")?;
    assert_eq!(
        ctx.exec::<i64>(&["PEXPIREAT", "pexpireat:xx", &plus_60, "XX"])?,
        0
    );
    assert_eq!(
        ctx.exec::<i64>(&["PEXPIREAT", "pexpireat:xx", &plus_60])?,
        1
    );
    assert_eq!(
        ctx.exec::<i64>(&["PEXPIREAT", "pexpireat:xx", &plus_90, "XX"])?,
        1
    );

    ctx.set("pexpireat:gtlt", "token")?;
    assert_eq!(
        ctx.exec::<i64>(&["PEXPIREAT", "pexpireat:gtlt", &plus_60])?,
        1
    );
    assert_eq!(
        ctx.exec::<i64>(&["PEXPIREAT", "pexpireat:gtlt", &plus_30, "GT"])?,
        0
    );
    assert_eq!(
        ctx.exec::<i64>(&["PEXPIREAT", "pexpireat:gtlt", &plus_90, "GT"])?,
        1
    );
    assert_eq!(
        ctx.exec::<i64>(&["PEXPIREAT", "pexpireat:gtlt", &plus_120, "LT"])?,
        0
    );
    assert_eq!(
        ctx.exec::<i64>(&["PEXPIREAT", "pexpireat:gtlt", &plus_30, "LT"])?,
        1
    );
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("PEXPIREAT", CommandGroup::Key, SupportLevel::Supported)
        .summary("Sets an absolute millisecond expiration deadline.")
        .syntax(&["PEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT]"])
        .tested(&[
            "Future absolute millisecond deadline",
            "Dynamic absolute millisecond deadline from TIME",
            "NX / XX / GT / LT option routing",
        ])
        .case(CaseDef::new(
            "stores future unix millisecond deadline",
            "PEXPIREAT should persist the exact absolute millisecond deadline.",
            stores_future_unix_millis_deadline,
        ))
        .case(CaseDef::new(
            "stores dynamic unix millisecond deadline",
            "PEXPIREAT should apply a positive PTTL and PEXPIRETIME should report the requested deadline.",
            stores_dynamic_unix_millis_deadline,
        ))
        .case(CaseDef::new(
            "condition options are routed",
            "PEXPIREAT should accept NX, XX, GT, and LT through the normal client dispatch path.",
            option_variants_are_routed,
        ))
}
