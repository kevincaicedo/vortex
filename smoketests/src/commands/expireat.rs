use anyhow::{Result, ensure};

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn stores_future_unix_seconds_deadline(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("session", "token")?;
    let deadline = "4102444800";
    let applied: i64 = ctx.exec(&["EXPIREAT", "session", deadline])?;
    assert_eq!(applied, 1);
    let deadline: i64 = ctx.exec(&["EXPIRETIME", "session"])?;
    assert_eq!(deadline, 4_102_444_800);
    Ok(())
}

fn stores_dynamic_unix_seconds_deadline(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("session", "token")?;
    let (now_secs, _): (String, String) = ctx.exec(&["TIME"])?;
    let deadline = (now_secs.parse::<i64>()? + 60).to_string();
    let applied: i64 = ctx.exec(&["EXPIREAT", "session", &deadline])?;
    assert_eq!(applied, 1);

    let ttl = ctx.ttl("session")?;
    ensure!(
        ttl > 0 && ttl <= 60,
        "EXPIREAT TTL out of range: now_secs={now_secs}, deadline={deadline}, ttl={ttl}"
    );

    let reported_deadline: i64 = ctx.exec(&["EXPIRETIME", "session"])?;
    assert_eq!(reported_deadline, deadline.parse::<i64>()?);
    Ok(())
}

fn option_variants_are_routed(ctx: &mut SmokeContext) -> Result<()> {
    let (now_secs, _): (String, String) = ctx.exec(&["TIME"])?;
    let now_secs = now_secs.parse::<i64>()?;
    let plus_30 = (now_secs + 30).to_string();
    let plus_60 = (now_secs + 60).to_string();
    let plus_90 = (now_secs + 90).to_string();
    let plus_120 = (now_secs + 120).to_string();

    ctx.set("expireat:nx", "token")?;
    assert_eq!(
        ctx.exec::<i64>(&["EXPIREAT", "expireat:nx", &plus_60, "NX"])?,
        1
    );
    assert_eq!(
        ctx.exec::<i64>(&["EXPIREAT", "expireat:nx", &plus_90, "NX"])?,
        0
    );

    ctx.set("expireat:xx", "token")?;
    assert_eq!(
        ctx.exec::<i64>(&["EXPIREAT", "expireat:xx", &plus_60, "XX"])?,
        0
    );
    assert_eq!(ctx.exec::<i64>(&["EXPIREAT", "expireat:xx", &plus_60])?, 1);
    assert_eq!(
        ctx.exec::<i64>(&["EXPIREAT", "expireat:xx", &plus_90, "XX"])?,
        1
    );

    ctx.set("expireat:gtlt", "token")?;
    assert_eq!(
        ctx.exec::<i64>(&["EXPIREAT", "expireat:gtlt", &plus_60])?,
        1
    );
    assert_eq!(
        ctx.exec::<i64>(&["EXPIREAT", "expireat:gtlt", &plus_30, "GT"])?,
        0
    );
    assert_eq!(
        ctx.exec::<i64>(&["EXPIREAT", "expireat:gtlt", &plus_90, "GT"])?,
        1
    );
    assert_eq!(
        ctx.exec::<i64>(&["EXPIREAT", "expireat:gtlt", &plus_120, "LT"])?,
        0
    );
    assert_eq!(
        ctx.exec::<i64>(&["EXPIREAT", "expireat:gtlt", &plus_30, "LT"])?,
        1
    );
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("EXPIREAT", CommandGroup::Key, SupportLevel::Supported)
        .summary("Sets an absolute second-based expiration deadline.")
        .syntax(&["EXPIREAT key unix-time-seconds [NX | XX | GT | LT]"])
        .tested(&[
            "Future absolute expiry is stored exactly",
            "Dynamic absolute expiry from TIME reports the same deadline",
            "NX / XX / GT / LT option routing",
        ])
        .case(CaseDef::new(
            "stores future unix second deadline",
            "EXPIREAT should persist the exact absolute second deadline.",
            stores_future_unix_seconds_deadline,
        ))
        .case(CaseDef::new(
            "stores dynamic unix second deadline",
            "EXPIREAT should apply a positive TTL and EXPIRETIME should report the requested deadline.",
            stores_dynamic_unix_seconds_deadline,
        ))
        .case(CaseDef::new(
            "condition options are routed",
            "EXPIREAT should accept NX, XX, GT, and LT through the normal client dispatch path.",
            option_variants_are_routed,
        ))
}
