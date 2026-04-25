use anyhow::{Result, ensure};

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn stores_future_unix_seconds_deadline(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("session", "token")?;
    let applied: i64 = ctx.exec(&["EXPIREAT", "session", "4102444800"])?;
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

pub fn spec() -> CommandSpec {
    CommandSpec::new("EXPIREAT", CommandGroup::Key, SupportLevel::Supported)
        .summary("Sets an absolute second-based expiration deadline.")
        .syntax(&["EXPIREAT key unix-time-seconds [NX | XX | GT | LT]"])
        .tested(&[
            "Future absolute expiry is stored exactly",
            "Dynamic absolute expiry from TIME reports the same deadline",
        ])
        .not_tested(&["Option variants NX / XX / GT / LT"])
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
}
