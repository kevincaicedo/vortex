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

pub fn spec() -> CommandSpec {
    CommandSpec::new("PEXPIREAT", CommandGroup::Key, SupportLevel::Supported)
        .summary("Sets an absolute millisecond expiration deadline.")
        .syntax(&["PEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT]"])
        .tested(&[
            "Future absolute millisecond deadline",
            "Dynamic absolute millisecond deadline from TIME",
        ])
        .not_tested(&["Option variants NX / XX / GT / LT"])
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
}
