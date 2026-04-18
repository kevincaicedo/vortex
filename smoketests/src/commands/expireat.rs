use anyhow::Result;

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

pub fn spec() -> CommandSpec {
    CommandSpec::new("EXPIREAT", CommandGroup::Key, SupportLevel::Supported)
        .summary("Sets an absolute second-based expiration deadline.")
        .syntax(&["EXPIREAT key unix-time-seconds [NX | XX | GT | LT]"])
        .tested(&["Future absolute expiry is stored exactly"])
        .not_tested(&["Option variants NX / XX / GT / LT"])
        .case(CaseDef::new(
            "stores future unix second deadline",
            "EXPIREAT should persist the exact absolute second deadline.",
            stores_future_unix_seconds_deadline,
        ))
}
