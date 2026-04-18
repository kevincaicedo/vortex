use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn stores_future_unix_millis_deadline(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("session", "token")?;
    let applied: i64 = ctx.exec(&["PEXPIREAT", "session", "4102444800000"])?;
    assert_eq!(applied, 1);
    let deadline: i64 = ctx.exec(&["PEXPIRETIME", "session"])?;
    assert_eq!(deadline, 4_102_444_800_000);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("PEXPIREAT", CommandGroup::Key, SupportLevel::Supported)
        .summary("Sets an absolute millisecond expiration deadline.")
        .syntax(&["PEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT]"])
        .tested(&["Future absolute millisecond deadline"])
        .not_tested(&["Option variants NX / XX / GT / LT"])
        .case(CaseDef::new(
            "stores future unix millisecond deadline",
            "PEXPIREAT should persist the exact absolute millisecond deadline.",
            stores_future_unix_millis_deadline,
        ))
}
