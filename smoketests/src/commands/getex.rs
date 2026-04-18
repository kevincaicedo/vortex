use anyhow::Result;

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

pub fn spec() -> CommandSpec {
    CommandSpec::new("GETEX", CommandGroup::String, SupportLevel::Supported)
        .summary("Returns the string value of a key after updating its expiration.")
        .syntax(&[
            "GETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | PERSIST]",
        ])
        .tested(&["Relative EX expiry", "PERSIST clears the TTL"])
        .not_tested(&["EXAT and PXAT variants"])
        .case(CaseDef::new(
            "sets relative expiry",
            "GETEX EX should return the value and update TTL.",
            getex_sets_relative_expiry,
        ))
        .case(CaseDef::new(
            "persist clears ttl",
            "GETEX PERSIST should return the value and remove expiry metadata.",
            getex_persist_clears_expiry,
        ))
}
