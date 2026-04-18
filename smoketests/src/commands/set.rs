use anyhow::Result;

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
        ])
        .not_tested(&["EXAT, PXAT, and KEEPTTL combinations"])
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
}
