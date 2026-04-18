use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn writes_value_with_millisecond_ttl(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["PSETEX", "job", "60000", "queued"])?;
    assert_eq!(ctx.get("job")?, Some("queued".to_string()));
    let pttl = ctx.pttl("job")?;
    assert!(pttl > 0 && pttl <= 60_000);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("PSETEX", CommandGroup::String, SupportLevel::Supported)
        .summary("Sets a string key with a millisecond TTL.")
        .syntax(&["PSETEX key milliseconds value"])
        .tested(&["Value write", "Millisecond TTL assignment"])
        .not_tested(&["Immediate expiry edge cases"])
        .case(CaseDef::new(
            "stores value with millisecond ttl",
            "PSETEX should write the value and assign a positive PTTL.",
            writes_value_with_millisecond_ttl,
        ))
}
