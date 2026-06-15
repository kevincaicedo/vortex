use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn writes_value_with_second_ttl(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["SETEX", "job", "60", "queued"])?;
    assert_eq!(ctx.get("job")?, Some("queued".to_string()));
    let ttl = ctx.ttl("job")?;
    assert!(ttl > 0 && ttl <= 60);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("SETEX", CommandGroup::String, SupportLevel::Supported)
        .summary("Sets a string key with a second-based TTL.")
        .syntax(&["SETEX key seconds value"])
        .tested(&["Value write", "Second TTL assignment"])
        .not_tested(&["Immediate expiry edge cases"])
        .case(CaseDef::new(
            "stores value with second ttl",
            "SETEX should write the value and assign a positive TTL.",
            writes_value_with_second_ttl,
        ))
}
