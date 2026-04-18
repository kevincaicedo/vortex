use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn missing_key_returns_nil_then_sets_value(ctx: &mut SmokeContext) -> Result<()> {
    let previous: Option<String> = ctx.exec(&["GETSET", "mode", "on"])?;
    assert_eq!(previous, None);
    assert_eq!(ctx.get("mode")?, Some("on".to_string()));
    Ok(())
}

fn existing_key_returns_previous_value(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("mode", "on")?;
    let previous: Option<String> = ctx.exec(&["GETSET", "mode", "off"])?;
    assert_eq!(previous, Some("on".to_string()));
    assert_eq!(ctx.get("mode")?, Some("off".to_string()));
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("GETSET", CommandGroup::String, SupportLevel::Supported)
        .summary("Returns the old value of a key after replacing it.")
        .syntax(&["GETSET key value"])
        .tested(&[
            "Missing key semantics",
            "Previous value is returned when replacing",
        ])
        .not_tested(&["Wrong-type behavior"])
        .case(CaseDef::new(
            "missing key returns nil",
            "GETSET should return nil and create the key when it is missing.",
            missing_key_returns_nil_then_sets_value,
        ))
        .case(CaseDef::new(
            "existing key returns previous value",
            "GETSET should return the old value before writing the new one.",
            existing_key_returns_previous_value,
        ))
}
