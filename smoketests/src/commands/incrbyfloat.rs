use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn increments_existing_float(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("temperature", "10.5")?;
    let value: String = ctx.exec(&["INCRBYFLOAT", "temperature", "0.25"])?;
    assert_eq!(value, "10.75");
    assert_eq!(ctx.get("temperature")?, Some("10.75".to_string()));
    Ok(())
}

fn missing_key_uses_zero(ctx: &mut SmokeContext) -> Result<()> {
    let value: String = ctx.exec(&["INCRBYFLOAT", "temperature", "1.5"])?;
    assert_eq!(value, "1.5");
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("INCRBYFLOAT", CommandGroup::String, SupportLevel::Supported)
        .summary("Increments a floating-point string by the provided amount.")
        .syntax(&["INCRBYFLOAT key increment"])
        .tested(&["Existing float increment", "Missing key starts from zero"])
        .not_tested(&["NaN / infinity behavior"])
        .case(CaseDef::new(
            "existing float increments",
            "INCRBYFLOAT should update the stored decimal string.",
            increments_existing_float,
        ))
        .case(CaseDef::new(
            "missing key starts from zero",
            "INCRBYFLOAT should create a missing key when needed.",
            missing_key_uses_zero,
        ))
}
