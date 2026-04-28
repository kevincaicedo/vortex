use anyhow::Result;
use std::{thread, time::Duration};

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

fn preserves_existing_ttl(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("float:ttl", "1.25")?;
    let applied: i64 = ctx.exec(&["PEXPIRE", "float:ttl", "100"])?;
    assert_eq!(applied, 1);

    let value: String = ctx.exec(&["INCRBYFLOAT", "float:ttl", "0.25"])?;
    assert_eq!(value, "1.5");
    assert!(ctx.pttl("float:ttl")? > 0);

    thread::sleep(Duration::from_millis(150));
    assert_eq!(ctx.get("float:ttl")?, None);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("INCRBYFLOAT", CommandGroup::String, SupportLevel::Supported)
        .summary("Increments a floating-point string by the provided amount.")
        .syntax(&["INCRBYFLOAT key increment"])
        .tested(&[
            "Existing float increment",
            "Missing key starts from zero",
            "TTL preservation after increment",
        ])
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
        .case(CaseDef::new(
            "preserves ttl",
            "INCRBYFLOAT should preserve the TTL of an existing key.",
            preserves_existing_ttl,
        ))
}
