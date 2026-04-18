use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn returns_nil_for_missing_key(ctx: &mut SmokeContext) -> Result<()> {
    let value: Option<String> = ctx.exec(&["GET", "missing"])?;
    assert_eq!(value, None);
    Ok(())
}

fn returns_existing_value(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("hello", "world")?;
    let value: Option<String> = ctx.exec(&["GET", "hello"])?;
    assert_eq!(value, Some("world".to_string()));
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("GET", CommandGroup::String, SupportLevel::Supported)
        .summary("Returns the string value of a key.")
        .syntax(&["GET key"])
        .tested(&["Missing key returns nil", "Existing key returns its value"])
        .not_tested(&["Wrong-type behavior once additional data types ship"])
        .case(CaseDef::new(
            "missing key returns nil",
            "GET should return nil for a missing key.",
            returns_nil_for_missing_key,
        ))
        .case(CaseDef::new(
            "existing key returns value",
            "GET should return the stored bytes for an existing key.",
            returns_existing_value,
        ))
}
