use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn creates_missing_key(ctx: &mut SmokeContext) -> Result<()> {
    let len: i64 = ctx.exec(&["APPEND", "greeting", "hello"])?;
    assert_eq!(len, 5);
    assert_eq!(ctx.get("greeting")?, Some("hello".to_string()));
    Ok(())
}

fn appends_to_existing_value(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("greeting", "hello")?;
    let len: i64 = ctx.exec(&["APPEND", "greeting", " world"])?;
    assert_eq!(len, 11);
    assert_eq!(ctx.get("greeting")?, Some("hello world".to_string()));
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("APPEND", CommandGroup::String, SupportLevel::Supported)
        .summary("Appends to an existing string or creates the key if it is missing.")
        .syntax(&["APPEND key value"])
        .tested(&[
            "Missing key creation",
            "Appending to an existing inline string",
        ])
        .not_tested(&["Wrong-type behavior once non-string data types land"])
        .case(CaseDef::new(
            "creates missing key",
            "APPEND should create a missing key and return the new length.",
            creates_missing_key,
        ))
        .case(CaseDef::new(
            "appends existing value",
            "APPEND should preserve existing bytes and return the new length.",
            appends_to_existing_value,
        ))
}
