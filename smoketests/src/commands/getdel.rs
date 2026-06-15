use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn returns_value_and_deletes_key(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("session", "token")?;
    let deleted: Option<String> = ctx.exec(&["GETDEL", "session"])?;
    assert_eq!(deleted, Some("token".to_string()));
    assert_eq!(ctx.get("session")?, None);
    Ok(())
}

fn missing_key_returns_nil(ctx: &mut SmokeContext) -> Result<()> {
    let deleted: Option<String> = ctx.exec(&["GETDEL", "missing"])?;
    assert_eq!(deleted, None);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("GETDEL", CommandGroup::String, SupportLevel::Supported)
        .summary("Returns the string value of a key after deleting it.")
        .syntax(&["GETDEL key"])
        .tested(&["Value is returned and deleted", "Missing key returns nil"])
        .not_tested(&["Wrong-type behavior"])
        .case(CaseDef::new(
            "returns value and deletes key",
            "GETDEL should return the value and remove the key in one step.",
            returns_value_and_deletes_key,
        ))
        .case(CaseDef::new(
            "missing key returns nil",
            "GETDEL should return nil when the key is absent.",
            missing_key_returns_nil,
        ))
}
