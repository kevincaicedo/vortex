use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn renames_existing_key(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("old", "value")?;
    ctx.assert_ok(&["RENAME", "old", "new"])?;
    assert_eq!(ctx.get("old")?, None);
    assert_eq!(ctx.get("new")?, Some("value".to_string()));
    Ok(())
}

fn missing_source_returns_error(ctx: &mut SmokeContext) -> Result<()> {
    let err = ctx.exec_error(&["RENAME", "missing", "new"])?;
    assert!(err.to_string().to_ascii_lowercase().contains("no such key"));
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("RENAME", CommandGroup::Key, SupportLevel::Supported)
        .summary("Renames a key, overwriting the destination if it already exists.")
        .syntax(&["RENAME key newkey"])
        .tested(&["Successful rename", "Missing source error"])
        .not_tested(&["Destination overwrite with existing TTL"])
        .case(CaseDef::new(
            "renames existing key",
            "RENAME should move the value to the new key name.",
            renames_existing_key,
        ))
        .case(CaseDef::new(
            "missing source errors",
            "RENAME should return an error when the source key is absent.",
            missing_source_returns_error,
        ))
}
