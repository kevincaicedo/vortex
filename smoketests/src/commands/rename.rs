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

fn rename_preserves_ttl(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["SET", "ttl-src", "value", "EX", "60"])?;
    ctx.assert_ok(&["RENAME", "ttl-src", "ttl-dst"])?;
    assert!(ctx.ttl("ttl-src")? < 0);
    let ttl = ctx.ttl("ttl-dst")?;
    assert!(ttl > 0 && ttl <= 60);
    Ok(())
}

fn rename_overwrites_destination_and_replaces_ttl(ctx: &mut SmokeContext) -> Result<()> {
    ctx.assert_ok(&["SET", "src", "value", "EX", "60"])?;
    ctx.assert_ok(&["SET", "dst", "old", "EX", "5"])?;

    ctx.assert_ok(&["RENAME", "src", "dst"])?;
    assert_eq!(ctx.get("dst")?, Some("value".to_string()));
    assert_eq!(ctx.ttl("src")?, -2);
    let ttl = ctx.ttl("dst")?;
    assert!(ttl > 5 && ttl <= 60);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("RENAME", CommandGroup::Key, SupportLevel::Supported)
        .summary("Renames a key, overwriting the destination if it already exists.")
        .syntax(&["RENAME key newkey"])
        .tested(&[
            "Successful rename",
            "Missing source error",
            "TTL preservation on rename",
            "Destination overwrite replaces previous TTL",
        ])
        .not_tested(&[])
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
        .case(CaseDef::new(
            "preserves ttl",
            "RENAME should keep the source TTL on the destination key.",
            rename_preserves_ttl,
        ))
        .case(CaseDef::new(
            "overwrites destination ttl",
            "RENAME should overwrite an existing destination and replace its TTL with the source TTL.",
            rename_overwrites_destination_and_replaces_ttl,
        ))
}
