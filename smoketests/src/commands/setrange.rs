use anyhow::Result;
use std::{thread, time::Duration};

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn overwrites_in_place(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("greeting", "hello world")?;
    let len: i64 = ctx.exec(&["SETRANGE", "greeting", "6", "redis"])?;
    assert_eq!(len, 11);
    assert_eq!(ctx.get("greeting")?, Some("hello redis".to_string()));
    Ok(())
}

fn pads_missing_key_with_zero_bytes(ctx: &mut SmokeContext) -> Result<()> {
    let len: i64 = ctx.exec(&["SETRANGE", "blob", "3", "x"])?;
    assert_eq!(len, 4);
    let value: Option<Vec<u8>> = ctx.exec(&["GET", "blob"])?;
    assert_eq!(value, Some(vec![0, 0, 0, b'x']));
    Ok(())
}

fn preserves_existing_ttl(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("ttlrange", "hello")?;
    let applied: i64 = ctx.exec(&["PEXPIRE", "ttlrange", "100"])?;
    assert_eq!(applied, 1);

    let before = ctx.pttl("ttlrange")?;
    assert!(before > 0);

    let len: i64 = ctx.exec(&["SETRANGE", "ttlrange", "1", "a"])?;
    assert_eq!(len, 5);
    let after = ctx.pttl("ttlrange")?;
    assert!(after > 0);
    assert_eq!(ctx.get("ttlrange")?, Some("hallo".to_string()));

    thread::sleep(Duration::from_millis(150));
    assert_eq!(ctx.get("ttlrange")?, None);
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("SETRANGE", CommandGroup::String, SupportLevel::Supported)
        .summary("Overwrites a substring in-place at a given offset.")
        .syntax(&["SETRANGE key offset value"])
        .tested(&[
            "Overwrite existing bytes",
            "Zero-padding on missing key",
            "TTL preservation on existing key",
        ])
        .not_tested(&["Very large offsets"])
        .case(CaseDef::new(
            "overwrites in place",
            "SETRANGE should replace bytes without truncating the string.",
            overwrites_in_place,
        ))
        .case(CaseDef::new(
            "pads missing key with zero bytes",
            "SETRANGE should zero-pad a missing key like Redis.",
            pads_missing_key_with_zero_bytes,
        ))
        .case(CaseDef::new(
            "preserves ttl",
            "SETRANGE should preserve the existing TTL on the rewritten key.",
            preserves_existing_ttl,
        ))
}
