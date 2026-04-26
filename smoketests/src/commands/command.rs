use std::collections::BTreeSet;

use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn value_as_array(value: &redis::Value) -> &[redis::Value] {
    match value {
        redis::Value::Array(values) => values,
        other => panic!("expected RESP array, got {other:?}"),
    }
}

fn value_as_text(value: &redis::Value) -> String {
    match value {
        redis::Value::BulkString(bytes) => {
            String::from_utf8(bytes.clone()).expect("command metadata should be valid UTF-8")
        }
        redis::Value::SimpleString(text) => text.clone(),
        redis::Value::Okay => "OK".to_string(),
        other => panic!("expected RESP string, got {other:?}"),
    }
}

fn command_count_matches_visible_surface(ctx: &mut SmokeContext) -> Result<()> {
    let count: i64 = ctx.exec(&["COMMAND", "COUNT"])?;
    let visible: redis::Value = ctx.exec(&["COMMAND"])?;
    let entries = value_as_array(&visible);
    assert_eq!(count as usize, entries.len());
    Ok(())
}

fn command_surface_hides_alpha_unsupported_commands(ctx: &mut SmokeContext) -> Result<()> {
    let visible: redis::Value = ctx.exec(&["COMMAND"])?;
    let entries = value_as_array(&visible);
    let command_names: BTreeSet<String> = entries
        .iter()
        .map(|entry| {
            let fields = value_as_array(entry);
            value_as_text(
                fields
                    .first()
                    .expect("command metadata should include a name"),
            )
            .to_ascii_uppercase()
        })
        .collect();

    assert!(command_names.contains("SET"));
    assert!(command_names.contains("WATCH"));
    assert!(!command_names.contains("HSET"));
    assert!(!command_names.contains("BGREWRITEAOF"));
    Ok(())
}

fn command_info_returns_null_for_unsupported_commands(ctx: &mut SmokeContext) -> Result<()> {
    let info: redis::Value = ctx.exec(&["COMMAND", "INFO", "SET", "HSET", "BGREWRITEAOF"])?;
    let entries = value_as_array(&info);

    assert_eq!(entries.len(), 3);
    assert!(matches!(entries.first(), Some(redis::Value::Array(_))));
    assert!(matches!(entries.get(1), Some(redis::Value::Nil)));
    assert!(matches!(entries.get(2), Some(redis::Value::Nil)));
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("COMMAND", CommandGroup::Server, SupportLevel::Partial)
        .summary("Exposes only the alpha-supported command metadata over the live server surface.")
        .syntax(&[
            "COMMAND",
            "COMMAND COUNT",
            "COMMAND INFO command [command ...]",
        ])
        .tested(&[
            "COMMAND COUNT matches the visible COMMAND list",
            "COMMAND omits alpha-unsupported commands such as HSET and BGREWRITEAOF",
            "COMMAND INFO returns null metadata for unsupported commands",
        ])
        .not_tested(&[
            "COMMAND DOCS and COMMAND LIST remain placeholder responses",
            "Full Redis metadata parity",
        ])
        .case(CaseDef::new(
            "command count matches list",
            "COMMAND COUNT should match the number of commands visible through COMMAND.",
            command_count_matches_visible_surface,
        ))
        .case(CaseDef::new(
            "command hides unsupported surface",
            "COMMAND should omit alpha-unsupported commands from the visible server surface.",
            command_surface_hides_alpha_unsupported_commands,
        ))
        .case(CaseDef::new(
            "command info hides unsupported metadata",
            "COMMAND INFO should return null metadata for unsupported commands.",
            command_info_returns_null_for_unsupported_commands,
        ))
}
