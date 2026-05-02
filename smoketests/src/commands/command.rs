use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn value_as_array(value: &redis::Value) -> &[redis::Value] {
    match value {
        redis::Value::Array(values) => values,
        other => panic!("expected RESP array, got {other:?}"),
    }
}

fn command_count_is_positive(ctx: &mut SmokeContext) -> Result<()> {
    let count: i64 = ctx.exec(&["COMMAND", "COUNT"])?;
    assert!(count > 0);
    Ok(())
}

fn command_surface_hides_alpha_unsupported_commands(ctx: &mut SmokeContext) -> Result<()> {
    let info: redis::Value =
        ctx.exec(&["COMMAND", "INFO", "SET", "WATCH", "HSET", "BGREWRITEAOF"])?;
    let entries = value_as_array(&info);

    assert_eq!(entries.len(), 4);
    assert!(matches!(entries.first(), Some(redis::Value::Array(_))));
    assert!(matches!(entries.get(1), Some(redis::Value::Array(_))));
    assert!(matches!(entries.get(2), Some(redis::Value::Nil)));
    assert!(matches!(entries.get(3), Some(redis::Value::Nil)));
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
            "COMMAND COUNT",
            "COMMAND INFO command [command ...]",
        ])
        .tested(&[
            "COMMAND COUNT returns a positive visible-command count",
            "COMMAND INFO hides alpha-unsupported commands such as HSET and BGREWRITEAOF",
            "COMMAND INFO returns null metadata for unsupported commands",
        ])
        .not_tested(&[
            "COMMAND list-all output remains a large-response placeholder on the live alpha server surface",
            "COMMAND DOCS and COMMAND LIST remain placeholder responses",
            "Full Redis metadata parity",
        ])
        .case(CaseDef::new(
            "command count is positive",
            "COMMAND COUNT should report a positive number of visible commands.",
            command_count_is_positive,
        ))
        .case(CaseDef::new(
            "command hides unsupported surface",
            "COMMAND INFO should return null metadata for alpha-unsupported commands.",
            command_surface_hides_alpha_unsupported_commands,
        ))
        .case(CaseDef::new(
            "command info hides unsupported metadata",
            "COMMAND INFO should return null metadata for unsupported commands.",
            command_info_returns_null_for_unsupported_commands,
        ))
}
