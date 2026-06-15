use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn copies_value_to_new_destination(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("src", "value")?;
    let copied: i64 = ctx.exec(&["COPY", "src", "dst"])?;
    assert_eq!(copied, 1);
    assert_eq!(ctx.get("src")?, Some("value".to_string()));
    assert_eq!(ctx.get("dst")?, Some("value".to_string()));
    Ok(())
}

fn replace_flag_overwrites_existing_destination(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("src", "value")?;
    ctx.set("dst", "old")?;
    let copied: i64 = ctx.exec(&["COPY", "src", "dst", "REPLACE"])?;
    assert_eq!(copied, 1);
    assert_eq!(ctx.get("dst")?, Some("value".to_string()));
    Ok(())
}

fn db_zero_copies_and_invalid_db_errors(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("src", "value")?;
    let copied: i64 = ctx.exec(&["COPY", "src", "dst", "DB", "0"])?;
    assert_eq!(copied, 1);
    assert_eq!(ctx.get("dst")?, Some("value".to_string()));

    let err = ctx.exec_error(&["COPY", "src", "bad", "DB", "1"])?;
    assert!(err.to_string().to_ascii_lowercase().contains("db index"));

    let err = ctx.exec_error(&["COPY", "src", "bad", "DB"])?;
    assert!(err.to_string().contains("integer"));

    let err = ctx.exec_error(&["COPY", "src", "bad", "UNKNOWN"])?;
    assert!(err.to_string().contains("syntax"));
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("COPY", CommandGroup::Key, SupportLevel::Partial)
        .summary("Copies the value of one key to another key.")
        .syntax(&["COPY source destination [DB destination-db] [REPLACE]"])
        .tested(&[
            "Copy to new destination",
            "REPLACE overwrites existing destination",
            "DB 0 accepted and malformed DB options rejected",
        ])
        .not_tested(&["Cross-database COPY because Vortex alpha has only DB 0"])
        .case(
            CaseDef::new(
                "copies to new destination",
                "COPY should duplicate the value while preserving the source key.",
                copies_value_to_new_destination,
            )
            .compare_with_baseline(),
        )
        .case(
            CaseDef::new(
                "replace overwrites existing destination",
                "COPY REPLACE should overwrite an existing destination key.",
                replace_flag_overwrites_existing_destination,
            )
            .compare_with_baseline(),
        )
        .case(CaseDef::new(
            "db option validation",
            "COPY should accept DB 0 and reject malformed or non-zero DB options.",
            db_zero_copies_and_invalid_db_errors,
        ))
}
