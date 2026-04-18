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

pub fn spec() -> CommandSpec {
    CommandSpec::new("COPY", CommandGroup::Key, SupportLevel::Supported)
        .summary("Copies the value of one key to another key.")
        .syntax(&["COPY source destination [DB destination-db] [REPLACE]"])
        .tested(&[
            "Copy to new destination",
            "REPLACE overwrites existing destination",
        ])
        .not_tested(&["DB option because Vortex currently supports only DB 0"])
        .case(CaseDef::new(
            "copies to new destination",
            "COPY should duplicate the value while preserving the source key.",
            copies_value_to_new_destination,
        ))
        .case(CaseDef::new(
            "replace overwrites existing destination",
            "COPY REPLACE should overwrite an existing destination key.",
            replace_flag_overwrites_existing_destination,
        ))
}
