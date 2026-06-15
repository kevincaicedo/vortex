use anyhow::Result;
use std::{thread, time::Duration};

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn returns_info_sections(ctx: &mut SmokeContext) -> Result<()> {
    let info: String = ctx.exec(&["INFO"])?;
    assert!(info.contains("# Server"));
    assert!(info.contains("# Keyspace"));
    Ok(())
}

fn keyspace_section_counts_live_keys(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("live", "1")?;
    ctx.set("expired", "1")?;
    let applied: i64 = ctx.exec(&["PEXPIRE", "expired", "100"])?;
    assert_eq!(applied, 1);

    thread::sleep(Duration::from_millis(150));

    let info: String = ctx.exec(&["INFO", "keyspace"])?;
    assert!(
        info.contains("db0:keys=1,expires=0"),
        "unexpected info payload: {info}"
    );
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("INFO", CommandGroup::Server, SupportLevel::Partial)
        .summary("Returns a reduced Redis-compatible INFO payload for server diagnostics.")
        .syntax(&["INFO [section]"])
        .tested(&[
            "Default INFO output contains server and keyspace sections",
            "Keyspace section reports live-key counts",
        ])
        .not_tested(&["Full Redis INFO field parity"])
        .case(CaseDef::new(
            "returns key sections",
            "INFO should include the server and keyspace sections Vortex exposes.",
            returns_info_sections,
        ))
        .case(CaseDef::new(
            "keyspace counts live keys",
            "INFO keyspace should count only live keys and live expirations.",
            keyspace_section_counts_live_keys,
        ))
}
