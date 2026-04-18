use anyhow::Result;

use crate::context::SmokeContext;
use crate::spec::{CaseDef, CommandGroup, CommandSpec, SupportLevel};

fn returns_values_in_requested_order(ctx: &mut SmokeContext) -> Result<()> {
    ctx.set("a", "1")?;
    ctx.set("b", "2")?;
    let values: Vec<Option<String>> = ctx.exec(&["MGET", "a", "missing", "b"])?;
    assert_eq!(
        values,
        vec![Some("1".to_string()), None, Some("2".to_string())]
    );
    Ok(())
}

pub fn spec() -> CommandSpec {
    CommandSpec::new("MGET", CommandGroup::String, SupportLevel::Supported)
        .summary("Atomically fetches the values of multiple string keys.")
        .syntax(&["MGET key [key ...]"])
        .tested(&["Order preservation", "Missing keys return nil entries"])
        .not_tested(&["Large-key batches and prehash hot-path tuning"])
        .case(CaseDef::new(
            "ordered mixed hit miss response",
            "MGET should preserve the input order and return nil for missing keys.",
            returns_values_in_requested_order,
        ))
}
