use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};

use crate::commands;
use crate::context::SmokeContext;
use crate::spec::{CommandGroup, CommandSpec, SupportLevel};

#[derive(Debug, Default, Clone)]
pub struct Selection {
    pub commands: Vec<String>,
    pub groups: Vec<String>,
    pub include_stubbed: bool,
}

impl Selection {
    pub fn matches(&self, spec: &CommandSpec) -> bool {
        if !self.include_stubbed && spec.support == SupportLevel::Stubbed {
            return false;
        }

        if !self.commands.is_empty()
            && !self
                .commands
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(spec.name))
        {
            return false;
        }

        if !self.groups.is_empty()
            && !self.groups.iter().any(|candidate| {
                candidate.eq_ignore_ascii_case(spec.group.as_str())
                    || match spec.group {
                        CommandGroup::String => candidate.eq_ignore_ascii_case("strings"),
                        CommandGroup::Key => candidate.eq_ignore_ascii_case("keys"),
                        CommandGroup::Server => {
                            candidate.eq_ignore_ascii_case("server")
                                || candidate.eq_ignore_ascii_case("connection")
                        }
                    }
            })
        {
            return false;
        }

        true
    }
}

#[derive(Debug, Clone)]
pub struct CaseReport {
    pub command: &'static str,
    pub case: &'static str,
    pub summary: &'static str,
    pub duration: Duration,
    pub success: bool,
    pub detail: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RunSummary {
    pub server_url: String,
    pub command_count: usize,
    pub case_count: usize,
    pub failed_cases: usize,
    pub reports: Vec<CaseReport>,
}

impl RunSummary {
    pub fn to_markdown(&self) -> String {
        let mut out = String::new();
        out.push_str("# Vortex Smoke Test Report\n\n");
        out.push_str(&format!("- Target: `{}`\n", self.server_url));
        out.push_str(&format!("- Commands: {}\n", self.command_count));
        out.push_str(&format!("- Cases: {}\n", self.case_count));
        out.push_str(&format!("- Failures: {}\n\n", self.failed_cases));
        out.push_str("| Command | Case | Result | Duration | Detail |\n");
        out.push_str("|---|---|---|---:|---|\n");
        for report in &self.reports {
            let result = if report.success { "PASS" } else { "FAIL" };
            let detail = report
                .detail
                .as_deref()
                .unwrap_or("")
                .replace('|', "\\|")
                .replace('\n', " <br> ");
            out.push_str(&format!(
                "| {} | {} | {} | {} ms | {} |\n",
                report.command,
                report.case,
                result,
                report.duration.as_millis(),
                detail
            ));
        }
        out
    }

    pub fn write_markdown(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, self.to_markdown())?;
        Ok(())
    }
}

pub fn selected_specs(selection: &Selection) -> Vec<CommandSpec> {
    commands::all_specs()
        .into_iter()
        .filter(|spec| selection.matches(spec))
        .collect()
}

pub fn run(server_url: &str, selection: &Selection, fail_fast: bool) -> Result<RunSummary> {
    let specs = selected_specs(selection);
    if specs.is_empty() {
        bail!("selection did not match any smoke-test command specs");
    }

    let mut reports = Vec::new();
    let mut failed_cases = 0usize;
    let mut case_count = 0usize;

    for spec in &specs {
        println!(
            "== {} [{} / {}] ==",
            spec.name,
            spec.group.as_str(),
            spec.support.as_str()
        );
        for case in &spec.cases {
            case_count += 1;
            let mut ctx = SmokeContext::connect(server_url)?;
            ctx.reset()?;

            let started = Instant::now();
            let outcome =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (case.run)(&mut ctx)));
            let duration = started.elapsed();

            let report = match outcome {
                Ok(Ok(())) => {
                    println!("  PASS {}", case.name);
                    CaseReport {
                        command: spec.name,
                        case: case.name,
                        summary: case.summary,
                        duration,
                        success: true,
                        detail: None,
                    }
                }
                Ok(Err(err)) => {
                    failed_cases += 1;
                    let detail = format!("{err:#}");
                    println!("  FAIL {}: {}", case.name, detail);
                    CaseReport {
                        command: spec.name,
                        case: case.name,
                        summary: case.summary,
                        duration,
                        success: false,
                        detail: Some(detail),
                    }
                }
                Err(panic) => {
                    failed_cases += 1;
                    let detail = panic_payload_to_string(panic);
                    println!("  FAIL {}: {}", case.name, detail);
                    CaseReport {
                        command: spec.name,
                        case: case.name,
                        summary: case.summary,
                        duration,
                        success: false,
                        detail: Some(detail),
                    }
                }
            };

            reports.push(report);
            if fail_fast && failed_cases > 0 {
                return Ok(RunSummary {
                    server_url: server_url.to_string(),
                    command_count: specs.len(),
                    case_count,
                    failed_cases,
                    reports,
                });
            }
        }
    }

    Ok(RunSummary {
        server_url: server_url.to_string(),
        command_count: specs.len(),
        case_count,
        failed_cases,
        reports,
    })
}

fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_string();
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    "panic without string payload".to_string()
}
