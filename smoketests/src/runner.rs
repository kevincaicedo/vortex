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
                        CommandGroup::Transaction => candidate.eq_ignore_ascii_case("transactions"),
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
    pub iteration: usize,
    pub target_duration: Duration,
    pub baseline_duration: Option<Duration>,
    pub success: bool,
    pub detail: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RunSummary {
    pub server_url: String,
    pub baseline_url: Option<String>,
    pub repeat: usize,
    pub command_count: usize,
    pub case_count: usize,
    pub failed_cases: usize,
    pub reports: Vec<CaseReport>,
}

#[derive(Debug, Clone)]
pub struct RunOptions {
    pub fail_fast: bool,
    pub repeat: usize,
    pub baseline_url: Option<String>,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            fail_fast: false,
            repeat: 1,
            baseline_url: None,
        }
    }
}

impl RunSummary {
    pub fn to_markdown(&self) -> String {
        let mut out = String::new();
        out.push_str("# Vortex Smoke Test Report\n\n");
        out.push_str(&format!("- Target: `{}`\n", self.server_url));
        if let Some(baseline_url) = &self.baseline_url {
            out.push_str(&format!("- Baseline: `{}`\n", baseline_url));
        }
        out.push_str(&format!("- Repeat count: {}\n", self.repeat));
        out.push_str(&format!("- Commands: {}\n", self.command_count));
        out.push_str(&format!("- Cases: {}\n", self.case_count));
        out.push_str(&format!("- Failures: {}\n\n", self.failed_cases));
        out.push_str("| Command | Case | Iteration | Result | Target | Baseline | Detail |\n");
        out.push_str("|---|---|---:|---|---:|---:|---|\n");
        for report in &self.reports {
            let result = if report.success { "PASS" } else { "FAIL" };
            let baseline = report
                .baseline_duration
                .map(|duration| format!("{} ms", duration.as_millis()))
                .unwrap_or_else(|| "-".to_string());
            let detail = report
                .detail
                .as_deref()
                .unwrap_or("")
                .replace('|', "\\|")
                .replace('\n', " <br> ");
            out.push_str(&format!(
                "| {} | {} | {} | {} | {} ms | {} | {} |\n",
                report.command,
                report.case,
                report.iteration,
                result,
                report.target_duration.as_millis(),
                baseline,
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

pub fn run(server_url: &str, selection: &Selection, options: &RunOptions) -> Result<RunSummary> {
    let specs = selected_specs(selection);
    if specs.is_empty() {
        bail!("selection did not match any smoke-test command specs");
    }
    if options.repeat == 0 {
        bail!("repeat count must be at least 1");
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
            for iteration in 0..options.repeat {
                case_count += 1;

                let baseline = if spec.support == SupportLevel::Supported {
                    match options.baseline_url.as_deref() {
                        Some(baseline_url) => Some(run_case(baseline_url, case)?),
                        None => None,
                    }
                } else {
                    None
                };
                let target = run_case(server_url, case)?;

                let report = match (baseline, target) {
                    (Some(Ok((baseline_duration, _))), Ok((target_duration, _))) => {
                        println!("  PASS {} [iter {}]", case.name, iteration + 1);
                        CaseReport {
                            command: spec.name,
                            case: case.name,
                            summary: case.summary,
                            iteration: iteration + 1,
                            target_duration,
                            baseline_duration: Some(baseline_duration),
                            success: true,
                            detail: None,
                        }
                    }
                    (Some(Ok((baseline_duration, _))), Err(target_detail)) => {
                        failed_cases += 1;
                        println!(
                            "  FAIL {} [iter {}]: target failed while baseline passed: {}",
                            case.name,
                            iteration + 1,
                            target_detail.detail
                        );
                        CaseReport {
                            command: spec.name,
                            case: case.name,
                            summary: case.summary,
                            iteration: iteration + 1,
                            target_duration: target_detail.duration,
                            baseline_duration: Some(baseline_duration),
                            success: false,
                            detail: Some(format!(
                                "baseline passed; target failed: {}",
                                target_detail.detail
                            )),
                        }
                    }
                    (Some(Err(baseline_detail)), Ok((target_duration, _))) => {
                        failed_cases += 1;
                        println!(
                            "  FAIL {} [iter {}]: baseline failed while target passed: {}",
                            case.name,
                            iteration + 1,
                            baseline_detail.detail
                        );
                        CaseReport {
                            command: spec.name,
                            case: case.name,
                            summary: case.summary,
                            iteration: iteration + 1,
                            target_duration,
                            baseline_duration: Some(baseline_detail.duration),
                            success: false,
                            detail: Some(format!(
                                "baseline failed; case may not match Redis semantics: {}",
                                baseline_detail.detail
                            )),
                        }
                    }
                    (Some(Err(baseline_detail)), Err(target_detail)) => {
                        failed_cases += 1;
                        println!(
                            "  FAIL {} [iter {}]: baseline and target failed",
                            case.name,
                            iteration + 1
                        );
                        CaseReport {
                            command: spec.name,
                            case: case.name,
                            summary: case.summary,
                            iteration: iteration + 1,
                            target_duration: target_detail.duration,
                            baseline_duration: Some(baseline_detail.duration),
                            success: false,
                            detail: Some(format!(
                                "baseline failed: {}; target failed: {}",
                                baseline_detail.detail, target_detail.detail
                            )),
                        }
                    }
                    (None, Ok((target_duration, _))) => {
                        println!("  PASS {} [iter {}]", case.name, iteration + 1);
                        CaseReport {
                            command: spec.name,
                            case: case.name,
                            summary: case.summary,
                            iteration: iteration + 1,
                            target_duration,
                            baseline_duration: None,
                            success: true,
                            detail: if spec.support != SupportLevel::Supported
                                && options.baseline_url.is_some()
                            {
                                Some(
                                    "baseline compare skipped for partial/stubbed command"
                                        .to_string(),
                                )
                            } else {
                                None
                            },
                        }
                    }
                    (None, Err(target_detail)) => {
                        failed_cases += 1;
                        println!(
                            "  FAIL {} [iter {}]: {}",
                            case.name,
                            iteration + 1,
                            target_detail.detail
                        );
                        CaseReport {
                            command: spec.name,
                            case: case.name,
                            summary: case.summary,
                            iteration: iteration + 1,
                            target_duration: target_detail.duration,
                            baseline_duration: None,
                            success: false,
                            detail: Some(target_detail.detail),
                        }
                    }
                };

                reports.push(report);
                if options.fail_fast && failed_cases > 0 {
                    return Ok(RunSummary {
                        server_url: server_url.to_string(),
                        baseline_url: options.baseline_url.clone(),
                        repeat: options.repeat,
                        command_count: specs.len(),
                        case_count,
                        failed_cases,
                        reports,
                    });
                }
            }
        }
    }

    Ok(RunSummary {
        server_url: server_url.to_string(),
        baseline_url: options.baseline_url.clone(),
        repeat: options.repeat,
        command_count: specs.len(),
        case_count,
        failed_cases,
        reports,
    })
}

#[derive(Debug)]
struct CaseFailure {
    duration: Duration,
    detail: String,
}

fn run_case(
    server_url: &str,
    case: &crate::spec::CaseDef,
) -> Result<Result<(Duration, ()), CaseFailure>> {
    let mut ctx = SmokeContext::connect(server_url)?;
    ctx.reset()?;

    let started = Instant::now();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (case.run)(&mut ctx)));
    let duration = started.elapsed();

    Ok(match outcome {
        Ok(Ok(())) => Ok((duration, ())),
        Ok(Err(err)) => Err(CaseFailure {
            duration,
            detail: format!("{err:#}"),
        }),
        Err(panic) => Err(CaseFailure {
            duration,
            detail: panic_payload_to_string(panic),
        }),
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
