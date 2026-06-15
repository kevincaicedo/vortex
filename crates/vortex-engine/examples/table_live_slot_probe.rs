use std::env;
use std::fmt::Write as _;
use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use std::time::Instant;

use vortex_common::{VortexKey, VortexValue};
use vortex_engine::SwissTable;

fn main() -> Result<(), String> {
    let args = Args::parse()?;
    let summary = run_probe(&args)?;
    let json = summary.to_json();
    if let Some(path) = args.json {
        fs::write(&path, json.as_bytes())
            .map_err(|error| format!("failed to write {}: {error}", path.display()))?;
    }
    println!("{json}");
    Ok(())
}

struct Args {
    scenario: Scenario,
    capacity: usize,
    live_keys: usize,
    value_size: usize,
    json: Option<PathBuf>,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let mut scenario = Scenario::EmptyPreSized;
        let mut capacity = 1_000_000usize;
        let mut live_keys = 100_000usize;
        let mut value_size = 16usize;
        let mut json = None;
        let mut args = env::args().skip(1);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--scenario" => {
                    scenario = Scenario::parse(args.next())?;
                }
                "--capacity" => {
                    capacity = parse_value(&arg, args.next())?;
                }
                "--live-keys" => {
                    live_keys = parse_value(&arg, args.next())?;
                }
                "--value-size" => {
                    value_size = parse_value(&arg, args.next())?;
                }
                "--json" => {
                    json = Some(PathBuf::from(
                        args.next()
                            .ok_or_else(|| "--json requires a value".to_string())?,
                    ));
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => return Err(format!("unknown argument: {other}")),
            }
        }

        if capacity == 0 {
            return Err("--capacity must be greater than zero".to_string());
        }
        if value_size == 0 {
            return Err("--value-size must be greater than zero".to_string());
        }
        if matches!(scenario, Scenario::EmptyPreSized) {
            live_keys = 0;
        }
        if live_keys > capacity {
            return Err("--live-keys must be <= --capacity".to_string());
        }

        Ok(Self {
            scenario,
            capacity,
            live_keys,
            value_size,
            json,
        })
    }
}

#[derive(Clone, Copy)]
enum Scenario {
    EmptyPreSized,
    LowFillInlineString,
}

impl Scenario {
    fn parse(value: Option<String>) -> Result<Self, String> {
        match value.as_deref() {
            Some("empty-pre-sized") => Ok(Self::EmptyPreSized),
            Some("low-fill-inline-string") => Ok(Self::LowFillInlineString),
            Some(other) => Err(format!("unknown --scenario: {other}")),
            None => Err("--scenario requires a value".to_string()),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::EmptyPreSized => "empty-pre-sized",
            Self::LowFillInlineString => "low-fill-inline-string",
        }
    }
}

struct ProbeSummary {
    scenario: Scenario,
    requested_capacity: usize,
    live_keys: usize,
    value_size_bytes: usize,
    total_slots: usize,
    load_factor: f64,
    table_logical_bytes: usize,
    table_allocated_bytes: usize,
    allocation_elapsed_ns: u128,
    insert_elapsed_ns: Option<u128>,
    process_rss_after_alloc_bytes: Option<usize>,
    process_rss_after_fill_bytes: Option<usize>,
}

impl ProbeSummary {
    fn to_json(&self) -> String {
        let mut json = String::with_capacity(768);
        let _ = writeln!(&mut json, "{{");
        write_str(&mut json, "scenario", self.scenario.name(), true);
        write_usize(
            &mut json,
            "requested_capacity",
            self.requested_capacity,
            true,
        );
        write_usize(&mut json, "live_keys", self.live_keys, true);
        write_usize(&mut json, "value_size_bytes", self.value_size_bytes, true);
        write_usize(&mut json, "total_slots", self.total_slots, true);
        let _ = writeln!(&mut json, "  \"load_factor\": {:.6},", self.load_factor);
        write_usize(
            &mut json,
            "table_logical_bytes",
            self.table_logical_bytes,
            true,
        );
        write_usize(
            &mut json,
            "table_allocated_bytes",
            self.table_allocated_bytes,
            true,
        );
        write_u128(
            &mut json,
            "allocation_elapsed_ns",
            self.allocation_elapsed_ns,
            true,
        );
        write_option_u128(&mut json, "insert_elapsed_ns", self.insert_elapsed_ns, true);
        write_option_usize(
            &mut json,
            "process_rss_after_alloc_bytes",
            self.process_rss_after_alloc_bytes,
            true,
        );
        write_option_usize(
            &mut json,
            "process_rss_after_fill_bytes",
            self.process_rss_after_fill_bytes,
            false,
        );
        let _ = writeln!(&mut json, "}}");
        json
    }
}

fn run_probe(args: &Args) -> Result<ProbeSummary, String> {
    let allocation_started = Instant::now();
    let mut table = SwissTable::with_capacity(args.capacity);
    let allocation_elapsed = allocation_started.elapsed();
    let rss_after_alloc = current_process_rss_bytes();

    let insert_elapsed = if args.live_keys == 0 {
        None
    } else {
        let started = Instant::now();
        for index in 0..args.live_keys {
            table.insert(
                VortexKey::from(make_key(index).as_slice()),
                VortexValue::from_bytes(make_value(index, args.value_size).as_slice()),
            );
        }
        Some(started.elapsed().as_nanos())
    };
    let rss_after_fill = current_process_rss_bytes();

    black_box(table.len());

    Ok(ProbeSummary {
        scenario: args.scenario,
        requested_capacity: args.capacity,
        live_keys: table.len(),
        value_size_bytes: args.value_size,
        total_slots: table.total_slots(),
        load_factor: table.load_factor(),
        table_logical_bytes: table.memory_used(),
        table_allocated_bytes: table.allocated_bytes(),
        allocation_elapsed_ns: allocation_elapsed.as_nanos(),
        insert_elapsed_ns: insert_elapsed,
        process_rss_after_alloc_bytes: rss_after_alloc,
        process_rss_after_fill_bytes: rss_after_fill,
    })
}

fn make_key(index: usize) -> Vec<u8> {
    format!("key:{index:08}").into_bytes()
}

fn make_value(index: usize, value_size: usize) -> Vec<u8> {
    let mut value = format!("value:{index:08}").into_bytes();
    value.resize(value_size, b'v');
    value
}

fn current_process_rss_bytes() -> Option<usize> {
    current_process_rss_bytes_procfs().or_else(current_process_peak_rss_bytes)
}

#[cfg(target_os = "linux")]
fn current_process_rss_bytes_procfs() -> Option<usize> {
    let statm = fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages = statm.split_whitespace().nth(1)?.parse::<usize>().ok()?;
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return None;
    }
    resident_pages.checked_mul(page_size as usize)
}

#[cfg(not(target_os = "linux"))]
fn current_process_rss_bytes_procfs() -> Option<usize> {
    None
}

fn current_process_peak_rss_bytes() -> Option<usize> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let max_rss = unsafe { usage.assume_init() }.ru_maxrss;
    if max_rss <= 0 {
        return None;
    }

    #[cfg(target_os = "macos")]
    {
        Some(max_rss as usize)
    }

    #[cfg(not(target_os = "macos"))]
    {
        (max_rss as usize).checked_mul(1024)
    }
}

fn parse_value<T>(name: &str, value: Option<String>) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .ok_or_else(|| format!("{name} requires a value"))?
        .parse()
        .map_err(|error| format!("invalid {name}: {error}"))
}

fn write_str(buf: &mut String, name: &str, value: &str, trailing_comma: bool) {
    let trailing = if trailing_comma { "," } else { "" };
    let _ = writeln!(buf, "  \"{name}\": \"{value}\"{trailing}");
}

fn write_usize(buf: &mut String, name: &str, value: usize, trailing_comma: bool) {
    let trailing = if trailing_comma { "," } else { "" };
    let _ = writeln!(buf, "  \"{name}\": {value}{trailing}");
}

fn write_u128(buf: &mut String, name: &str, value: u128, trailing_comma: bool) {
    let trailing = if trailing_comma { "," } else { "" };
    let _ = writeln!(buf, "  \"{name}\": {value}{trailing}");
}

fn write_option_usize(buf: &mut String, name: &str, value: Option<usize>, trailing_comma: bool) {
    let trailing = if trailing_comma { "," } else { "" };
    match value {
        Some(value) => {
            let _ = writeln!(buf, "  \"{name}\": {value}{trailing}");
        }
        None => {
            let _ = writeln!(buf, "  \"{name}\": null{trailing}");
        }
    }
}

fn write_option_u128(buf: &mut String, name: &str, value: Option<u128>, trailing_comma: bool) {
    let trailing = if trailing_comma { "," } else { "" };
    match value {
        Some(value) => {
            let _ = writeln!(buf, "  \"{name}\": {value}{trailing}");
        }
        None => {
            let _ = writeln!(buf, "  \"{name}\": null{trailing}");
        }
    }
}

fn print_usage() {
    println!(
        "table_live_slot_probe --scenario empty-pre-sized|low-fill-inline-string \n\
         [--capacity N] [--live-keys N] [--value-size N] [--json PATH]"
    );
}
