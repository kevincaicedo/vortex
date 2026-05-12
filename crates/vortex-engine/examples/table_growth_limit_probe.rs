use std::env;
use std::fmt::Write as _;
use std::time::{Duration, Instant};

use vortex_common::{VortexKey, VortexValue};
use vortex_engine::SwissTable;

const LOAD_FACTOR_N: usize = 7;
const LOAD_FACTOR_D: usize = 8;

fn main() -> Result<(), String> {
    let args = Args::parse()?;
    let summary = run_probe(args);
    println!("{}", summary.to_json());
    Ok(())
}

#[derive(Clone, Copy)]
struct Args {
    capacity: usize,
    repeats: usize,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let mut capacity = 16_384usize;
        let mut repeats = 64usize;
        let mut args = env::args().skip(1);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--capacity" => {
                    capacity = parse_value(&arg, args.next())?;
                }
                "--repeats" => {
                    repeats = parse_value(&arg, args.next())?;
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
        if repeats == 0 {
            return Err("--repeats must be greater than zero".to_string());
        }

        Ok(Self { capacity, repeats })
    }
}

struct ProbeSummary {
    requested_capacity: usize,
    repeats: usize,
    total_slots_before: usize,
    growth_limit: usize,
    resize_events: usize,
    total_slots_after_min: usize,
    total_slots_after_max: usize,
    mean_ns: f64,
    p50_ns: u128,
    p95_ns: u128,
    p99_ns: u128,
}

impl ProbeSummary {
    fn to_json(&self) -> String {
        let mut json = String::with_capacity(512);
        let _ = writeln!(&mut json, "{{");
        write_usize(
            &mut json,
            "requested_capacity",
            self.requested_capacity,
            true,
        );
        write_usize(&mut json, "repeats", self.repeats, true);
        write_usize(
            &mut json,
            "total_slots_before",
            self.total_slots_before,
            true,
        );
        write_usize(&mut json, "growth_limit", self.growth_limit, true);
        write_usize(&mut json, "resize_events", self.resize_events, true);
        write_usize(
            &mut json,
            "total_slots_after_min",
            self.total_slots_after_min,
            true,
        );
        write_usize(
            &mut json,
            "total_slots_after_max",
            self.total_slots_after_max,
            true,
        );
        let _ = writeln!(&mut json, "  \"mean_ns\": {:.3},", self.mean_ns);
        write_u128(&mut json, "p50_ns", self.p50_ns, true);
        write_u128(&mut json, "p95_ns", self.p95_ns, true);
        write_u128(&mut json, "p99_ns", self.p99_ns, false);
        let _ = writeln!(&mut json, "}}");
        json
    }
}

fn run_probe(args: Args) -> ProbeSummary {
    let mut durations = Vec::with_capacity(args.repeats);
    let mut resize_events = 0usize;
    let mut total_slots_after_min = usize::MAX;
    let mut total_slots_after_max = 0usize;
    let mut total_slots_before = 0usize;
    let mut growth_limit = 0usize;

    for repeat in 0..args.repeats {
        let mut table = SwissTable::with_capacity(args.capacity);
        total_slots_before = table.total_slots();
        growth_limit = total_slots_before * LOAD_FACTOR_N / LOAD_FACTOR_D;

        for index in 0..growth_limit {
            table.insert(make_key(index), VortexValue::Integer(index as i64));
        }

        let replace_key = make_key(repeat % growth_limit);
        let slots_before = table.total_slots();
        let start = Instant::now();
        let previous = table.insert(replace_key, VortexValue::Integer(-(repeat as i64) - 1));
        let elapsed = start.elapsed();
        let slots_after = table.total_slots();

        assert!(
            previous.is_some(),
            "growth-limit probe must replace an existing key"
        );
        if slots_after != slots_before {
            resize_events += 1;
        }
        total_slots_after_min = total_slots_after_min.min(slots_after);
        total_slots_after_max = total_slots_after_max.max(slots_after);
        durations.push(elapsed);
    }

    durations.sort_unstable();
    let total_ns: u128 = durations.iter().map(Duration::as_nanos).sum();
    let repeats = args.repeats.max(1);

    ProbeSummary {
        requested_capacity: args.capacity,
        repeats: args.repeats,
        total_slots_before,
        growth_limit,
        resize_events,
        total_slots_after_min,
        total_slots_after_max,
        mean_ns: total_ns as f64 / repeats as f64,
        p50_ns: percentile(&durations, 50),
        p95_ns: percentile(&durations, 95),
        p99_ns: percentile(&durations, 99),
    }
}

fn make_key(index: usize) -> VortexKey {
    VortexKey::from(format!("growth:{index:08}").as_str())
}

fn percentile(durations: &[Duration], percentile: usize) -> u128 {
    let index = durations.len().saturating_sub(1).saturating_mul(percentile) / 100;
    durations[index].as_nanos()
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

fn write_usize(buf: &mut String, name: &str, value: usize, trailing_comma: bool) {
    let trailing = if trailing_comma { "," } else { "" };
    let _ = writeln!(buf, "  \"{name}\": {value}{trailing}");
}

fn write_u128(buf: &mut String, name: &str, value: u128, trailing_comma: bool) {
    let trailing = if trailing_comma { "," } else { "" };
    let _ = writeln!(buf, "  \"{name}\": {value}{trailing}");
}

fn print_usage() {
    println!(
        "table_growth_limit_probe [--capacity N] [--repeats N]\n\
         Measures one existing-key replacement when occupied == table growth limit."
    );
}
