use std::alloc::{GlobalAlloc, Layout, System};
use std::fmt::Write as _;
use std::fs;
use std::hint::black_box;
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use clap::{Parser, ValueEnum};
use vortex_common::{VortexKey, VortexValue};
use vortex_engine::commands::{CommandClock, execute_command};
use vortex_engine::keyspace::ShardCount;
use vortex_engine::{ConcurrentKeyspace, EvictionPolicy};
use vortex_proto::RespTape;

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static DEALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static REALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: This allocator is only a transparent profiling wrapper around
        // the platform allocator and forwards the caller-provided layout.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        DEALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: `ptr` and `layout` are exactly the pair passed by the global
        // allocation API for a prior allocation from this allocator.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        REALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        DEALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: This forwards the original pointer/layout and requested size
        // to the platform allocator without changing allocator ownership.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Workload {
    Exists,
    Del,
    Mset,
    Msetnx,
}

#[derive(Debug, Parser)]
#[command(about = "Count hot-loop allocation calls for selected engine command paths")]
struct Args {
    #[arg(long, value_enum)]
    workload: Workload,

    #[arg(long, default_value_t = 16)]
    width: usize,

    #[arg(long, default_value_t = 100_000)]
    iterations: u64,

    #[arg(long, default_value_t = 64)]
    shards: usize,

    #[arg(long)]
    json: Option<PathBuf>,
}

#[derive(Clone, Copy)]
struct AllocationSnapshot {
    alloc_calls: u64,
    dealloc_calls: u64,
    realloc_calls: u64,
    allocated_bytes: u64,
    deallocated_bytes: u64,
}

impl AllocationSnapshot {
    fn read() -> Self {
        Self {
            alloc_calls: ALLOC_CALLS.load(Ordering::Relaxed),
            dealloc_calls: DEALLOC_CALLS.load(Ordering::Relaxed),
            realloc_calls: REALLOC_CALLS.load(Ordering::Relaxed),
            allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
            deallocated_bytes: DEALLOCATED_BYTES.load(Ordering::Relaxed),
        }
    }
}

struct ProbeSummary {
    workload: Workload,
    width: usize,
    iterations: u64,
    shards: usize,
    alloc_calls: u64,
    dealloc_calls: u64,
    realloc_calls: u64,
    allocated_bytes: u64,
    deallocated_bytes: u64,
}

impl ProbeSummary {
    fn to_json(&self) -> String {
        let mut json = String::with_capacity(512);
        let operations = self.iterations.max(1) as f64;
        let _ = writeln!(&mut json, "{{");
        write_json_string(&mut json, "workload", workload_name(self.workload), true);
        write_json_usize(&mut json, "width", self.width, true);
        write_json_u64(&mut json, "iterations", self.iterations, true);
        write_json_usize(&mut json, "shards", self.shards, true);
        write_json_u64(&mut json, "alloc_calls", self.alloc_calls, true);
        write_json_u64(&mut json, "realloc_calls", self.realloc_calls, true);
        write_json_u64(&mut json, "dealloc_calls", self.dealloc_calls, true);
        write_json_u64(&mut json, "allocated_bytes", self.allocated_bytes, true);
        write_json_u64(&mut json, "deallocated_bytes", self.deallocated_bytes, true);
        write_json_f64(
            &mut json,
            "alloc_calls_per_op",
            self.alloc_calls as f64 / operations,
            true,
        );
        write_json_f64(
            &mut json,
            "allocated_bytes_per_op",
            self.allocated_bytes as f64 / operations,
            false,
        );
        let _ = writeln!(&mut json, "}}");
        json
    }
}

fn main() -> Result<(), String> {
    let args = Args::parse();
    if args.width == 0 {
        return Err("--width must be greater than zero".to_string());
    }
    if args.iterations == 0 {
        return Err("--iterations must be greater than zero".to_string());
    }
    ShardCount::try_new(args.shards).map_err(|error| format!("--shards {error}"))?;

    let keyspace = ConcurrentKeyspace::try_with_capacity(args.shards, args.width.max(128))
        .map_err(|error| format!("failed to create keyspace: {error}"))?;
    keyspace.configure_eviction(usize::MAX, EvictionPolicy::NoEviction);
    let parts = command_parts(args.workload, args.width);
    let wire = make_resp(&parts);
    let tape = RespTape::parse_pipeline(&wire)
        .map_err(|error| format!("failed to parse generated command: {error:?}"))?;
    let frame = tape
        .iter()
        .next()
        .ok_or_else(|| "generated command did not produce a frame".to_string())?;
    let command = command_name(args.workload);

    prefill(&keyspace, args.workload, args.width)?;

    let before = AllocationSnapshot::read();
    for _ in 0..args.iterations {
        let executed = execute_command(&keyspace, command, &frame, CommandClock::from(0u64))
            .ok_or_else(|| {
                format!(
                    "unknown generated command: {}",
                    workload_name(args.workload)
                )
            })?;
        if executed.response.is_error() {
            return Err(format!(
                "{} returned an error response",
                workload_name(args.workload)
            ));
        }
        black_box(executed);
    }
    let after = AllocationSnapshot::read();

    let summary = ProbeSummary {
        workload: args.workload,
        width: args.width,
        iterations: args.iterations,
        shards: args.shards,
        alloc_calls: after.alloc_calls.saturating_sub(before.alloc_calls),
        dealloc_calls: after.dealloc_calls.saturating_sub(before.dealloc_calls),
        realloc_calls: after.realloc_calls.saturating_sub(before.realloc_calls),
        allocated_bytes: after.allocated_bytes.saturating_sub(before.allocated_bytes),
        deallocated_bytes: after
            .deallocated_bytes
            .saturating_sub(before.deallocated_bytes),
    };
    let json = summary.to_json();

    if let Some(path) = &args.json {
        fs::write(path, json.as_bytes())
            .map_err(|error| format!("failed to write {}: {error}", path.display()))?;
    }
    print!("{json}");
    Ok(())
}

fn prefill(keyspace: &ConcurrentKeyspace, workload: Workload, width: usize) -> Result<(), String> {
    if !matches!(
        workload,
        Workload::Exists | Workload::Mset | Workload::Msetnx
    ) {
        return Ok(());
    }

    for index in 0..width {
        let key = VortexKey::from_bytes(key_bytes(index).as_slice());
        // SAFETY: This is benchmark setup before the measured loop. No
        // concurrent command path observes the setup mutations.
        unsafe {
            keyspace.benchmark_insert_unchecked(key, VortexValue::from_bytes(b"v0"));
        }
    }
    Ok(())
}

fn command_parts(workload: Workload, width: usize) -> Vec<Vec<u8>> {
    let mut parts = Vec::with_capacity(1 + width.saturating_mul(2));
    parts.push(command_name(workload).to_vec());
    match workload {
        Workload::Exists | Workload::Del => {
            for index in 0..width {
                parts.push(key_bytes(index));
            }
        }
        Workload::Mset | Workload::Msetnx => {
            for index in 0..width {
                parts.push(key_bytes(index));
                parts.push(value_bytes(index));
            }
        }
    }
    parts
}

fn key_bytes(index: usize) -> Vec<u8> {
    format!("k{index:06}").into_bytes()
}

fn value_bytes(index: usize) -> Vec<u8> {
    format!("v{index:06}").into_bytes()
}

fn make_resp(parts: &[Vec<u8>]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(parts.len().saturating_mul(32));
    let _ = write!(&mut buf, "*{}\r\n", parts.len());
    for part in parts {
        let _ = write!(&mut buf, "${}\r\n", part.len());
        buf.extend_from_slice(part);
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

fn command_name(workload: Workload) -> &'static [u8] {
    match workload {
        Workload::Exists => b"EXISTS",
        Workload::Del => b"DEL",
        Workload::Mset => b"MSET",
        Workload::Msetnx => b"MSETNX",
    }
}

fn workload_name(workload: Workload) -> &'static str {
    match workload {
        Workload::Exists => "exists",
        Workload::Del => "del",
        Workload::Mset => "mset",
        Workload::Msetnx => "msetnx",
    }
}

fn write_json_string(out: &mut String, key: &str, value: &str, comma: bool) {
    let suffix = if comma { "," } else { "" };
    let _ = writeln!(out, "  \"{key}\": \"{value}\"{suffix}");
}

fn write_json_usize(out: &mut String, key: &str, value: usize, comma: bool) {
    write_json_u64(out, key, value as u64, comma);
}

fn write_json_u64(out: &mut String, key: &str, value: u64, comma: bool) {
    let suffix = if comma { "," } else { "" };
    let _ = writeln!(out, "  \"{key}\": {value}{suffix}");
}

fn write_json_f64(out: &mut String, key: &str, value: f64, comma: bool) {
    let suffix = if comma { "," } else { "" };
    let _ = writeln!(out, "  \"{key}\": {value:.6}{suffix}");
}
