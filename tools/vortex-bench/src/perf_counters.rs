//! Hardware performance counter wrapper using `perf_event_open`.
//!
//! Only available on Linux. Measures: IPC, L1/L2/LLC cache misses,
//! branch mispredictions.
//!
//! ```ignore
//! let mut group = PerfCounterGroup::new().unwrap();
//! group.start().unwrap();
//! // ... work ...
//! group.stop().unwrap();
//! let snapshot = group.read().unwrap();
//! println!("IPC: {:.2}", snapshot.ipc());
//! ```

use std::io;

// ── Linux perf_event constants (not exported by libc crate) ─────────

const PERF_TYPE_HARDWARE: u32 = 0;
const PERF_COUNT_HW_CPU_CYCLES: u64 = 0;
const PERF_COUNT_HW_INSTRUCTIONS: u64 = 1;
const PERF_COUNT_HW_CACHE_REFERENCES: u64 = 3;
const PERF_COUNT_HW_CACHE_MISSES: u64 = 4;
const PERF_COUNT_HW_BRANCH_INSTRUCTIONS: u64 = 5;
const PERF_COUNT_HW_BRANCH_MISSES: u64 = 6;

const PERF_FLAG_FD_CLOEXEC: u64 = 1 << 3;

// ioctl request codes for perf events (from linux/perf_event.h)
// These are _IO / _IOW macros expanded for '$' (0x24) type.
const PERF_EVENT_IOC_ENABLE: u64 = 0x2400;
const PERF_EVENT_IOC_DISABLE: u64 = 0x2401;
const PERF_EVENT_IOC_RESET: u64 = 0x2403;
const PERF_IOC_FLAG_GROUP: u64 = 1;

/// Minimal `perf_event_attr` matching the kernel ABI (first 112 bytes).
/// We only need `type_`, `size`, `config`, and the bitfield flags.
#[repr(C)]
struct PerfEventAttr {
    type_: u32,
    size: u32,
    config: u64,
    sample_period_or_freq: u64,
    sample_type: u64,
    read_format: u64,
    /// Bitfield flags packed as a u64.
    /// Bit 0: disabled, bit 1: inherit, bit 2: pinned, bit 3: exclusive,
    /// bit 4: exclude_user, bit 5: exclude_kernel, bit 6: exclude_hv, ...
    flags: u64,
    wakeup_events_or_watermark: u32,
    bp_type: u32,
    config1: u64,
    config2: u64,
    branch_sample_type: u64,
    sample_regs_user: u64,
    sample_stack_user: u32,
    clockid: i32,
    sample_regs_intr: u64,
    aux_watermark: u32,
    sample_max_stack: u16,
    __reserved_2: u16,
}

impl PerfEventAttr {
    fn new(type_: u32, config: u64, disabled: bool) -> Self {
        let mut attr = Self {
            type_,
            size: std::mem::size_of::<Self>() as u32,
            config,
            sample_period_or_freq: 0,
            sample_type: 0,
            read_format: 0,
            flags: 0,
            wakeup_events_or_watermark: 0,
            bp_type: 0,
            config1: 0,
            config2: 0,
            branch_sample_type: 0,
            sample_regs_user: 0,
            sample_stack_user: 0,
            clockid: 0,
            sample_regs_intr: 0,
            aux_watermark: 0,
            sample_max_stack: 0,
            __reserved_2: 0,
        };
        // flags bitfield: bit 0 = disabled, bit 5 = exclude_kernel, bit 6 = exclude_hv
        let mut bits: u64 = 0;
        if disabled {
            bits |= 1 << 0;
        }
        bits |= 1 << 5; // exclude_kernel
        bits |= 1 << 6; // exclude_hv
        attr.flags = bits;
        attr
    }
}

/// Raw file descriptor for a perf event.
struct PerfFd(i32);

impl Drop for PerfFd {
    fn drop(&mut self) {
        unsafe { libc::close(self.0) };
    }
}

/// Snapshot of hardware counter readings.
#[derive(Debug, Clone, Copy)]
pub struct PerfSnapshot {
    pub cycles: u64,
    pub instructions: u64,
    pub cache_misses: u64,
    pub cache_references: u64,
    pub branch_misses: u64,
    pub branch_instructions: u64,
}

impl PerfSnapshot {
    /// Instructions per cycle.
    pub fn ipc(&self) -> f64 {
        if self.cycles == 0 {
            return 0.0;
        }
        self.instructions as f64 / self.cycles as f64
    }

    /// Cache miss rate (0.0–1.0).
    pub fn cache_miss_rate(&self) -> f64 {
        if self.cache_references == 0 {
            return 0.0;
        }
        self.cache_misses as f64 / self.cache_references as f64
    }

    /// Branch misprediction rate (0.0–1.0).
    pub fn branch_miss_rate(&self) -> f64 {
        if self.branch_instructions == 0 {
            return 0.0;
        }
        self.branch_misses as f64 / self.branch_instructions as f64
    }
}

/// A group of hardware performance counters.
pub struct PerfCounterGroup {
    leader: PerfFd,
    members: [PerfFd; 5],
}

fn perf_event_open(attr: &PerfEventAttr, group_fd: i32) -> io::Result<PerfFd> {
    let fd = unsafe {
        libc::syscall(
            libc::SYS_perf_event_open,
            attr as *const PerfEventAttr,
            0i32,  // pid: current process
            -1i32, // cpu: any CPU
            group_fd,
            PERF_FLAG_FD_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(PerfFd(fd as i32))
}

impl PerfCounterGroup {
    /// Creates a new counter group. Returns `Err` if perf is unavailable.
    pub fn new() -> io::Result<Self> {
        let leader_attr = PerfEventAttr::new(PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES, true);
        let leader = perf_event_open(&leader_attr, -1)?;

        let configs = [
            PERF_COUNT_HW_INSTRUCTIONS,
            PERF_COUNT_HW_CACHE_MISSES,
            PERF_COUNT_HW_CACHE_REFERENCES,
            PERF_COUNT_HW_BRANCH_MISSES,
            PERF_COUNT_HW_BRANCH_INSTRUCTIONS,
        ];

        let mut members_vec = Vec::with_capacity(5);
        for &cfg in &configs {
            let attr = PerfEventAttr::new(PERF_TYPE_HARDWARE, cfg, false);
            members_vec.push(perf_event_open(&attr, leader.0)?);
        }

        let members: [PerfFd; 5] = match members_vec.try_into() {
            Ok(arr) => arr,
            Err(_) => unreachable!(),
        };

        Ok(Self { leader, members })
    }

    /// Starts all counters in the group.
    pub fn start(&mut self) -> io::Result<()> {
        let ret = unsafe {
            libc::ioctl(
                self.leader.0,
                PERF_EVENT_IOC_RESET as _,
                PERF_IOC_FLAG_GROUP,
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        let ret = unsafe {
            libc::ioctl(
                self.leader.0,
                PERF_EVENT_IOC_ENABLE as _,
                PERF_IOC_FLAG_GROUP,
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Stops all counters in the group.
    pub fn stop(&mut self) -> io::Result<()> {
        let ret = unsafe {
            libc::ioctl(
                self.leader.0,
                PERF_EVENT_IOC_DISABLE as _,
                PERF_IOC_FLAG_GROUP,
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Reads all counter values.
    pub fn read(&self) -> io::Result<PerfSnapshot> {
        fn read_counter(fd: i32) -> io::Result<u64> {
            let mut val: u64 = 0;
            let ret = unsafe {
                libc::read(
                    fd,
                    &mut val as *mut u64 as *mut libc::c_void,
                    std::mem::size_of::<u64>(),
                )
            };
            if ret < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(val)
        }

        Ok(PerfSnapshot {
            cycles: read_counter(self.leader.0)?,
            instructions: read_counter(self.members[0].0)?,
            cache_misses: read_counter(self.members[1].0)?,
            cache_references: read_counter(self.members[2].0)?,
            branch_misses: read_counter(self.members[3].0)?,
            branch_instructions: read_counter(self.members[4].0)?,
        })
    }
}
