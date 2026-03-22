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
    /// Group leader fd (cycles counter).
    leader: PerfFd,
    /// Member fds: instructions, cache_misses, cache_refs, branch_misses, branch_insns
    members: [PerfFd; 5],
}

fn perf_event_open(attr: &libc::perf_event_attr, group_fd: i32) -> io::Result<PerfFd> {
    let fd = unsafe {
        libc::syscall(
            libc::SYS_perf_event_open,
            attr as *const libc::perf_event_attr,
            0i32,  // pid: current process
            -1i32, // cpu: any CPU
            group_fd,
            libc::PERF_FLAG_FD_CLOEXEC as u64,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(PerfFd(fd as i32))
}

fn make_attr(type_: u32, config: u64, disabled: bool) -> libc::perf_event_attr {
    let mut attr: libc::perf_event_attr = unsafe { std::mem::zeroed() };
    attr.type_ = type_;
    attr.size = std::mem::size_of::<libc::perf_event_attr>() as u32;
    attr.config = config;
    // Bitfield flags — set via the __bindgen fields
    attr.set_disabled(disabled as u64);
    attr.set_exclude_kernel(1);
    attr.set_exclude_hv(1);
    attr
}

impl PerfCounterGroup {
    /// Creates a new counter group. Returns `Err` if perf is unavailable.
    pub fn new() -> io::Result<Self> {
        let hw = libc::PERF_TYPE_HARDWARE;

        // Leader: cycles
        let leader_attr = make_attr(hw, libc::PERF_COUNT_HW_CPU_CYCLES as u64, true);
        let leader = perf_event_open(&leader_attr, -1)?;

        let configs = [
            libc::PERF_COUNT_HW_INSTRUCTIONS as u64,
            libc::PERF_COUNT_HW_CACHE_MISSES as u64,
            libc::PERF_COUNT_HW_CACHE_REFERENCES as u64,
            libc::PERF_COUNT_HW_BRANCH_MISSES as u64,
            libc::PERF_COUNT_HW_BRANCH_INSTRUCTIONS as u64,
        ];

        let mut members_vec = Vec::with_capacity(5);
        for &cfg in &configs {
            let attr = make_attr(hw, cfg, false);
            members_vec.push(perf_event_open(&attr, leader.0)?);
        }

        // Convert Vec to fixed-size array
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
                libc::PERF_EVENT_IOC_RESET as _,
                libc::PERF_IOC_FLAG_GROUP,
            )
        };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        let ret = unsafe {
            libc::ioctl(
                self.leader.0,
                libc::PERF_EVENT_IOC_ENABLE as _,
                libc::PERF_IOC_FLAG_GROUP,
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
                libc::PERF_EVENT_IOC_DISABLE as _,
                libc::PERF_IOC_FLAG_GROUP,
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
