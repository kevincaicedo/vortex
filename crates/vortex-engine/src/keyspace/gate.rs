use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;

use super::{ConcurrentKeyspace, TRANSACTION_GATE_COUNTERS};

#[derive(Debug)]
pub(super) struct TransactionGate {
    pub(super) active: AtomicBool,
    pub(super) readers: Box<[CachePadded<AtomicUsize>]>,
}

impl Default for TransactionGate {
    fn default() -> Self {
        let readers: Vec<CachePadded<AtomicUsize>> = (0..TRANSACTION_GATE_COUNTERS)
            .map(|_| CachePadded::new(AtomicUsize::new(0)))
            .collect();
        Self {
            active: AtomicBool::new(false),
            readers: readers.into_boxed_slice(),
        }
    }
}

pub struct CommandGateGuard<'a> {
    pub(super) reader: &'a CachePadded<AtomicUsize>,
}

pub struct TransactionGateGuard<'a> {
    pub(super) gate: &'a TransactionGate,
}

#[inline]
fn spin_or_yield(spins: &mut usize) {
    if *spins < 64 {
        *spins += 1;
        std::hint::spin_loop();
    } else {
        std::thread::yield_now();
    }
}

impl ConcurrentKeyspace {
    #[inline]
    pub fn enter_command_gate(&self) -> CommandGateGuard<'_> {
        self.enter_command_gate_slot(0)
    }

    #[inline]
    pub fn enter_command_gate_slot(&self, slot: usize) -> CommandGateGuard<'_> {
        let gate = &self.transaction_gate;
        let reader = &gate.readers[slot % gate.readers.len()];
        let mut spins = 0usize;
        loop {
            while gate.active.load(Ordering::Acquire) {
                spin_or_yield(&mut spins);
            }

            reader.fetch_add(1, Ordering::Acquire);
            if !gate.active.load(Ordering::Acquire) {
                return CommandGateGuard { reader };
            }

            reader.fetch_sub(1, Ordering::Release);
            spin_or_yield(&mut spins);
        }
    }

    #[inline]
    pub fn enter_transaction_gate(&self) -> TransactionGateGuard<'_> {
        let gate = &self.transaction_gate;
        let mut spins = 0usize;
        loop {
            if gate
                .active
                .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
            spin_or_yield(&mut spins);
        }

        for reader in gate.readers.iter() {
            while reader.load(Ordering::Acquire) != 0 {
                spin_or_yield(&mut spins);
            }
        }

        TransactionGateGuard { gate }
    }
}

impl Drop for CommandGateGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.reader.fetch_sub(1, Ordering::Release);
    }
}

impl Drop for TransactionGateGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.gate.active.store(false, Ordering::Release);
    }
}
