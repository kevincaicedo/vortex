use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use smallvec::SmallVec;

use super::{ConcurrentKeyspace, ShardPlan};

#[derive(Debug)]
pub(super) struct TransactionGate {
    pub(super) active: AtomicBool,
    pub(super) readers: AtomicUsize,
}

impl Default for TransactionGate {
    fn default() -> Self {
        Self {
            active: AtomicBool::new(false),
            readers: AtomicUsize::new(0),
        }
    }
}

pub struct CommandGateGuard<'a> {
    pub(super) gates: SmallVec<[&'a TransactionGate; 16]>,
}

pub struct TransactionGateGuard<'a> {
    pub(super) gates: SmallVec<[&'a TransactionGate; 16]>,
}

#[derive(Clone, Debug, Default)]
pub struct TransactionGatePlan {
    sorted_shards: SmallVec<[usize; 16]>,
}

impl TransactionGatePlan {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.sorted_shards.is_empty()
    }

    #[inline]
    pub fn shard_count(&self) -> usize {
        self.sorted_shards.len()
    }

    #[inline]
    pub fn merge(&mut self, other: Self) {
        self.sorted_shards.extend(other.sorted_shards);
        self.sorted_shards.sort_unstable();
        self.sorted_shards.dedup();
    }
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

#[inline]
fn enter_reader(gate: &TransactionGate, spins: &mut usize) {
    loop {
        while gate.active.load(Ordering::Acquire) {
            spin_or_yield(spins);
        }

        gate.readers.fetch_add(1, Ordering::Acquire);
        if !gate.active.load(Ordering::Acquire) {
            return;
        }

        gate.readers.fetch_sub(1, Ordering::Release);
        spin_or_yield(spins);
    }
}

#[inline]
fn enter_writer(gate: &TransactionGate, spins: &mut usize) {
    loop {
        if gate
            .active
            .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            break;
        }
        spin_or_yield(spins);
    }

    while gate.readers.load(Ordering::Acquire) != 0 {
        spin_or_yield(spins);
    }
}

impl ConcurrentKeyspace {
    #[inline]
    pub fn enter_command_gate(&self) -> CommandGateGuard<'_> {
        self.enter_command_gate_slot(0)
    }

    #[inline]
    pub fn enter_command_gate_slot(&self, slot: usize) -> CommandGateGuard<'_> {
        self.enter_all_shard_command_gate(slot)
    }

    #[inline]
    pub fn enter_command_gate_for_key(&self, key: &[u8], slot: usize) -> CommandGateGuard<'_> {
        let shard = self.shard_id(key);
        let mut gates = SmallVec::new();
        gates.push(&*self.transaction_gates[shard.get()]);
        self.enter_command_gates(gates, slot)
    }

    #[inline]
    pub fn enter_command_gate_for_keys(&self, keys: &[&[u8]], slot: usize) -> CommandGateGuard<'_> {
        let gates = self.transaction_gates_for_keys(keys);
        self.enter_command_gates(gates, slot)
    }

    #[inline]
    pub fn enter_all_shard_command_gate(&self, slot: usize) -> CommandGateGuard<'_> {
        let gates = self.all_transaction_gates();
        self.enter_command_gates(gates, slot)
    }

    #[inline]
    fn enter_command_gates<'a>(
        &self,
        gates: SmallVec<[&'a TransactionGate; 16]>,
        _slot: usize,
    ) -> CommandGateGuard<'a> {
        let mut spins = 0usize;
        let mut acquired = SmallVec::with_capacity(gates.len());
        for gate in &gates {
            enter_reader(gate, &mut spins);
            acquired.push(*gate);
        }
        CommandGateGuard { gates: acquired }
    }

    #[inline]
    pub fn enter_transaction_gate(&self) -> TransactionGateGuard<'_> {
        self.enter_all_shard_transaction_gate()
    }

    #[inline]
    pub fn enter_transaction_gate_for_keys(&self, keys: &[&[u8]]) -> TransactionGateGuard<'_> {
        let plan = self.transaction_gate_plan_for_keys(keys);
        self.enter_transaction_gate_for_plan(&plan)
    }

    #[inline]
    pub fn enter_transaction_gate_for_plan(
        &self,
        plan: &TransactionGatePlan,
    ) -> TransactionGateGuard<'_> {
        let gates = self.transaction_gates_for_plan(plan);
        self.enter_transaction_gates(gates)
    }

    #[inline]
    pub fn enter_all_shard_transaction_gate(&self) -> TransactionGateGuard<'_> {
        let gates = self.all_transaction_gates();
        self.enter_transaction_gates(gates)
    }

    #[inline]
    fn enter_transaction_gates<'a>(
        &self,
        gates: SmallVec<[&'a TransactionGate; 16]>,
    ) -> TransactionGateGuard<'a> {
        let mut spins = 0usize;
        let mut acquired = SmallVec::with_capacity(gates.len());
        for gate in gates {
            enter_writer(gate, &mut spins);
            acquired.push(gate);
        }
        TransactionGateGuard { gates: acquired }
    }

    #[inline]
    fn transaction_gates_for_keys(&self, keys: &[&[u8]]) -> SmallVec<[&TransactionGate; 16]> {
        if keys.is_empty() {
            return SmallVec::new();
        }

        let plan = self.transaction_gate_plan_for_keys(keys);
        self.transaction_gates_for_plan(&plan)
    }

    #[inline]
    pub fn transaction_gate_plan_for_keys(&self, keys: &[&[u8]]) -> TransactionGatePlan {
        if keys.is_empty() {
            return TransactionGatePlan::default();
        }

        let plan = ShardPlan::new(self, keys);
        let mut sorted_shards = SmallVec::with_capacity(plan.sorted_shards().len());
        for shard in plan.sorted_shards() {
            sorted_shards.push(shard.get());
        }
        TransactionGatePlan { sorted_shards }
    }

    #[inline]
    fn transaction_gates_for_plan(
        &self,
        plan: &TransactionGatePlan,
    ) -> SmallVec<[&TransactionGate; 16]> {
        let mut gates = SmallVec::with_capacity(plan.sorted_shards.len());
        for shard in &plan.sorted_shards {
            gates.push(&*self.transaction_gates[*shard]);
        }
        gates
    }

    #[inline]
    fn all_transaction_gates(&self) -> SmallVec<[&TransactionGate; 16]> {
        let mut gates = SmallVec::with_capacity(self.transaction_gates.len());
        for gate in self.transaction_gates.iter() {
            gates.push(&**gate);
        }
        gates
    }
}

impl Drop for CommandGateGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        for gate in &self.gates {
            gate.readers.fetch_sub(1, Ordering::Release);
        }
    }
}

impl Drop for TransactionGateGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        for gate in self.gates.iter().rev() {
            gate.active.store(false, Ordering::Release);
        }
    }
}
