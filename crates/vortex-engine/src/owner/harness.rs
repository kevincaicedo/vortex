//! Engine-only owner harness for the shared-nothing experiment.
//!
//! This module is deliberately separate from command execution and sockets. It
//! measures owner-local table access and one-hop owner thread handoff with
//! owned synthetic key/value payloads.

use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use vortex_common::{VortexKey, VortexValue};
use vortex_sync::{SpscReceiver, SpscSender, spsc_channel};

use crate::{ConcurrentKeyspace, SwissTable};

use super::{OwnerId, OwnerTopology, TopologyConfig, TopologyConfigError};

/// Synthetic command executed by the engine-only owner harness.
#[derive(Clone, Debug, PartialEq)]
pub enum OwnerHarnessCommand {
    /// Lookup a string key.
    Get { key: VortexKey },
    /// Replace or insert a string key.
    Set { key: VortexKey, value: VortexValue },
    /// Remove a string key.
    Del { key: VortexKey },
}

impl OwnerHarnessCommand {
    /// Creates a GET command.
    #[inline]
    pub const fn get(key: VortexKey) -> Self {
        Self::Get { key }
    }

    /// Creates a SET command.
    #[inline]
    pub const fn set(key: VortexKey, value: VortexValue) -> Self {
        Self::Set { key, value }
    }

    /// Creates a DEL command.
    #[inline]
    pub const fn del(key: VortexKey) -> Self {
        Self::Del { key }
    }

    /// Returns the command key.
    #[inline]
    pub fn key(&self) -> &VortexKey {
        match self {
            Self::Get { key } | Self::Set { key, .. } | Self::Del { key } => key,
        }
    }
}

/// Synthetic command result from the engine-only owner harness.
#[derive(Clone, Debug, PartialEq)]
pub enum OwnerHarnessReply {
    /// GET result.
    Value(Option<VortexValue>),
    /// SET previous value.
    Stored(Option<VortexValue>),
    /// DEL removed value.
    Deleted(Option<VortexValue>),
}

/// Completed owner-thread command result.
#[derive(Clone, Debug, PartialEq)]
pub struct OwnerHarnessResult {
    /// Owner that executed the command.
    pub owner: OwnerId,
    /// Command reply.
    pub reply: OwnerHarnessReply,
    /// Time between request publish and owner dequeue.
    pub queue_wait: Duration,
}

struct OwnerPartition {
    table: SwissTable,
}

impl OwnerPartition {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            table: SwissTable::with_capacity(capacity),
        }
    }

    #[inline]
    fn execute(&mut self, command: OwnerHarnessCommand) -> OwnerHarnessReply {
        match command {
            OwnerHarnessCommand::Get { key } => OwnerHarnessReply::Value(self.get(&key)),
            OwnerHarnessCommand::Set { key, value } => {
                OwnerHarnessReply::Stored(self.set(key, value))
            }
            OwnerHarnessCommand::Del { key } => OwnerHarnessReply::Deleted(self.del(&key)),
        }
    }

    #[inline]
    fn get(&self, key: &VortexKey) -> Option<VortexValue> {
        self.table.get(key).cloned()
    }

    #[inline]
    fn set(&mut self, key: VortexKey, value: VortexValue) -> Option<VortexValue> {
        self.table.insert(key, value)
    }

    #[inline]
    fn del(&mut self, key: &VortexKey) -> Option<VortexValue> {
        self.table.remove(key)
    }
}

/// Direct owner-table harness without sockets or shard locks.
pub struct EngineOwnerHarness {
    topology: OwnerTopology,
    owners: Vec<OwnerPartition>,
}

impl EngineOwnerHarness {
    /// Creates an owner harness with one local table per owner.
    ///
    /// # Errors
    ///
    /// Returns a topology validation error when `owner_count` or
    /// `capsule_count` is invalid.
    pub fn new(
        owner_count: usize,
        capsule_count: usize,
        capacity_per_owner: usize,
    ) -> Result<Self, TopologyConfigError> {
        let config = TopologyConfig::new(owner_count, capsule_count)?;
        Ok(Self::from_config(config, capacity_per_owner))
    }

    /// Creates an owner harness from a validated topology config.
    pub fn from_config(config: TopologyConfig, capacity_per_owner: usize) -> Self {
        let owners = (0..config.owner_count())
            .map(|_| OwnerPartition::with_capacity(capacity_per_owner))
            .collect();

        Self {
            topology: OwnerTopology::new(config),
            owners,
        }
    }

    /// Returns the harness topology config.
    #[inline]
    pub const fn config(&self) -> TopologyConfig {
        self.topology.config()
    }

    /// Returns an owner ID for a valid owner index.
    #[inline]
    pub fn owner_id(&self, index: usize) -> Option<OwnerId> {
        self.topology.owner_id(index)
    }

    /// Routes `key` to its owner.
    #[inline]
    pub fn route_owner(&self, key: &VortexKey) -> OwnerId {
        self.topology.route_key(key.as_bytes()).owner()
    }

    /// Inserts a key into the owner selected by the static route.
    #[inline]
    pub fn insert_routed(&mut self, key: VortexKey, value: VortexValue) -> Option<VortexValue> {
        let owner = self.route_owner(&key);
        self.set_local(owner, key, value)
    }

    /// Executes a command on its routed owner.
    #[inline]
    pub fn execute_routed(&mut self, command: OwnerHarnessCommand) -> OwnerHarnessResult {
        let owner = self.route_owner(command.key());
        let reply = self.execute_local(owner, command);
        OwnerHarnessResult {
            owner,
            reply,
            queue_wait: Duration::ZERO,
        }
    }

    /// Executes a command on a known owner-local table.
    #[inline]
    pub fn execute_local(
        &mut self,
        owner: OwnerId,
        command: OwnerHarnessCommand,
    ) -> OwnerHarnessReply {
        self.owner_mut(owner).execute(command)
    }

    /// Owner-local GET.
    #[inline]
    pub fn get_local(&self, owner: OwnerId, key: &VortexKey) -> Option<VortexValue> {
        self.owner(owner).get(key)
    }

    /// Owner-local SET.
    #[inline]
    pub fn set_local(
        &mut self,
        owner: OwnerId,
        key: VortexKey,
        value: VortexValue,
    ) -> Option<VortexValue> {
        self.owner_mut(owner).set(key, value)
    }

    /// Owner-local DEL.
    #[inline]
    pub fn del_local(&mut self, owner: OwnerId, key: &VortexKey) -> Option<VortexValue> {
        self.owner_mut(owner).del(key)
    }

    #[inline]
    fn owner(&self, owner: OwnerId) -> &OwnerPartition {
        &self.owners[owner.get()]
    }

    #[inline]
    fn owner_mut(&mut self, owner: OwnerId) -> &mut OwnerPartition {
        &mut self.owners[owner.get()]
    }
}

enum OwnerWorkerRequest {
    Execute {
        command: OwnerHarnessCommand,
        enqueued_at: Option<Instant>,
    },
    Shutdown,
}

struct OwnerWorkerReply {
    result: OwnerHarnessResult,
}

struct OwnerWorkerClient<const N: usize> {
    owner: OwnerId,
    request_tx: SpscSender<OwnerWorkerRequest, N>,
    reply_rx: SpscReceiver<OwnerWorkerReply, N>,
    join: Option<JoinHandle<()>>,
}

/// Owner-thread harness using bounded SPSC request and reply queues.
pub struct ThreadedOwnerHarness<const N: usize> {
    topology: OwnerTopology,
    workers: Vec<OwnerWorkerClient<N>>,
}

impl<const N: usize> ThreadedOwnerHarness<N> {
    /// Starts one owner worker thread per partition.
    pub fn start(harness: EngineOwnerHarness) -> Self {
        let config = harness.config();
        let topology = OwnerTopology::new(config);
        let mut workers = Vec::with_capacity(config.owner_count());

        for (index, partition) in harness.owners.into_iter().enumerate() {
            let owner = OwnerId::from_validated_index(index);
            let (request_tx, request_rx) = spsc_channel();
            let (reply_tx, reply_rx) = spsc_channel();
            let join = thread::Builder::new()
                .name(format!("vortex-owner-harness-{index}"))
                .spawn(move || owner_worker_loop(owner, partition, request_rx, reply_tx))
                .expect("owner harness worker thread spawned");

            workers.push(OwnerWorkerClient {
                owner,
                request_tx,
                reply_rx,
                join: Some(join),
            });
        }

        Self { topology, workers }
    }

    /// Executes a command on the routed owner without queue-wait timing.
    #[inline]
    pub fn execute(&self, command: OwnerHarnessCommand) -> OwnerHarnessResult {
        self.execute_with_timing(command, false)
    }

    /// Executes a command on the routed owner and records owner queue wait.
    #[inline]
    pub fn execute_timed(&self, command: OwnerHarnessCommand) -> OwnerHarnessResult {
        self.execute_with_timing(command, true)
    }

    #[inline]
    fn execute_with_timing(&self, command: OwnerHarnessCommand, timed: bool) -> OwnerHarnessResult {
        let owner = self.topology.route_key(command.key().as_bytes()).owner();
        let client = &self.workers[owner.get()];
        debug_assert_eq!(client.owner, owner);
        let enqueued_at = timed.then(Instant::now);
        let mut request = OwnerWorkerRequest::Execute {
            command,
            enqueued_at,
        };

        loop {
            match client.request_tx.try_send(request) {
                Ok(()) => break,
                Err(rejected) => {
                    request = rejected;
                    std::hint::spin_loop();
                }
            }
        }

        loop {
            if let Some(reply) = client.reply_rx.try_recv() {
                return reply.result;
            }
            std::hint::spin_loop();
        }
    }
}

impl<const N: usize> Drop for ThreadedOwnerHarness<N> {
    fn drop(&mut self) {
        for client in &self.workers {
            let mut request = OwnerWorkerRequest::Shutdown;
            loop {
                match client.request_tx.try_send(request) {
                    Ok(()) => break,
                    Err(rejected) => {
                        request = rejected;
                        std::hint::spin_loop();
                    }
                }
            }
        }

        for client in &mut self.workers {
            if let Some(join) = client.join.take() {
                join.join().expect("owner harness worker exits");
            }
        }
    }
}

fn owner_worker_loop<const N: usize>(
    owner: OwnerId,
    mut partition: OwnerPartition,
    request_rx: SpscReceiver<OwnerWorkerRequest, N>,
    reply_tx: SpscSender<OwnerWorkerReply, N>,
) {
    loop {
        let Some(request) = request_rx.try_recv() else {
            std::hint::spin_loop();
            continue;
        };

        match request {
            OwnerWorkerRequest::Execute {
                command,
                enqueued_at,
            } => {
                let queue_wait = enqueued_at.map_or(Duration::ZERO, |started| started.elapsed());
                let reply = partition.execute(command);
                let result = OwnerHarnessResult {
                    owner,
                    reply,
                    queue_wait,
                };
                let mut response = OwnerWorkerReply { result };

                loop {
                    match reply_tx.try_send(response) {
                        Ok(()) => break,
                        Err(rejected) => {
                            response = rejected;
                            std::hint::spin_loop();
                        }
                    }
                }
            }
            OwnerWorkerRequest::Shutdown => break,
        }
    }
}

/// Benchmark helper for shared-keyspace GET using the same raw table semantics.
#[inline]
pub fn shared_keyspace_get(keyspace: &ConcurrentKeyspace, key: &VortexKey) -> Option<VortexValue> {
    keyspace.read(key.as_bytes(), |table| table.get(key).cloned())
}

/// Benchmark helper for shared-keyspace SET using the same raw table semantics.
#[inline]
pub fn shared_keyspace_set(
    keyspace: &ConcurrentKeyspace,
    key: VortexKey,
    value: VortexValue,
) -> Option<VortexValue> {
    let mut guard = keyspace.write_shard_by_index(keyspace.shard_index(key.as_bytes()));
    guard.insert(key, value)
}

/// Benchmark helper for shared-keyspace DEL using the same raw table semantics.
#[inline]
pub fn shared_keyspace_del(keyspace: &ConcurrentKeyspace, key: &VortexKey) -> Option<VortexValue> {
    let mut guard = keyspace.write_shard_by_index(keyspace.shard_index(key.as_bytes()));
    guard.remove(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(index: usize) -> VortexKey {
        VortexKey::from(format!("sn003-key:{index:04}"))
    }

    fn value(index: usize) -> VortexValue {
        VortexValue::Integer(index as i64)
    }

    #[test]
    fn owner_harness_matches_shared_keyspace_get_set_del() {
        let mut owner_harness = EngineOwnerHarness::new(4, 64, 32).expect("valid harness");
        let shared = ConcurrentKeyspace::with_capacity(64, 128);

        let commands = [
            OwnerHarnessCommand::set(key(1), value(10)),
            OwnerHarnessCommand::get(key(1)),
            OwnerHarnessCommand::set(key(1), value(11)),
            OwnerHarnessCommand::get(key(1)),
            OwnerHarnessCommand::del(key(1)),
            OwnerHarnessCommand::get(key(1)),
            OwnerHarnessCommand::del(key(2)),
            OwnerHarnessCommand::set(key(2), value(20)),
            OwnerHarnessCommand::get(key(2)),
        ];

        for command in commands {
            let owner_reply = owner_harness.execute_routed(command.clone()).reply;
            let shared_reply = match command {
                OwnerHarnessCommand::Get { key } => {
                    OwnerHarnessReply::Value(shared_keyspace_get(&shared, &key))
                }
                OwnerHarnessCommand::Set { key, value } => {
                    OwnerHarnessReply::Stored(shared_keyspace_set(&shared, key, value))
                }
                OwnerHarnessCommand::Del { key } => {
                    OwnerHarnessReply::Deleted(shared_keyspace_del(&shared, &key))
                }
            };

            assert_eq!(owner_reply, shared_reply);
        }
    }

    #[test]
    fn threaded_owner_harness_executes_remote_round_trip() {
        let mut owner_harness = EngineOwnerHarness::new(4, 64, 32).expect("valid harness");
        owner_harness.insert_routed(key(7), value(70));
        let threaded = ThreadedOwnerHarness::<64>::start(owner_harness);

        let get = threaded.execute_timed(OwnerHarnessCommand::get(key(7)));
        assert_eq!(get.reply, OwnerHarnessReply::Value(Some(value(70))));
        assert!(get.queue_wait > Duration::ZERO);

        let set = threaded.execute(OwnerHarnessCommand::set(key(7), value(71)));
        assert_eq!(set.reply, OwnerHarnessReply::Stored(Some(value(70))));

        let del = threaded.execute(OwnerHarnessCommand::del(key(7)));
        assert_eq!(del.reply, OwnerHarnessReply::Deleted(Some(value(71))));
    }
}
