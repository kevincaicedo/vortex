use super::*;

impl ConcurrentKeyspace {
    pub(crate) fn cmd_dbsize(&self, now_nanos: u64) -> usize {
        let (keys, _) = self.exact_keyspace_counts(now_nanos);
        keys
    }

    pub(crate) fn cmd_flush_all(&self) -> Option<AofLsn> {
        self.flush_all_with_lsn()
    }

    pub(crate) fn info_keyspace(&self, now_nanos: u64) -> (usize, usize) {
        self.exact_keyspace_counts(now_nanos)
    }
}
