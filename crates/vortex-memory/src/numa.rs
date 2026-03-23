//! NUMA topology detection.
//!
//! On Linux, parses `/sys/devices/system/node/` to build a core-to-node
//! mapping. On macOS (and other platforms), returns a single-node topology.
//! Used by [`ReactorPool`] to allocate NUMA-local memory for each reactor.

/// NUMA topology of the current system.
#[derive(Debug, Clone)]
pub struct NumaTopology {
    num_nodes: usize,
    /// Maps core index → NUMA node. Cores beyond the length default to node 0.
    core_to_node: Vec<usize>,
}

impl NumaTopology {
    /// Detect the NUMA topology of the current machine.
    pub fn detect() -> Self {
        #[cfg(target_os = "linux")]
        {
            Self::detect_linux()
        }
        #[cfg(not(target_os = "linux"))]
        {
            Self::single_node()
        }
    }

    /// Fallback: single NUMA node containing all cores.
    fn single_node() -> Self {
        Self {
            num_nodes: 1,
            core_to_node: Vec::new(),
        }
    }

    /// Returns the NUMA node for the given core index.
    /// Returns 0 for unknown cores or single-node systems.
    pub fn node_for_core(&self, core_id: usize) -> usize {
        self.core_to_node.get(core_id).copied().unwrap_or(0)
    }

    /// Returns the total number of NUMA nodes.
    pub fn num_nodes(&self) -> usize {
        self.num_nodes
    }

    /// Returns the core-to-node mapping slice.
    pub fn core_to_node(&self) -> &[usize] {
        &self.core_to_node
    }

    /// Linux: parse `/sys/devices/system/node/nodeN/cpulist` files.
    #[cfg(target_os = "linux")]
    fn detect_linux() -> Self {
        use std::fs;
        use std::path::Path;

        let base = Path::new("/sys/devices/system/node");
        let Ok(entries) = fs::read_dir(base) else {
            return Self::single_node();
        };

        let mut max_core: usize = 0;
        let mut node_cpus: Vec<(usize, Vec<usize>)> = Vec::new();

        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if !name.starts_with("node") {
                continue;
            }
            let Ok(node_id) = name[4..].parse::<usize>() else {
                continue;
            };

            let cpulist_path = entry.path().join("cpulist");
            let Ok(contents) = fs::read_to_string(&cpulist_path) else {
                continue;
            };

            let cpus = parse_cpulist(&contents);
            if let Some(&m) = cpus.iter().max() {
                max_core = max_core.max(m);
            }
            node_cpus.push((node_id, cpus));
        }

        if node_cpus.is_empty() {
            return Self::single_node();
        }

        let num_nodes = node_cpus.iter().map(|(id, _)| *id + 1).max().unwrap_or(1);
        let mut core_to_node = vec![0usize; max_core + 1];
        for (node_id, cpus) in &node_cpus {
            for &cpu in cpus {
                if cpu < core_to_node.len() {
                    core_to_node[cpu] = *node_id;
                }
            }
        }

        Self {
            num_nodes,
            core_to_node,
        }
    }
}

/// Parse a Linux cpulist string like `"0-3,8-11"` into a `Vec<usize>`.
#[cfg(any(target_os = "linux", test))]
fn parse_cpulist(s: &str) -> Vec<usize> {
    let mut cpus = Vec::new();
    for part in s.trim().split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((start, end)) = part.split_once('-') {
            if let (Ok(s), Ok(e)) = (
                start.trim().parse::<usize>(),
                end.trim().parse::<usize>(),
            ) {
                cpus.extend(s..=e);
            }
        } else if let Ok(cpu) = part.parse::<usize>() {
            cpus.push(cpu);
        }
    }
    cpus
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cpulist_single() {
        assert_eq!(parse_cpulist("0"), vec![0]);
    }

    #[test]
    fn parse_cpulist_range() {
        assert_eq!(parse_cpulist("0-3"), vec![0, 1, 2, 3]);
    }

    #[test]
    fn parse_cpulist_mixed() {
        assert_eq!(parse_cpulist("0-2,5,8-10"), vec![0, 1, 2, 5, 8, 9, 10]);
    }

    #[test]
    fn parse_cpulist_with_whitespace() {
        assert_eq!(parse_cpulist("  0-1 , 3  \n"), vec![0, 1, 3]);
    }

    #[test]
    fn parse_cpulist_empty() {
        assert_eq!(parse_cpulist(""), Vec::<usize>::new());
    }

    #[test]
    fn single_node_topology() {
        let topo = NumaTopology::single_node();
        assert_eq!(topo.num_nodes(), 1);
        assert_eq!(topo.node_for_core(0), 0);
        assert_eq!(topo.node_for_core(999), 0);
    }

    #[test]
    fn detect_returns_at_least_one_node() {
        let topo = NumaTopology::detect();
        assert!(topo.num_nodes() >= 1);
    }
}
