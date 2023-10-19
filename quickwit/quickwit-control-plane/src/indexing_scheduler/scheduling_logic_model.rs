// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;

pub type SourceId = u32;
pub type NodeId = usize;
pub type Load = u32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Source {
    pub source_id: SourceId,
    pub load_per_shard: Load,
    pub num_shards: u32,
}

#[derive(Default, Debug)]
pub struct Problem {
    sources: Vec<Source>,
    node_max_loads: Vec<Load>,
}

impl Problem {
    pub fn new_solution(&self) -> Solution {
        Solution::with_num_nodes(self.node_max_loads.len())
    }

    pub fn node_max_load(&self, node_id: NodeId) -> Load {
        self.node_max_loads[node_id]
    }

    pub fn with_node_maximum_load(node_max_loads: Vec<Load>) -> Problem {
        Problem {
            sources: Vec::new(),
            node_max_loads,
        }
    }
    pub fn set_node_loads(&mut self, node_max_loads: Vec<Load>) {
        self.node_max_loads = node_max_loads;
    }

    pub fn sources(&self) -> impl Iterator<Item = Source> + '_ {
        self.sources.iter().copied()
    }

    pub fn source(&self, source_id: SourceId) -> Source {
        self.sources[source_id as usize]
    }

    pub fn add_source(&mut self, num_shards: u32, load_per_shard: Load) {
        let source_id = self.sources.len() as SourceId;
        self.sources.push(Source {
            source_id,
            num_shards,
            load_per_shard,
        });
    }

    pub fn source_load_per_shard(&self, source_id: SourceId) -> Load {
        self.sources[source_id as usize].load_per_shard
    }

    pub fn num_sources(&self) -> usize {
        self.sources.len()
    }

    pub fn num_nodes(&self) -> usize {
        self.node_max_loads.len()
    }
}

#[derive(Clone, Debug)]
pub struct NodeAssignment {
    pub node_id: NodeId,
    pub num_shards_per_source: BTreeMap<SourceId, u32>,
}

impl NodeAssignment {
    pub fn new(node_id: NodeId) -> NodeAssignment {
        NodeAssignment {
            node_id,
            num_shards_per_source: Default::default(),
        }
    }

    pub fn node_available_capacity(&self, problem: &Problem) -> Load {
        problem.node_max_loads[self.node_id as usize].saturating_sub(self.total_load(problem))
    }

    pub fn truncate_num_sources(&mut self, num_sources: usize) {
        while let Some((&source_id, _)) = self.num_shards_per_source.last_key_value() {
            if source_id >= num_sources as u32 {
                self.num_shards_per_source.pop_last();
            } else {
                break;
            }
        }
    }

    pub fn total_load(&self, problem: &Problem) -> Load {
        self.num_shards_per_source
            .iter()
            .map(|(source_id, num_shards)| problem.source_load_per_shard(*source_id) * num_shards)
            .sum()
    }

    pub fn num_shards(&self, source_id: SourceId) -> u32 {
        self.num_shards_per_source
            .get(&source_id)
            .copied()
            .unwrap_or(0u32)
    }

    pub fn add_shard(&mut self, source_id: u32, num_shards: u32) {
        *self.num_shards_per_source.entry(source_id).or_default() += num_shards;
    }

    pub fn remove_shards(&mut self, source_id: u32, num_shards_removed: u32) {
        let entry = self.num_shards_per_source.entry(source_id);
        let Entry::Occupied(mut occupied_entry) = entry else {
            assert_eq!(num_shards_removed, 0);
            return;
        };
        let previous_shard_count = *occupied_entry.get();
        assert!(previous_shard_count >= num_shards_removed);
        if previous_shard_count > num_shards_removed {
            *occupied_entry.get_mut() -= num_shards_removed
        } else {
            occupied_entry.remove();
        }
    }
}

#[derive(Clone, Debug)]
pub struct Solution {
    pub node_assignments: Vec<NodeAssignment>,
}

impl Solution {
    pub(crate) fn with_num_nodes(num_nodes: usize) -> Solution {
        Solution {
            node_assignments: (0..num_nodes).map(NodeAssignment::new).collect(),
        }
    }

    pub fn num_nodes(&self) -> usize {
        self.node_assignments.len()
    }
}
