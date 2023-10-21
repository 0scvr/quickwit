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

mod scheduling_logic;
pub(crate) mod scheduling_logic_model;

use quickwit_proto::{IndexUid, ShardId};
use scheduling_logic_model::SourceOrd;
use scheduling_logic_model::NodeOrd;

use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use fnv::{FnvHashMap, FnvHashSet};
use itertools::Itertools;
use quickwit_metastore::Metastore;
use quickwit_proto::indexing::{ApplyIndexingPlanRequest, IndexingService, IndexingTask};
use quickwit_proto::NodeId;
use quickwit_proto::metastore::SourceType;
use serde::Serialize;
use tracing::{debug, error, info, warn};

use crate::control_plane_model::ControlPlaneModel;
use crate::indexing_plan::PhysicalIndexingPlan;
use crate::indexing_scheduler::scheduling_logic_model::SchedulingSolution;
use crate::indexing_scheduler::scheduling_logic_model::{SchedulingProblem, Load};
use crate::{IndexerNodeInfo, IndexerPool, SourceUid};

const PIPELINE_FULL_LOAD: Load = 1_000u32;
const LOAD_PER_NODE: Load = 4_000u32;

pub(crate) const MIN_DURATION_BETWEEN_SCHEDULING: Duration =
    if cfg!(any(test, feature = "testsuite")) {
        Duration::from_millis(50)
    } else {
        Duration::from_secs(30)
    };

#[derive(Debug, Clone, Default, Serialize)]
pub struct IndexingSchedulerState {
    pub num_applied_physical_indexing_plan: usize,
    pub num_schedule_indexing_plan: usize,
    pub last_applied_physical_plan: Option<PhysicalIndexingPlan>,
    #[serde(skip)]
    pub last_applied_plan_timestamp: Option<Instant>,
}

fn create_shard_to_node_map(physical_plan: &PhysicalIndexingPlan,
                            id_to_ord_map: &IdToOrdMap) -> FnvHashMap<SourceOrd, FnvHashMap<ShardId, NodeOrd>> {
    let mut source_to_shard_to_node: FnvHashMap<SourceOrd, FnvHashMap<ShardId, NodeOrd>> = Default::default();
    for (node_id, indexing_tasks) in physical_plan.indexing_tasks_per_node().iter() {
        for indexing_task in indexing_tasks {
            let index_uid = IndexUid::from(indexing_task.index_uid.clone());
            let Some(indexer_ord) = id_to_ord_map.indexer_ord(node_id) else { continue; };
            let source_uid = SourceUid {
                index_uid,
                source_id: indexing_task.source_id.clone(),
            };
            let Some(source_ord) = id_to_ord_map.source_ord(&source_uid) else {
                continue;
            };
            for &shard_id in &indexing_task.shard_ids {
                source_to_shard_to_node.entry(source_ord)
                    .or_default()
                    .insert(shard_id, indexer_ord);
            }
        }
    }
    source_to_shard_to_node
}

/// The [`IndexingScheduler`] is responsible for listing indexing tasks and assiging them to
/// indexers.
/// We call this duty `scheduling`. Contrary to what the name suggests, most indexing tasks are
/// ever running. We just borrowed the terminology to Kubernetes.
///
/// Scheduling executes the following steps:
/// 1. List all of the logical indexing tasks, from the model. (See [`list_indexing_tasks`])
/// 2. Builds a [`PhysicalIndexingPlan`] from the list of logical indexing tasks. See
///    [`build_physical_indexing_plan`] for the implementation details.
/// 3. Apply the [`PhysicalIndexingPlan`]: for each indexer, the scheduler send the indexing tasks
///    by gRPC. An indexer immediately returns an Ok and apply asynchronously the received plan. Any
///    errors (network) happening in this step are ignored. The scheduler runs a control loop that
///    regularly checks if indexers are effectively running their plans (more details in the next
///    section).
///
/// All events altering the list of indexes and sources are proxied through
/// through the control plane. The control plane model is therefore guaranteed to be up-to-date
/// (at the cost of making the control plane a single point of failure).
///
/// Each change to the model triggers the production of a new `PhysicalIndexingPlan`.
///
/// A `ControlPlanLoop` event is scheduled every `CONTROL_PLAN_LOOP_INTERVAL` and steers
/// the cluster toward the last applied [`PhysicalIndexingPlan`].
///
/// This physical plan is a desired state. Even after that state is reached, it can be altered due
/// to faulty server for instance.
///
/// We then need to detect deviation, possibly recompute the desired `PhysicalIndexingPlan`
/// and steer back the cluster to the right state.
///
/// First to detect deviation, the control plan gathers an eventually consistent view of what is
/// running on the different nodes of the cluster: the `running plan`. This is done via `chitchat`.
///
/// If the list of node ids has changed, the scheduler will retrigger a scheduling.
/// If the indexing tasks do not match, the scheduler will apply again the last applied plan.
/// Concretely, it will send the faulty nodes of the plan they are supposed to follow.
//
/// Finally, in order to give the time for each indexer to run their indexing tasks, the control
/// plane will wait at least [`MIN_DURATION_BETWEEN_SCHEDULING`] before comparing the desired
/// plan with the running plan.
pub struct IndexingScheduler {
    cluster_id: String,
    self_node_id: NodeId,
    metastore: Arc<dyn Metastore>,
    indexer_pool: IndexerPool,
    state: IndexingSchedulerState,
}

impl fmt::Debug for IndexingScheduler {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("IndexingScheduler")
            .field("cluster_id", &self.cluster_id)
            .field("node_id", &self.self_node_id)
            .field("metastore_uri", &self.metastore.uri())
            .field(
                "last_applied_plan_ts",
                &self.state.last_applied_plan_timestamp,
            )
            .finish()
    }
}


fn populate_problem(source: &SourceToSchedule, problem: &mut SchedulingProblem) -> Option<SourceOrd> {
    match &source.source_type {
        SourceToScheduleType::IngestV1 => {
            // TODO ingest v1 is scheduled differently
            None
        }
        SourceToScheduleType::Sharded { shards, load_per_shard }=> {
            let num_shards = shards.len() as u32;
            let source_id = problem.add_source(num_shards, *load_per_shard);
            Some(source_id)
        }
        SourceToScheduleType::NonSharded { num_pipelines, load_per_pipeline } => {
            let source_id = problem.add_source(*num_pipelines, *load_per_pipeline);
            Some(source_id)
        }
    }
}

#[derive(Default)]
struct IdToOrdMap {
    indexer_uids: Vec<String>,
    source_uids: Vec<SourceUid>,
    indexer_uid_to_indexer_ord: FnvHashMap<String, NodeOrd>,
    source_uid_to_source_ord: FnvHashMap<SourceUid, SourceOrd>,
}

impl IdToOrdMap {
    pub fn add_source_uid(&mut self, source_uid: SourceUid) -> SourceOrd {
        let source_ord = self.source_uid_to_source_ord.len() as SourceOrd;
        self.source_uid_to_source_ord.insert(source_uid.clone(), source_ord);
        self.source_uids.push(source_uid);
        source_ord
    }

    pub fn source_ord(&self, source_uid: &SourceUid) -> Option<SourceOrd> {
        self.source_uid_to_source_ord.get(source_uid).copied()
    }
    pub fn indexer_id(&self, node_ord: NodeOrd) -> &String {
        &self.indexer_uids[node_ord]
    }

    pub fn indexer_ord(&self, indexer_uid: &str) -> Option<NodeOrd> {
        self.indexer_uid_to_indexer_ord.get(indexer_uid).copied()
    }

    pub fn add_indexer_id(&mut self, node_id: String) -> NodeOrd {
        let indexer_ord = self.indexer_uids.len() as NodeOrd;
        self.indexer_uid_to_indexer_ord.insert(node_id.clone(), indexer_ord);
        self.indexer_uids.push(node_id);
        indexer_ord
    }
}


fn convert_physical_plan_to_solution(plan: &PhysicalIndexingPlan, id_to_ord_map: &IdToOrdMap, solution: &mut SchedulingSolution) {
    for (indexer_id, indexing_tasks) in plan.indexing_tasks_per_node() {
        if let Some(node_ord) = id_to_ord_map.indexer_ord(indexer_id) {
            let node_assignment = &mut solution.node_assignments[node_ord as usize];
            for indexing_task in indexing_tasks {
                let source_uid = SourceUid {
                    index_uid: IndexUid::from(indexing_task.index_uid.clone()),
                    source_id: indexing_task.source_id.clone(),
                };
                if let Some(source_ord) = id_to_ord_map.source_ord(&source_uid) {
                    node_assignment.add_shard(source_ord, indexing_task.shard_ids.len() as u32);
                }
            }
        }
    }
}

/// Spreads the list of shard_ids optimally amongst the different nodes.
/// This function also receives a `previous_shard_to_node_ord` map, informing
/// use of the previous configuration.
///
/// Whenever possible this function tries to keep shards on the same node.
///
/// Contract:
/// The sum of the number of shards (values of node_num_shards) should match
/// the length of shard_ids.
/// Note that all shards are not necessarily in previous_shard_to_node_ord.
fn spread_shards_optimally(shard_ids: &[ShardId],
                           mut node_num_shards: FnvHashMap<NodeOrd, NonZeroU32>,
                           previous_shard_to_node_ord: FnvHashMap<ShardId, NodeOrd>,
) -> FnvHashMap<NodeOrd, Vec<ShardId>> {
    assert_eq!(
        shard_ids.len(),
        node_num_shards.values().map(|num_shards| num_shards.get() as usize).sum::<usize>(),
    );
    let mut shard_ids_per_node: FnvHashMap<NodeOrd, Vec<ShardId>> = Default::default();
    // let shard_ids: Vec<ShardId> = model.list_shards(source_uid);
    // let shard_to_node_ord = previous_shard_to_node_map.remove(&source_ord).unwrap_or_default();
    let mut unassigned_shard_ids: Vec<ShardId> = Vec::new();
    for &shard_id in shard_ids {
        if let Some(previous_node_ord) = previous_shard_to_node_ord.get(&shard_id).cloned() {
            if let Entry::Occupied(mut num_shards_entry) = node_num_shards.entry(previous_node_ord) {
                if let Some(new_num_shards) = NonZeroU32::new(num_shards_entry.get().get() - 1u32) {
                    *num_shards_entry.get_mut() = new_num_shards;
                } else {
                    num_shards_entry.remove();
                }
                // We keep the shard on the node it used to be.
                shard_ids_per_node.entry(previous_node_ord)
                    .or_default()
                    .push(shard_id);
                continue;
            }
        }
        unassigned_shard_ids.push(shard_id);
    }

    // Finally, we need to add the missing shards.
    for (node, num_shards) in node_num_shards {
        assert!(unassigned_shard_ids.len() >= num_shards.get() as usize);
        shard_ids_per_node.entry(node)
            .or_default()
            .extend(unassigned_shard_ids.drain(..num_shards.get() as usize));
    }

    // At this point, we should have applied all of the missing shards.
    assert!(unassigned_shard_ids.is_empty());

    shard_ids_per_node
}





struct SourceToSchedule {
    source_uid: SourceUid,
    source_type: SourceToScheduleType,
}

enum SourceToScheduleType {
    Sharded {
        shards: Vec<ShardId>,
        load_per_shard: Load,
    },
    NonSharded {
        num_pipelines: u32,
        load_per_pipeline: Load,
    },
    // deprecated
    IngestV1,
}


fn get_sources_to_schedule(model: &ControlPlaneModel) -> Vec<SourceToSchedule> {
    let mut sources = Vec::new();
    for (source_uid, source_config) in model.get_source_configs() {
        if !source_config.enabled {
            continue;
        }
        match source_config.source_type() {
            SourceType::Cli | SourceType::File | SourceType::Vec | SourceType::Void => {
                // We don't need to schedule those.
            }
            SourceType::IngestV1 => {
                // TODO ingest v1 is scheduled differently
                sources.push(SourceToSchedule {
                    source_uid,
                    source_type: SourceToScheduleType::IngestV1,
                });
            }
            SourceType::IngestV2 => {
                let shards = model.list_shards(&source_uid);
                sources.push(SourceToSchedule {
                    source_uid,
                    source_type: SourceToScheduleType::Sharded {
                        shards,
                        // FIXME
                        load_per_shard: 250u32,
                    },
                });
            }
            SourceType::Kafka
            | SourceType::Kinesis
            | SourceType::GcpPubsub
            | SourceType::Nats
            | SourceType::Pulsar => {
                sources.push(SourceToSchedule {
                    source_uid,
                    source_type: SourceToScheduleType::NonSharded {
                        num_pipelines: source_config.desired_num_pipelines.get() as u32,
                        // FIXME
                        load_per_pipeline: PIPELINE_FULL_LOAD ,
                    },
                });
            }
        }
    }
    sources
}

fn convert_scheduling_solution_to_physical_plan(solution: SchedulingSolution,
                                                id_to_ord_map: &IdToOrdMap,
                                                sources: &[SourceToSchedule],
                                                previous_plan_opt: Option<&PhysicalIndexingPlan>,
) -> PhysicalIndexingPlan {
    let mut previous_shard_to_node_map: FnvHashMap<SourceOrd, FnvHashMap<ShardId, NodeOrd>> = previous_plan_opt
        .map(|previous_plan| {
            create_shard_to_node_map(previous_plan, id_to_ord_map)
        })
        .unwrap_or_default();


    let mut physical_indexing_plan = PhysicalIndexingPlan::default();

    for source in sources {
        let source_ord = id_to_ord_map.source_ord(&source.source_uid).unwrap();
        let node_num_shards: FnvHashMap<NodeOrd, NonZeroU32> = solution.node_shards(source_ord).collect();

        match &source.source_type  {
            SourceToScheduleType::Sharded { shards, load_per_shard: _load_per_shard } => {
                // That's ingest v2.
                // The logic is complicated here. At this point we know the number of shards to
                // be assign to each node, but we want to convert that number into a list of shard
                // ids, without moving a shard from a node to another whenever possible.
                let shard_to_node_ord = previous_shard_to_node_map.remove(&source_ord).unwrap_or_default();
                let shard_ids_per_node = spread_shards_optimally(&shards,
                    node_num_shards,
                    shard_to_node_ord);

                for (node_ord, shard_ids_for_node) in shard_ids_per_node {
                    let node_id = id_to_ord_map.indexer_id(node_ord);
                    let indexing_task = IndexingTask {
                        index_uid: source.source_uid.index_uid.to_string(),
                        source_id: source.source_uid.source_id.to_string(),
                        shard_ids: shard_ids_for_node,
                    };
                    physical_indexing_plan.add_indexing_task(node_id, indexing_task);
                }
            }
            SourceToScheduleType::NonSharded { .. } => {
                // These are the sources that are not sharded (Kafka-like).
                //
                // Here one shard is one pipeline.
                for (node_ord, num_shards) in node_num_shards {
                    let node_id = id_to_ord_map.indexer_id(node_ord);
                    for _ in 0..num_shards.get() {
                        let indexing_task = IndexingTask {
                            index_uid: source.source_uid.index_uid.to_string(),
                            source_id: source.source_uid.source_id.to_string(),
                            shard_ids: Vec::new(),
                        };
                        physical_indexing_plan.add_indexing_task(node_id, indexing_task);
                    }
                }
                continue;
            }
            SourceToScheduleType::IngestV1 => {
                // Ingest V1 requires to start one pipeline on each node.
                // This pipeline is off-the-grid: it is not taken in account in the
                // node capacity. We start it to ensure backward compatibility
                // a little, but we want to remove it rapidly.
                for node_id in &id_to_ord_map.indexer_uids {
                    let indexing_task = IndexingTask {
                        index_uid: source.source_uid.index_uid.to_string(),
                        source_id: source.source_uid.source_id.to_string(),
                        shard_ids: Vec::new(),
                    };
                    physical_indexing_plan.add_indexing_task(node_id, indexing_task);
                }
            }
        }

    }

    physical_indexing_plan
}


/// Creates a physical plan given the current situation of the cluster and the list of sources
/// to schedule.
///
/// The scheduling problem abstracts all notion of shard ids, source types, and node_ids,
/// to transform scheduling into a math problem.
///
/// This function implementation therefore goes
/// - 1) transform our problem into a scheduling problem. Something closer to a well-defined
/// optimization problem. In particular this step removes:
///   - the notion of shard ids, and only considers a number of shards being allocated.
///   - node_ids and shard ids. These are replaced by integers.
/// - 2) convert the current situation of the cluster into something a previous scheduling
/// solution.
/// - 3) compute the new scheduling solution.
/// - 4) convert the new scheduling solution back to the real world by reallocating the shard ids.
///
/// TODO cut into pipelines.
fn build_physical_indexing_plan(sources: &[SourceToSchedule],
                                indexer_max_load: &FnvHashMap<String, Load>,
                                previous_plan_opt: Option<&PhysicalIndexingPlan>) -> PhysicalIndexingPlan {
    /// TODO make the load per node something that can be configured on each node.

    // Convert our problem to a scheduling problem.
    let mut id_to_ord_map = IdToOrdMap::default();

    let mut indexer_max_loads: Vec<Load> = Vec::with_capacity(indexer_max_load.len());
    for (indexer_id, &max_load) in indexer_max_load {
        let indexer_ord = id_to_ord_map.add_indexer_id(indexer_id.clone());
        assert_eq!(indexer_ord, indexer_max_loads.len() as NodeOrd);
        // TODO fix me: we should get the load from the indexer config. conveyed via chitchat.
        indexer_max_loads.push(max_load);
    }

    let mut problem = SchedulingProblem::with_node_maximum_load(indexer_max_loads);

    for source in sources {
        if let Some(source_id) = populate_problem(&source, &mut problem) {
            let source_ord = id_to_ord_map.add_source_uid(source.source_uid.clone());
            assert_eq!(source_ord, source_id);
        }
    }

    // Populate the previous solution.
    let mut previous_solution = problem.new_solution();
    if let Some(previous_plan) = previous_plan_opt {
        convert_physical_plan_to_solution(previous_plan, &id_to_ord_map, &mut previous_solution);
    }

    // Compute the new scheduling solution
    let (new_solution, unassigned_shards) =
        scheduling_logic::solve(&problem, previous_solution);

    if !unassigned_shards.is_empty() {
        // TODO this is probably a bad idea to just not overschedule, as having a single index trail
        // behind will prevent the log GC.
        // A better strategy would probably be to close shard, and start prevent ingestion.
        error!("unable to assign all sources in the cluster.");
    }


    // Convert the new scheduling solution back to a physical plan.
    convert_scheduling_solution_to_physical_plan(
        new_solution,
        &id_to_ord_map,
        sources,
        previous_plan_opt)
}

impl IndexingScheduler {
    pub fn new(
        cluster_id: String,
        self_node_id: NodeId,
        metastore: Arc<dyn Metastore>,
        indexer_pool: IndexerPool,
    ) -> Self {
        Self {
            cluster_id,
            self_node_id,
            metastore,
            indexer_pool,
            state: IndexingSchedulerState::default(),
        }
    }

    pub fn observable_state(&self) -> IndexingSchedulerState {
        self.state.clone()
    }

    pub(crate) fn schedule_indexing_plan_if_needed(
        &mut self,
        model: &ControlPlaneModel,
    ) -> anyhow::Result<()> {
        let mut indexers = self.get_indexers_from_indexer_pool();
        if indexers.is_empty() {
            warn!("No indexer available, cannot schedule an indexing plan.");
            return Ok(());
        };


        let sources = get_sources_to_schedule(&model);

        let indexer_max_loads: FnvHashMap<String, Load>  = indexers.iter()
            .map(|(indexer_id, _)| {
                // TODO Get info from chitchat.
                (indexer_id.to_string(), LOAD_PER_NODE)
            }).collect();

        let new_physical_plan = build_physical_indexing_plan(&sources, &indexer_max_loads, self.state.last_applied_physical_plan.as_ref());
        if let Some(last_applied_plan) = &self.state.last_applied_physical_plan {
            let plans_diff = get_indexing_plans_diff(
                last_applied_plan.indexing_tasks_per_node(),
                new_physical_plan.indexing_tasks_per_node(),
            );
            // No need to apply the new plan as it is the same as the old one.
            if plans_diff.is_empty() {
                return Ok(());
            }
        }
        self.apply_physical_indexing_plan(&mut indexers, new_physical_plan);
        self.state.num_schedule_indexing_plan += 1;
        Ok(())
    }

    /// Checks if the last applied plan corresponds to the running indexing tasks present in the
    /// chitchat cluster state. If true, do nothing.
    /// - If node IDs differ, schedule a new indexing plan.
    /// - If indexing tasks differ, apply again the last plan.
    pub(crate) fn control_running_plan(&mut self, model: &ControlPlaneModel) -> anyhow::Result<()> {
        let last_applied_plan =
            if let Some(last_applied_plan) = self.state.last_applied_physical_plan.as_ref() {
                last_applied_plan
            } else {
                // If there is no plan, the node is probably starting and the scheduler did not find
                // indexers yet. In this case, we want to schedule as soon as possible to find new
                // indexers.
                self.schedule_indexing_plan_if_needed(model)?;
                return Ok(());
            };

        if let Some(last_applied_plan_timestamp) = self.state.last_applied_plan_timestamp {
            if Instant::now().duration_since(last_applied_plan_timestamp)
                < MIN_DURATION_BETWEEN_SCHEDULING
            {
                return Ok(());
            }
        }

        let mut indexers = self.get_indexers_from_indexer_pool();
        let running_indexing_tasks_by_node_id: FnvHashMap<String, Vec<IndexingTask>> = indexers
            .iter()
            .map(|indexer| (indexer.0.clone(), indexer.1.indexing_tasks.clone()))
            .collect();

        let indexing_plans_diff = get_indexing_plans_diff(
            &running_indexing_tasks_by_node_id,
            last_applied_plan.indexing_tasks_per_node(),
        );
        if !indexing_plans_diff.has_same_nodes() {
            info!(plans_diff=?indexing_plans_diff, "Running plan and last applied plan node IDs differ: schedule an indexing plan.");
            self.schedule_indexing_plan_if_needed(model)?;
        } else if !indexing_plans_diff.has_same_tasks() {
            // Some nodes may have not received their tasks, apply it again.
            info!(plans_diff=?indexing_plans_diff, "Running tasks and last applied tasks differ: reapply last plan.");
            self.apply_physical_indexing_plan(&mut indexers, last_applied_plan.clone());
        }
        Ok(())
    }

    fn get_indexers_from_indexer_pool(&self) -> Vec<(String, IndexerNodeInfo)> {
        self.indexer_pool.pairs()
    }

    fn apply_physical_indexing_plan(
        &mut self,
        indexers: &mut [(String, IndexerNodeInfo)],
        new_physical_plan: PhysicalIndexingPlan,
    ) {
        debug!("Apply physical indexing plan: {:?}", new_physical_plan);
        for (node_id, indexing_tasks) in new_physical_plan.indexing_tasks_per_node() {
            // We don't want to block on a slow indexer so we apply this change asynchronously
            // TODO not blocking is cool, but we need to make sure there is not accumulation
            // possible here.
            tokio::spawn({
                let indexer = indexers
                    .iter()
                    .find(|indexer| &indexer.0 == node_id)
                    .expect("This should never happen as the plan was built from these indexers.")
                    .clone();
                let indexing_tasks = indexing_tasks.clone();
                async move {
                    if let Err(error) = indexer
                        .1
                        .client
                        .clone()
                        .apply_indexing_plan(ApplyIndexingPlanRequest { indexing_tasks })
                        .await
                    {
                        error!(indexer_node_id=%indexer.0, err=?error, "Error occurred when applying indexing plan to indexer.");
                    }
                }
            });
        }
        self.state.num_applied_physical_indexing_plan += 1;
        self.state.last_applied_plan_timestamp = Some(Instant::now());
        self.state.last_applied_physical_plan = Some(new_physical_plan);
    }

    // Should be called whenever a change in the list of index/shard
    // has happened
    pub(crate) async fn on_index_change(
        &mut self,
        model: &ControlPlaneModel,
    ) -> anyhow::Result<()> {
        self.schedule_indexing_plan_if_needed(model)
            .context("error when scheduling indexing plan")?;
        Ok(())
    }
}

struct IndexingPlansDiff<'a> {
    pub missing_node_ids: FnvHashSet<&'a str>,
    pub unplanned_node_ids: FnvHashSet<&'a str>,
    pub missing_tasks_by_node_id: FnvHashMap<&'a str, Vec<&'a IndexingTask>>,
    pub unplanned_tasks_by_node_id: FnvHashMap<&'a str, Vec<&'a IndexingTask>>,
}

impl<'a> IndexingPlansDiff<'a> {
    pub fn has_same_nodes(&self) -> bool {
        self.missing_node_ids.is_empty() && self.unplanned_node_ids.is_empty()
    }

    pub fn has_same_tasks(&self) -> bool {
        self.missing_tasks_by_node_id
            .values()
            .map(Vec::len)
            .sum::<usize>()
            == 0
            && self
            .unplanned_tasks_by_node_id
            .values()
            .map(Vec::len)
            .sum::<usize>()
            == 0
    }

    pub fn is_empty(&self) -> bool {
        self.has_same_nodes() && self.has_same_tasks()
    }
}

impl<'a> fmt::Debug for IndexingPlansDiff<'a> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.has_same_nodes() && self.has_same_tasks() {
            return write!(formatter, "EmptyIndexingPlanDiff");
        }
        write!(formatter, "IndexingPlanDiff(")?;
        let mut separator = "";
        if !self.missing_node_ids.is_empty() {
            write!(formatter, "missing_node_ids={:?}, ", self.missing_node_ids)?;
            separator = ", "
        }
        if !self.unplanned_node_ids.is_empty() {
            write!(
                formatter,
                "{separator}unplanned_node_ids={:?}",
                self.unplanned_node_ids
            )?;
            separator = ", "
        }
        if !self.missing_tasks_by_node_id.is_empty() {
            write!(
                formatter,
                "{separator}missing_tasks_by_node_id={:?}, ",
                self.missing_tasks_by_node_id
            )?;
            separator = ", "
        }
        if !self.unplanned_tasks_by_node_id.is_empty() {
            write!(
                formatter,
                "{separator}unplanned_tasks_by_node_id={:?}",
                self.unplanned_tasks_by_node_id
            )?;
        }
        write!(formatter, ")")
    }
}

/// Returns the difference between the `running_plan` retrieved from the chitchat state and
/// the last plan applied by the scheduler.
fn get_indexing_plans_diff<'a>(
    running_plan: &'a FnvHashMap<String, Vec<IndexingTask>>,
    last_applied_plan: &'a FnvHashMap<String, Vec<IndexingTask>>,
) -> IndexingPlansDiff<'a> {
    // Nodes diff.
    let running_node_ids: FnvHashSet<&str> = running_plan
        .iter()
        .map(|(node_id, _)| node_id.as_str())
        .collect();
    let planned_node_ids: FnvHashSet<&str> = last_applied_plan
        .iter()
        .map(|(node_id, _)| node_id.as_str())
        .collect();
    let missing_node_ids: FnvHashSet<&str> = planned_node_ids
        .difference(&running_node_ids)
        .copied()
        .collect();
    let unplanned_node_ids: FnvHashSet<&str> = running_node_ids
        .difference(&planned_node_ids)
        .copied()
        .collect();
    // Tasks diff.
    let mut missing_tasks_by_node_id: FnvHashMap<&str, Vec<&IndexingTask>> = FnvHashMap::default();
    let mut unplanned_tasks_by_node_id: FnvHashMap<&str, Vec<&IndexingTask>> =
        FnvHashMap::default();
    for node_id in running_node_ids.iter().chain(planned_node_ids.iter()) {
        let running_tasks = running_plan
            .get(*node_id)
            .map(Vec::as_slice)
            .unwrap_or_else(|| &[]);
        let last_applied_tasks = last_applied_plan
            .get(*node_id)
            .map(Vec::as_slice)
            .unwrap_or_else(|| &[]);
        let (missing_tasks, unplanned_tasks) =
            get_indexing_tasks_diff(running_tasks, last_applied_tasks);
        missing_tasks_by_node_id.insert(*node_id, missing_tasks);
        unplanned_tasks_by_node_id.insert(*node_id, unplanned_tasks);
    }
    IndexingPlansDiff {
        missing_node_ids,
        unplanned_node_ids,
        missing_tasks_by_node_id,
        unplanned_tasks_by_node_id,
    }
}

/// Computes the difference between `running_tasks` and `last_applied_tasks` and returns a tuple
/// of `missing_tasks` and `unplanned_tasks`.
/// Note: we need to handle duplicate tasks in each array, so we count them and make the diff.
fn get_indexing_tasks_diff<'a>(
    running_tasks: &'a [IndexingTask],
    last_applied_tasks: &'a [IndexingTask],
) -> (Vec<&'a IndexingTask>, Vec<&'a IndexingTask>) {
    let mut missing_tasks: Vec<&IndexingTask> = Vec::new();
    let mut unplanned_tasks: Vec<&IndexingTask> = Vec::new();
    let grouped_running_tasks: FnvHashMap<&IndexingTask, usize> = running_tasks
        .iter()
        .group_by(|&task| task)
        .into_iter()
        .map(|(key, group)| (key, group.count()))
        .collect();
    let grouped_last_applied_tasks: FnvHashMap<&IndexingTask, usize> = last_applied_tasks
        .iter()
        .group_by(|&task| task)
        .into_iter()
        .map(|(key, group)| (key, group.count()))
        .collect();
    let all_tasks: FnvHashSet<&IndexingTask> =
        FnvHashSet::from_iter(running_tasks.iter().chain(last_applied_tasks.iter()));
    for task in all_tasks {
        let running_task_count = grouped_running_tasks.get(task).unwrap_or(&0);
        let desired_task_count = grouped_last_applied_tasks.get(task).unwrap_or(&0);
        match running_task_count.cmp(desired_task_count) {
            Ordering::Greater => {
                unplanned_tasks
                    .extend_from_slice(&vec![task; running_task_count - desired_task_count]);
            }
            Ordering::Less => {
                missing_tasks
                    .extend_from_slice(&vec![task; desired_task_count - running_task_count])
            }
            _ => {}
        }
    }

    (missing_tasks, unplanned_tasks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_indexing_plans_diff() {
        {
            let running_plan = FnvHashMap::default();
            let desired_plan = FnvHashMap::default();
            let indexing_plans_diff = get_indexing_plans_diff(&running_plan, &desired_plan);
            assert!(indexing_plans_diff.is_empty());
        }
        {
            let mut running_plan = FnvHashMap::default();
            let mut desired_plan = FnvHashMap::default();
            let task_1 = IndexingTask {
                index_uid: "index-1:11111111111111111111111111".to_string(),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            let task_2 = IndexingTask {
                index_uid: "index-1:11111111111111111111111111".to_string(),
                source_id: "source-2".to_string(),
                shard_ids: Vec::new(),
            };
            running_plan.insert(
                "indexer-1".to_string(),
                vec![task_1.clone(), task_1.clone(), task_2.clone()],
            );
            desired_plan.insert(
                "indexer-1".to_string(),
                vec![task_2, task_1.clone(), task_1],
            );
            let indexing_plans_diff = get_indexing_plans_diff(&running_plan, &desired_plan);
            assert!(indexing_plans_diff.is_empty());
        }
        {
            let mut running_plan = FnvHashMap::default();
            let mut desired_plan = FnvHashMap::default();
            let task_1 = IndexingTask {
                index_uid: "index-1:11111111111111111111111111".to_string(),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            let task_2 = IndexingTask {
                index_uid: "index-1:11111111111111111111111111".to_string(),
                source_id: "source-2".to_string(),
                shard_ids: Vec::new(),
            };
            running_plan.insert("indexer-1".to_string(), vec![task_1.clone()]);
            desired_plan.insert("indexer-1".to_string(), vec![task_2.clone()]);

            let indexing_plans_diff = get_indexing_plans_diff(&running_plan, &desired_plan);
            assert!(!indexing_plans_diff.is_empty());
            assert!(indexing_plans_diff.has_same_nodes());
            assert!(!indexing_plans_diff.has_same_tasks());
            assert_eq!(
                indexing_plans_diff.unplanned_tasks_by_node_id,
                FnvHashMap::from_iter([("indexer-1", vec![&task_1])])
            );
            assert_eq!(
                indexing_plans_diff.missing_tasks_by_node_id,
                FnvHashMap::from_iter([("indexer-1", vec![&task_2])])
            );
        }
        {
            // Task assigned to indexer-1 in desired plan but another one running.
            let mut running_plan = FnvHashMap::default();
            let mut desired_plan = FnvHashMap::default();
            let task_1 = IndexingTask {
                index_uid: "index-1:11111111111111111111111111".to_string(),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            let task_2 = IndexingTask {
                index_uid: "index-2:11111111111111111111111111".to_string(),
                source_id: "source-2".to_string(),
                shard_ids: Vec::new(),
            };
            running_plan.insert("indexer-2".to_string(), vec![task_2.clone()]);
            desired_plan.insert("indexer-1".to_string(), vec![task_1.clone()]);

            let indexing_plans_diff = get_indexing_plans_diff(&running_plan, &desired_plan);
            assert!(!indexing_plans_diff.is_empty());
            assert!(!indexing_plans_diff.has_same_nodes());
            assert!(!indexing_plans_diff.has_same_tasks());
            assert_eq!(
                indexing_plans_diff.missing_node_ids,
                FnvHashSet::from_iter(["indexer-1"])
            );
            assert_eq!(
                indexing_plans_diff.unplanned_node_ids,
                FnvHashSet::from_iter(["indexer-2"])
            );
            assert_eq!(
                indexing_plans_diff.missing_tasks_by_node_id,
                FnvHashMap::from_iter([("indexer-1", vec![&task_1]), ("indexer-2", Vec::new())])
            );
            assert_eq!(
                indexing_plans_diff.unplanned_tasks_by_node_id,
                FnvHashMap::from_iter([("indexer-2", vec![&task_2]), ("indexer-1", Vec::new())])
            );
        }
        {
            // Diff with 3 same tasks running but only one on the desired plan.
            let mut running_plan = FnvHashMap::default();
            let mut desired_plan = FnvHashMap::default();
            let task_1 = IndexingTask {
                index_uid: "index-1:11111111111111111111111111".to_string(),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            running_plan.insert("indexer-1".to_string(), vec![task_1.clone()]);
            desired_plan.insert(
                "indexer-1".to_string(),
                vec![task_1.clone(), task_1.clone(), task_1.clone()],
            );

            let indexing_plans_diff = get_indexing_plans_diff(&running_plan, &desired_plan);
            assert!(!indexing_plans_diff.is_empty());
            assert!(indexing_plans_diff.has_same_nodes());
            assert!(!indexing_plans_diff.has_same_tasks());
            assert_eq!(
                indexing_plans_diff.missing_tasks_by_node_id,
                FnvHashMap::from_iter([("indexer-1", vec![&task_1, &task_1])])
            );
        }
        {
            // Diff with 3 same tasks on desired plan but only one running.
            let mut running_plan = FnvHashMap::default();
            let mut desired_plan = FnvHashMap::default();
            let task_1 = IndexingTask {
                index_uid: "index-1:11111111111111111111111111".to_string(),
                source_id: "source-1".to_string(),
                shard_ids: Vec::new(),
            };
            running_plan.insert(
                "indexer-1".to_string(),
                vec![task_1.clone(), task_1.clone(), task_1.clone()],
            );
            desired_plan.insert("indexer-1".to_string(), vec![task_1.clone()]);

            let indexing_plans_diff = get_indexing_plans_diff(&running_plan, &desired_plan);
            assert!(!indexing_plans_diff.is_empty());
            assert!(indexing_plans_diff.has_same_nodes());
            assert!(!indexing_plans_diff.has_same_tasks());
            assert_eq!(
                indexing_plans_diff.unplanned_tasks_by_node_id,
                FnvHashMap::from_iter([("indexer-1", vec![&task_1, &task_1])])
            );
        }
    }

    #[test]
    fn test_spread_shard_optimally() {
        let mut node_num_shards = FnvHashMap::default();
        node_num_shards.insert(0, NonZeroU32::new(2).unwrap());
        node_num_shards.insert(1, NonZeroU32::new(3).unwrap());
        let mut shard_to_node_ord = FnvHashMap::default();
        shard_to_node_ord.insert(0, 0);
        shard_to_node_ord.insert(1, 2);
        shard_to_node_ord.insert(3, 0);
        let node_to_shards = spread_shards_optimally(&[0, 1, 2, 3, 4],
               node_num_shards,
               shard_to_node_ord
        );
        assert_eq!(node_to_shards.get(&0), Some(&vec![0, 3]));
        assert_eq!(node_to_shards.get(&1), Some(&vec![1, 2, 4]));
    }
}
