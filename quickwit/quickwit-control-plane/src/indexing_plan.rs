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


use fnv::FnvHashMap;
use quickwit_proto::indexing::IndexingTask;
use serde::Serialize;

/// A [`PhysicalIndexingPlan`] defines the list of indexing tasks
/// each indexer, identified by its node ID, should run.
/// TODO(fmassot): a metastore version number will be attached to the plan
/// to identify if the plan is up to date with the metastore.
#[derive(Debug, PartialEq, Clone, Serialize, Default)]
pub struct PhysicalIndexingPlan {
    indexing_tasks_per_node_id: FnvHashMap<String, Vec<IndexingTask>>,
}

impl PhysicalIndexingPlan {
    pub fn add_indexing_task(&mut self, node_id: &str, indexing_task: IndexingTask) {
        self.indexing_tasks_per_node_id.entry(node_id.to_string())
            .or_default()
            .push(indexing_task);

    }

    /// Returns the hashmap of (node ID, indexing tasks).
    pub fn indexing_tasks_per_node(&self) -> &FnvHashMap<String, Vec<IndexingTask>> {
        &self.indexing_tasks_per_node_id
    }
}

#[cfg(test)]
mod tests {
    // use std::num::NonZeroUsize;
    //
    // use fnv::FnvHashMap;
    // use itertools::Itertools;
    // use proptest::prelude::*;
    // use quickwit_common::rand::append_random_suffix;
    // use quickwit_config::service::QuickwitService;
    // use quickwit_config::{
    //     FileSourceParams, IndexConfig, KafkaSourceParams, SourceConfig, SourceInputFormat,
    //     SourceParams, CLI_INGEST_SOURCE_ID, INGEST_API_SOURCE_ID,
    // };
    // use quickwit_metastore::IndexMetadata;
    // use quickwit_proto::indexing::{IndexingServiceClient, IndexingTask};
    // use quickwit_proto::IndexUid;
    // use rand::seq::SliceRandom;
    // use serde_json::json;
    // use tonic::transport::Endpoint;
    //
    // use crate::control_plane_model::ControlPlaneModel;
    // use crate::{IndexerNodeInfo, SourceUid};
    //
    // fn kafka_source_params_for_test() -> SourceParams {
    //     SourceParams::Kafka(KafkaSourceParams {
    //         topic: "topic".to_string(),
    //         client_log_level: None,
    //         client_params: json!({
    //             "bootstrap.servers": "localhost:9092",
    //         }),
    //         enable_backfill_mode: true,
    //     })
    // }
    //
    // async fn cluster_members_for_test(
    //     num_members: usize,
    //     _quickwit_service: QuickwitService,
    // ) -> Vec<(String, IndexerNodeInfo)> {
    //     let mut members = Vec::new();
    //     for idx in 0..num_members {
    //         let channel = Endpoint::from_static("http://127.0.0.1:10").connect_lazy();
    //         let client =
    //             IndexingServiceClient::from_channel("127.0.0.1:10".parse().unwrap(), channel);
    //         members.push((
    //             (1 + idx).to_string(),
    //             IndexerNodeInfo {
    //                 client,
    //                 indexing_tasks: Vec::new(),
    //             },
    //         ));
    //     }
    //     members
    // }
    //
    // fn count_indexing_tasks_count_for_test(
    //     num_indexers: usize,
    //     source_configs: &FnvHashMap<SourceUid, SourceConfig>,
    // ) -> usize {
    //     source_configs
    //         .iter()
    //         .map(|(_, source_config)| {
    //             std::cmp::min(
    //                 num_indexers * source_config.max_num_pipelines_per_indexer.get(),
    //                 source_config.desired_num_pipelines.get(),
    //             )
    //         })
    //         .sum()
    // }
    //
    // #[tokio::test]
    // async fn test_build_indexing_plan_one_source() {
    //     let source_config = SourceConfig {
    //         source_id: "source_id".to_string(),
    //         max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
    //         desired_num_pipelines: NonZeroUsize::new(3).unwrap(),
    //         enabled: true,
    //         source_params: kafka_source_params_for_test(),
    //         transform_config: None,
    //         input_format: SourceInputFormat::Json,
    //     };
    //     let mut model = ControlPlaneModel::default();
    //     let index_metadata = IndexMetadata::for_test("test-index", "ram://test");
    //     let index_uid = index_metadata.index_uid.clone();
    //     let source_uid = SourceUid {
    //         index_uid: index_uid.clone(),
    //         source_id: source_config.source_id.clone(),
    //     };
    //     model.add_index(index_metadata);
    //     model.add_source(&index_uid, source_config).unwrap();
    //     let indexing_tasks = list_indexing_tasks(4, &model);
    //     assert_eq!(indexing_tasks.len(), 3);
    //     for indexing_task in indexing_tasks {
    //         assert_eq!(
    //             indexing_task,
    //             LogicalIndexingTask {
    //                 source_uid: source_uid.clone(),
    //                 shard_ids: Vec::new(),
    //                 max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
    //             }
    //         );
    //     }
    // }

    // #[tokio::test]
    // async fn test_build_indexing_plan_with_ingest_api_source() {
    //     let indexers = cluster_members_for_test(4, QuickwitService::Indexer).await;
    //
    //     let index_metadata = IndexMetadata::for_test("test-index", "ram://test");
    //     let index_uid = index_metadata.index_uid.clone();
    //     let source_uid = SourceUid {
    //         index_uid: index_uid.clone(),
    //         source_id: INGEST_API_SOURCE_ID.to_string(),
    //     };
    //
    //     let source_config = SourceConfig {
    //         source_id: source_uid.source_id.to_string(),
    //         max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
    //         desired_num_pipelines: NonZeroUsize::new(3).unwrap(),
    //         enabled: true,
    //         source_params: SourceParams::IngestApi,
    //         transform_config: None,
    //         input_format: SourceInputFormat::Json,
    //     };
    //
    //     let mut control_plane_model = ControlPlaneModel::default();
    //     control_plane_model.add_index(index_metadata);
    //     control_plane_model
    //         .add_source(&index_uid, source_config)
    //         .unwrap();
    //     let indexing_tasks: Vec<LogicalIndexingTask> =
    //         list_indexing_tasks(indexers.len(), &control_plane_model);
    //
    //     assert_eq!(indexing_tasks.len(), 4);
    //     for indexing_task in indexing_tasks {
    //         assert_eq!(
    //             indexing_task,
    //             LogicalIndexingTask {
    //                 source_uid: source_uid.clone(),
    //                 shard_ids: Vec::new(),
    //                 max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
    //             }
    //         );
    //     }
    // }
    //
    // #[tokio::test]
    // async fn test_build_indexing_plan_with_sources_to_ignore() {
    //     let indexers = cluster_members_for_test(4, QuickwitService::Indexer).await;
    //     let mut source_configs_map = FnvHashMap::default();
    //     let file_index_source_id = SourceUid {
    //         index_uid: "one-source-index:11111111111111111111111111"
    //             .to_string()
    //             .into(),
    //         source_id: "file-source".to_string(),
    //     };
    //     let cli_ingest_index_source_id = SourceUid {
    //         index_uid: "second-source-index:11111111111111111111111111"
    //             .to_string()
    //             .into(),
    //         source_id: CLI_INGEST_SOURCE_ID.to_string(),
    //     };
    //     let kafka_index_source_id = SourceUid {
    //         index_uid: "third-source-index:11111111111111111111111111"
    //             .to_string()
    //             .into(),
    //         source_id: "kafka-source".to_string(),
    //     };
    //     source_configs_map.insert(
    //         file_index_source_id.clone(),
    //         SourceConfig {
    //             source_id: file_index_source_id.source_id,
    //             max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
    //             desired_num_pipelines: NonZeroUsize::new(3).unwrap(),
    //             enabled: true,
    //             source_params: SourceParams::File(FileSourceParams { filepath: None }),
    //             transform_config: None,
    //             input_format: SourceInputFormat::Json,
    //         },
    //     );
    //     source_configs_map.insert(
    //         cli_ingest_index_source_id.clone(),
    //         SourceConfig {
    //             source_id: cli_ingest_index_source_id.source_id,
    //             max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
    //             desired_num_pipelines: NonZeroUsize::new(3).unwrap(),
    //             enabled: true,
    //             source_params: SourceParams::IngestCli,
    //             transform_config: None,
    //             input_format: SourceInputFormat::Json,
    //         },
    //     );
    //     source_configs_map.insert(
    //         kafka_index_source_id.clone(),
    //         SourceConfig {
    //             source_id: kafka_index_source_id.source_id,
    //             max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
    //             desired_num_pipelines: NonZeroUsize::new(3).unwrap(),
    //             enabled: false,
    //             source_params: kafka_source_params_for_test(),
    //             transform_config: None,
    //             input_format: SourceInputFormat::Json,
    //         },
    //     );
    //
    //     let control_plane_model = ControlPlaneModel::default();
    //     let indexing_tasks = list_indexing_tasks(indexers.len(), &control_plane_model);
    //
    //     assert_eq!(indexing_tasks.len(), 0);
    // }
    //
    // #[tokio::test]
    // async fn test_build_physical_indexing_plan_simple() {
    //     quickwit_common::setup_logging_for_tests();
    //     // Rdv hashing for (index 1, source) returns [node 2, node 1].
    //     let index_1 = "1";
    //     let source_1 = "1";
    //     // Rdv hashing for (index 2, source) returns [node 1, node 2].
    //     let index_2 = "2";
    //     let source_2 = "0";
    //     // let mut source_configs_map = FnvHashMap::default();
    //     let kafka_index_source_id_1 = SourceUid {
    //         index_uid: IndexUid::from_parts(index_1, "11111111111111111111111111"),
    //         source_id: source_1.to_string(),
    //     };
    //     let kafka_index_source_id_2 = SourceUid {
    //         index_uid: IndexUid::from_parts(index_2, "11111111111111111111111111"),
    //         source_id: source_2.to_string(),
    //     };
    //     let mut indexing_tasks = Vec::new();
    //     for _ in 0..3 {
    //         indexing_tasks.push(LogicalIndexingTask {
    //             source_uid: kafka_index_source_id_1.clone(),
    //             shard_ids: Vec::new(),
    //             max_num_pipelines_per_indexer: NonZeroUsize::new(2).unwrap(),
    //         });
    //     }
    //     for _ in 0..2 {
    //         indexing_tasks.push(LogicalIndexingTask {
    //             source_uid: kafka_index_source_id_2.clone(),
    //             shard_ids: Vec::new(),
    //             max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
    //         });
    //     }
    //
    //     let indexers = cluster_members_for_test(2, QuickwitService::Indexer).await;
    //     let physical_plan = build_physical_indexing_plan(&indexers, indexing_tasks.clone());
    //     assert_eq!(physical_plan.indexing_tasks_per_node_id.len(), 2);
    //     let indexer_1_tasks = physical_plan
    //         .indexing_tasks_per_node_id
    //         .get(&indexers[0].0)
    //         .unwrap();
    //     let indexer_2_tasks = physical_plan
    //         .indexing_tasks_per_node_id
    //         .get(&indexers[1].0)
    //         .unwrap();
    //     // (index 1, source) tasks are first placed on indexer 2 by rdv hashing.
    //     // Thus task 0 => indexer 2, task 1 => indexer 1, task 2 => indexer 2, task 3 => indexer 1.
    //     let expected_indexer_1_tasks: Vec<IndexingTask> = indexing_tasks
    //         .iter()
    //         .cloned()
    //         .enumerate()
    //         .filter(|(idx, _)| idx % 2 == 1)
    //         .map(|(_, task)| task.into())
    //         .collect_vec();
    //     assert_eq!(indexer_1_tasks, &expected_indexer_1_tasks);
    //     // (index 1, source) tasks are first placed on node 1 by rdv hashing.
    //     let expected_indexer_2_tasks: Vec<IndexingTask> = indexing_tasks
    //         .into_iter()
    //         .enumerate()
    //         .filter(|(idx, _)| idx % 2 == 0)
    //         .map(|(_, task)| task.into())
    //         .collect_vec();
    //     assert_eq!(indexer_2_tasks, &expected_indexer_2_tasks);
    // }
    //
    // #[tokio::test]
    // async fn test_build_physical_indexing_plan_with_not_enough_indexers() {
    //     quickwit_common::setup_logging_for_tests();
    //     let index_1 = "test-indexing-plan-1";
    //     let source_1 = "source-1";
    //     let source_uid = SourceUid {
    //         index_uid: IndexUid::from_parts(index_1, "11111111111111111111111111"),
    //         source_id: source_1.to_string(),
    //     };
    //     let indexing_tasks = vec![
    //         LogicalIndexingTask {
    //             source_uid: source_uid.clone(),
    //             shard_ids: Vec::new(),
    //             max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
    //         },
    //         LogicalIndexingTask {
    //             source_uid: source_uid.clone(),
    //             shard_ids: Vec::new(),
    //             max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
    //         },
    //     ];
    //
    //     let indexers = cluster_members_for_test(1, QuickwitService::Indexer).await;
    //     // This case should never happens but we just check that the plan building is resilient
    //     // enough, it will ignore the tasks that cannot be allocated.
    //     let physical_plan = build_physical_indexing_plan(&indexers, indexing_tasks);
    //     assert_eq!(physical_plan.num_indexing_tasks(), 1);
    // }
    //
    // proptest! {
    //     #[test]
    //     fn test_building_indexing_tasks_and_physical_plan(num_indexers in 1usize..50usize, index_id_sources in proptest::collection::vec(gen_kafka_source(), 1..20)) {
    //         // proptest doesn't work with async
    //         let mut indexers = tokio::runtime::Runtime::new().unwrap().block_on(
    //             cluster_members_for_test(num_indexers, QuickwitService::Indexer)
    //         );
    //
    //         let index_uids: fnv::FnvHashSet<IndexUid> =
    //             index_id_sources.iter()
    //                 .map(|(index_uid, _)| index_uid.clone())
    //                 .collect();
    //
    //         let mut control_plane_model = ControlPlaneModel::default();
    //         for index_uid in index_uids {
    //             let index_config = IndexConfig::for_test(index_uid.index_id(), &format!("ram://test/{index_uid}"));
    //             control_plane_model.add_index(IndexMetadata::new_with_index_uid(index_uid, index_config));
    //         }
    //         for (index_uid, source_config) in &index_id_sources {
    //             control_plane_model.add_source(index_uid, source_config.clone()).unwrap();
    //         }
    //
    //         let source_configs: FnvHashMap<SourceUid, SourceConfig> = index_id_sources
    //             .into_iter()
    //             .map(|(index_uid, source_config)| {
    //                 (SourceUid { index_uid: index_uid.clone(), source_id: source_config.source_id.to_string(), }, source_config)
    //             })
    //             .collect();
    //
    //
    //         let mut indexing_tasks = list_indexing_tasks(indexers.len(), &control_plane_model);
    //         let num_indexing_tasks = indexing_tasks.len();
    //         assert_eq!(indexing_tasks.len(), count_indexing_tasks_count_for_test(indexers.len(), &source_configs));
    //         let physical_indexing_plan = build_physical_indexing_plan(&indexers, indexing_tasks.clone());
    //         indexing_tasks.shuffle(&mut rand::thread_rng());
    //         indexers.shuffle(&mut rand::thread_rng());
    //         let physical_indexing_plan_with_shuffle = build_physical_indexing_plan(&indexers, indexing_tasks.clone());
    //         assert_eq!(physical_indexing_plan, physical_indexing_plan_with_shuffle);
    //         // All indexing tasks must have been assigned to an indexer.
    //         assert_eq!(physical_indexing_plan.num_indexing_tasks(), num_indexing_tasks);
    //         // Indexing task must be spread over nodes: at maximum, a node can have only one more indexing task than other nodes.
    //         assert!(physical_indexing_plan.max_num_indexing_tasks_per_node() - physical_indexing_plan.min_num_indexing_tasks_per_node() <= 1);
    //         // Check basics math.
    //         assert_eq!(physical_indexing_plan.num_indexing_tasks_mean_per_node(), num_indexing_tasks as f32 / indexers.len() as f32 );
    //         assert_eq!(physical_indexing_plan.indexing_tasks_per_node().len(), indexers.len());
    //     }
    // }
    //
    // prop_compose! {
    //   fn gen_kafka_source()
    //     (index_idx in 0usize..100usize, desired_num_pipelines in 1usize..51usize, max_num_pipelines_per_indexer in 1usize..5usize) -> (IndexUid, SourceConfig) {
    //       let index_uid = IndexUid::from_parts(format!("index-id-{index_idx}"), "" /* this is the index uid */);
    //       let source_id = append_random_suffix("kafka-source");
    //       (index_uid, SourceConfig {
    //           source_id,
    //           desired_num_pipelines: NonZeroUsize::new(desired_num_pipelines).unwrap(),
    //           max_num_pipelines_per_indexer: NonZeroUsize::new(max_num_pipelines_per_indexer).unwrap(),
    //           enabled: true,
    //           source_params: kafka_source_params_for_test(),
    //           transform_config: None,
    //           input_format: SourceInputFormat::Json,
    //       })
    //   }
    // }
}
