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

use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, Handler, Mailbox, Supervisor, Universe,
};
use quickwit_config::SourceConfig;
use quickwit_ingest::IngesterPool;
use quickwit_metastore::IndexMetadata;
use quickwit_proto::control_plane::{
    ControlPlaneError, ControlPlaneResult, GetOrCreateOpenShardsRequest,
    GetOrCreateOpenShardsResponse,
};
use quickwit_proto::metastore::{
    serde_utils as metastore_serde_utils, AddSourceRequest, CloseShardsRequest, CreateIndexRequest,
    CreateIndexResponse, DeleteIndexRequest, DeleteShardsRequest, DeleteSourceRequest,
    EmptyResponse, MetastoreError, MetastoreService, MetastoreServiceClient, ToggleSourceRequest,
};
use quickwit_proto::{IndexUid, NodeId};
use serde::Serialize;
use tracing::error;

use crate::control_plane_model::{ControlPlaneModel, ControlPlaneModelMetrics};
use crate::indexing_scheduler::{IndexingScheduler, IndexingSchedulerState};
use crate::ingest::IngestController;
use crate::IndexerPool;

/// Interval between two controls (or checks) of the desired plan VS running plan.
pub(crate) const CONTROL_PLAN_LOOP_INTERVAL: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(500)
} else {
    Duration::from_secs(3)
};

#[derive(Debug)]
struct ControlPlanLoop;

#[derive(Debug)]
pub struct ControlPlane {
    metastore: MetastoreServiceClient,
    model: ControlPlaneModel,
    // The control plane state is split into to independent functions, that we naturally isolated
    // code wise and state wise.
    //
    // - The indexing scheduler is in charge of managing indexers: it decides which indexer should
    // index which source/shards.
    // - the ingest controller is in charge of managing ingesters: it opens and closes shards on
    // the different ingesters.
    indexing_scheduler: IndexingScheduler,
    ingest_controller: IngestController,
}

impl ControlPlane {
    pub fn spawn(
        universe: &Universe,
        cluster_id: String,
        self_node_id: NodeId,
        indexer_pool: IndexerPool,
        ingester_pool: IngesterPool,
        metastore: MetastoreServiceClient,
        replication_factor: usize,
    ) -> (Mailbox<Self>, ActorHandle<Supervisor<Self>>) {
        universe.spawn_builder().supervise_fn(move || {
            let indexing_scheduler = IndexingScheduler::new(
                cluster_id.clone(),
                self_node_id.clone(),
                metastore.clone(),
                indexer_pool.clone(),
            );
            let ingest_controller =
                IngestController::new(metastore.clone(), ingester_pool.clone(), replication_factor);
            ControlPlane {
                model: Default::default(),
                metastore: metastore.clone(),
                indexing_scheduler,
                ingest_controller,
            }
        })
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ControlPlaneObservableState {
    pub indexing_scheduler: IndexingSchedulerState,
    pub model_metrics: ControlPlaneModelMetrics,
}

#[async_trait]
impl Actor for ControlPlane {
    type ObservableState = ControlPlaneObservableState;

    fn name(&self) -> String {
        "ControlPlane".to_string()
    }

    fn observable_state(&self) -> Self::ObservableState {
        ControlPlaneObservableState {
            indexing_scheduler: self.indexing_scheduler.observable_state(),
            model_metrics: self.model.observable_state(),
        }
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.model
            .load_from_metastore(&mut self.metastore, ctx.progress())
            .await
            .context("failed to intialize the model")?;

        if let Err(error) = self
            .indexing_scheduler
            .schedule_indexing_plan_if_needed(&self.model)
        {
            // TODO inspect error.
            error!("error when scheduling indexing plan: `{}`.", error);
        }

        ctx.schedule_self_msg(CONTROL_PLAN_LOOP_INTERVAL, ControlPlanLoop)
            .await;
        Ok(())
    }
}

#[async_trait]
impl Handler<ControlPlanLoop> for ControlPlane {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: ControlPlanLoop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if let Err(error) = self.indexing_scheduler.control_running_plan(&self.model) {
            error!("error when controlling the running plan: `{}`", error);
        }
        ctx.schedule_self_msg(CONTROL_PLAN_LOOP_INTERVAL, ControlPlanLoop)
            .await;
        Ok(())
    }
}

/// This function converts a metastore error into an actor error.
///
/// If the metastore error is implying the transaction has not been
/// successful, then we do not need to restart the metastore.
/// If the metastore error does not let us know whether the transaction was
/// successful or not, we need to restart the actor and have it load its state from
/// the metastore.
///
/// This function also logs errors.
fn convert_metastore_error<T>(
    metastore_error: MetastoreError,
) -> Result<ControlPlaneResult<T>, ActorExitStatus> {
    // If true, we know that the transactions has not been recorded in the Metastore.
    // If false, we simply are not sure whether the transaction has been recorded or not.
    let is_transaction_certainly_aborted = match &metastore_error {
        MetastoreError::AlreadyExists(_)
        | MetastoreError::FailedPrecondition { .. }
        | MetastoreError::Forbidden { .. }
        | MetastoreError::InvalidArgument { .. }
        | MetastoreError::JsonDeserializeError { .. }
        | MetastoreError::JsonSerializeError { .. }
        | MetastoreError::NotFound(_) => true,
        MetastoreError::Unavailable(_)
        | MetastoreError::Internal { .. }
        | MetastoreError::Io { .. }
        | MetastoreError::Connection { .. }
        | MetastoreError::Db { .. } => false,
    };
    if is_transaction_certainly_aborted {
        // If the metastore transaction is certain to have been aborted,
        // this is actually a good thing.
        // We do not need to restart the control plane.
        error!(err=?metastore_error, transaction_outcome="aborted", "metastore error");
        Ok(Err(ControlPlaneError::Metastore(metastore_error)))
    } else {
        // If the metastore transaction may have been executed, we need to restart the control plane
        // so that it gets resynced with the metastore state.
        error!(err=?metastore_error, transaction_outcome="maybe-executed", "metastore error");
        Err(ActorExitStatus::from(anyhow::anyhow!(metastore_error)))
    }
}

// This handler is a metastore call proxied through the control plane: we must first forward the
// request to the metastore, and then act on the event.
#[async_trait]
impl Handler<CreateIndexRequest> for ControlPlane {
    type Reply = ControlPlaneResult<CreateIndexResponse>;

    async fn handle(
        &mut self,
        request: CreateIndexRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let index_config = match metastore_serde_utils::from_json_str(&request.index_config_json) {
            Ok(index_config) => index_config,
            Err(error) => {
                return Ok(Err(ControlPlaneError::from(error)));
            }
        };
        let index_uid: IndexUid = match self.metastore.create_index(request).await {
            Ok(response) => response.index_uid.into(),
            Err(metastore_error) => return convert_metastore_error(metastore_error),
        };

        let index_metadata: IndexMetadata =
            IndexMetadata::new_with_index_uid(index_uid.clone(), index_config);

        self.model.add_index(index_metadata);

        let response = CreateIndexResponse {
            index_uid: index_uid.into(),
        };
        // We do not need to inform the indexing scheduler as there are no shards at this point.
        Ok(Ok(response))
    }
}

// This handler is a metastore call proxied through the control plane: we must first forward the
// request to the metastore, and then act on the event.
#[async_trait]
impl Handler<DeleteIndexRequest> for ControlPlane {
    type Reply = ControlPlaneResult<EmptyResponse>;

    async fn handle(
        &mut self,
        request: DeleteIndexRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let index_uid: IndexUid = request.index_uid.clone().into();

        if let Err(error) = self.metastore.delete_index(request).await {
            return Ok(Err(ControlPlaneError::from(error)));
        };

        self.model.delete_index(&index_uid);

        let response = EmptyResponse {};

        // TODO: Refine the event. Notify index will have the effect to reload the entire state from
        // the metastore. We should update the state of the control plane.
        self.indexing_scheduler.on_index_change(&self.model).await?;

        Ok(Ok(response))
    }
}

// This handler is a metastore call proxied through the control plane: we must first forward the
// request to the metastore, and then act on the event.
#[async_trait]
impl Handler<AddSourceRequest> for ControlPlane {
    type Reply = ControlPlaneResult<EmptyResponse>;

    async fn handle(
        &mut self,
        request: AddSourceRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let index_uid: IndexUid = request.index_uid.clone().into();
        let source_config: SourceConfig =
            match metastore_serde_utils::from_json_str(&request.source_config_json) {
                Ok(source_config) => source_config,
                Err(error) => {
                    return Ok(Err(ControlPlaneError::from(error)));
                }
            };
        if let Err(error) = self.metastore.add_source(request).await {
            return Ok(Err(ControlPlaneError::from(error)));
        };

        self.model
            .add_source(&index_uid, source_config)
            .context("failed to add source")?;

        // TODO: Refine the event. Notify index will have the effect to reload the entire state from
        // the metastore. We should update the state of the control plane.
        self.indexing_scheduler.on_index_change(&self.model).await?;

        let response = EmptyResponse {};
        Ok(Ok(response))
    }
}

// This handler is a metastore call proxied through the control plane: we must first forward the
// request to the metastore, and then act on the event.
#[async_trait]
impl Handler<ToggleSourceRequest> for ControlPlane {
    type Reply = ControlPlaneResult<EmptyResponse>;

    async fn handle(
        &mut self,
        request: ToggleSourceRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        if let Err(error) = self.metastore.toggle_source(request).await {
            return Ok(Err(ControlPlaneError::from(error)));
        };

        // TODO update the internal view.
        // TODO: Refine the event. Notify index will have the effect to reload the entire state from
        // the metastore. We should update the state of the control plane.
        self.indexing_scheduler.on_index_change(&self.model).await?;

        let response = EmptyResponse {};
        Ok(Ok(response))
    }
}

// This handler is a metastore call proxied through the control plane: we must first forward the
// request to the metastore, and then act on the event.
#[async_trait]
impl Handler<DeleteSourceRequest> for ControlPlane {
    type Reply = ControlPlaneResult<EmptyResponse>;

    async fn handle(
        &mut self,
        request: DeleteSourceRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let index_uid: IndexUid = request.index_uid.clone().into();
        let source_id = request.source_id.clone();
        if let Err(metastore_error) = self.metastore.delete_source(request).await {
            return convert_metastore_error(metastore_error);
        };
        self.model.delete_source(&index_uid, &source_id);
        self.indexing_scheduler.on_index_change(&self.model).await?;
        let response = EmptyResponse {};
        Ok(Ok(response))
    }
}

// This is neither a proxied call nor a metastore callback.
#[async_trait]
impl Handler<GetOrCreateOpenShardsRequest> for ControlPlane {
    type Reply = ControlPlaneResult<GetOrCreateOpenShardsResponse>;

    async fn handle(
        &mut self,
        request: GetOrCreateOpenShardsRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self
            .ingest_controller
            .get_or_create_open_shards(request, &mut self.model, ctx.progress())
            .await)
    }
}

// This is a metastore callback. Ingesters call the metastore to close shards directly, then the
// metastore notifies the control plane of the event.
#[async_trait]
impl Handler<CloseShardsRequest> for ControlPlane {
    type Reply = ControlPlaneResult<EmptyResponse>;

    async fn handle(
        &mut self,
        request: CloseShardsRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        for close_shards_subrequest in request.subrequests {
            let index_uid: IndexUid = close_shards_subrequest.index_uid.into();
            let source_id = close_shards_subrequest.source_id;
            // TODO: Group by (index_uid, source_id) first, or change schema of
            // `CloseShardsSubrequest`.
            let shard_ids = [close_shards_subrequest.shard_id];
            self.model.close_shards(&index_uid, &source_id, &shard_ids)
        }
        Ok(Ok(EmptyResponse {}))
    }
}

// This is a metastore callback. Ingesters call the metastore to delete shards directly, then the
// metastore notifies the control plane of the event.
#[async_trait]
impl Handler<DeleteShardsRequest> for ControlPlane {
    type Reply = ControlPlaneResult<EmptyResponse>;

    async fn handle(
        &mut self,
        request: DeleteShardsRequest,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        for delete_shards_subrequest in request.subrequests {
            let index_uid: IndexUid = delete_shards_subrequest.index_uid.into();
            let source_id = delete_shards_subrequest.source_id;
            let shard_ids = delete_shards_subrequest.shard_ids;
            self.model.delete_shards(&index_uid, &source_id, &shard_ids)
        }
        Ok(Ok(EmptyResponse {}))
    }
}

#[cfg(test)]
mod tests {
    use quickwit_actors::{AskError, Observe, SupervisorMetrics};
    use quickwit_config::{IndexConfig, SourceParams, INGEST_SOURCE_ID};
    use quickwit_metastore::{
        CreateIndexRequestExt, IndexMetadata, ListIndexesMetadataRequestExt,
        ListIndexesMetadataResponseExt,
    };
    use quickwit_proto::control_plane::GetOrCreateOpenShardsSubrequest;
    use quickwit_proto::ingest::Shard;
    use quickwit_proto::metastore::{
        EntityKind, ListIndexesMetadataRequest, ListIndexesMetadataResponse, ListShardsRequest,
        ListShardsResponse, ListShardsSubresponse, MetastoreError, SourceType,
    };

    use super::*;

    #[tokio::test]
    async fn test_control_plane_create_index() {
        let universe = Universe::with_accelerated_time();

        let cluster_id = "test-cluster".to_string();
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MetastoreServiceClient::mock();
        mock_metastore
            .expect_create_index()
            .withf(|create_index_request| {
                let index_config: IndexConfig =
                    serde_json::from_str(&create_index_request.index_config_json).unwrap();
                assert_eq!(index_config.index_id, "test-index");
                assert_eq!(index_config.index_uri, "ram:///test-index");
                true
            })
            .returning(|_| {
                Ok(CreateIndexResponse {
                    index_uid: "test-index:0".to_string(),
                })
            });
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(|_| {
                Ok(ListIndexesMetadataResponse::try_from_indexes_metadata(Vec::new()).unwrap())
            });
        let replication_factor = 1;

        let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
            &universe,
            cluster_id,
            self_node_id,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from(mock_metastore),
            replication_factor,
        );
        let index_config = IndexConfig::for_test("test-index", "ram:///test-index");
        let create_index_request = CreateIndexRequest {
            index_config_json: serde_json::to_string(&index_config).unwrap(),
        };
        let create_index_response = control_plane_mailbox
            .ask_for_res(create_index_request)
            .await
            .unwrap();
        assert_eq!(create_index_response.index_uid, "test-index:0");

        // TODO: Test that create index event is properly sent to ingest controller.

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_delete_index() {
        let universe = Universe::with_accelerated_time();

        let cluster_id = "test-cluster".to_string();
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MetastoreServiceClient::mock();
        mock_metastore
            .expect_delete_index()
            .withf(|delete_index_request| delete_index_request.index_uid == "test-index:0")
            .returning(|_| Ok(EmptyResponse {}));
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(|_| {
                Ok(ListIndexesMetadataResponse::try_from_indexes_metadata(Vec::new()).unwrap())
            });
        let replication_factor = 1;

        let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
            &universe,
            cluster_id,
            self_node_id,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from(mock_metastore),
            replication_factor,
        );
        let delete_index_request = DeleteIndexRequest {
            index_uid: "test-index:0".to_string(),
        };
        control_plane_mailbox
            .ask_for_res(delete_index_request)
            .await
            .unwrap();

        // TODO: Test that delete index event is properly sent to ingest controller.

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_add_source() {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::with_accelerated_time();

        let cluster_id = "test-cluster".to_string();
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let index_metadata = IndexMetadata::for_test("test-index", "ram://test");
        let index_uid = index_metadata.index_uid.clone();
        let mut mock_metastore = MetastoreServiceClient::mock();
        mock_metastore
            .expect_add_source()
            .withf(|add_source_request| {
                let source_config: SourceConfig =
                    serde_json::from_str(&add_source_request.source_config_json).unwrap();
                assert_eq!(source_config.source_id, "test-source");
                assert_eq!(source_config.source_type(), SourceType::Void);
                true
            })
            .returning(|_| Ok(EmptyResponse {}));
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(move |_| {
                Ok(ListIndexesMetadataResponse::try_from_indexes_metadata(vec![
                    index_metadata.clone()
                ])
                .unwrap())
            });
        let replication_factor = 1;

        let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
            &universe,
            cluster_id,
            self_node_id,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from(mock_metastore),
            replication_factor,
        );
        let source_config = SourceConfig::for_test("test-source", SourceParams::void());
        let add_source_request = AddSourceRequest {
            index_uid: index_uid.to_string(),
            source_config_json: serde_json::to_string(&source_config).unwrap(),
        };
        control_plane_mailbox
            .ask_for_res(add_source_request)
            .await
            .unwrap();

        // TODO: Test that delete index event is properly sent to ingest controller.

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_toggle_source() {
        let universe = Universe::with_accelerated_time();

        let cluster_id = "test-cluster".to_string();
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MetastoreServiceClient::mock();
        mock_metastore
            .expect_toggle_source()
            .withf(|toggle_source_request| {
                assert_eq!(toggle_source_request.index_uid, "test-index:0");
                assert_eq!(toggle_source_request.source_id, "test-source");
                assert!(toggle_source_request.enable);
                true
            })
            .returning(|_| Ok(EmptyResponse {}));
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(|_| {
                Ok(ListIndexesMetadataResponse::try_from_indexes_metadata(Vec::new()).unwrap())
            });
        let replication_factor = 1;

        let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
            &universe,
            cluster_id,
            self_node_id,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from(mock_metastore),
            replication_factor,
        );
        let toggle_source_request = ToggleSourceRequest {
            index_uid: "test-index:0".to_string(),
            source_id: "test-source".to_string(),
            enable: true,
        };
        control_plane_mailbox
            .ask_for_res(toggle_source_request)
            .await
            .unwrap();

        // TODO: Test that delete index event is properly sent to ingest controller.

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_delete_source() {
        let universe = Universe::with_accelerated_time();

        let cluster_id = "test-cluster".to_string();
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MetastoreServiceClient::mock();
        mock_metastore
            .expect_delete_source()
            .withf(|delete_source_request| {
                assert_eq!(delete_source_request.index_uid, "test-index:0");
                assert_eq!(delete_source_request.source_id, "test-source");
                true
            })
            .returning(|_| Ok(EmptyResponse {}));
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(|_| {
                Ok(ListIndexesMetadataResponse::try_from_indexes_metadata(Vec::new()).unwrap())
            });
        let replication_factor = 1;

        let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
            &universe,
            cluster_id,
            self_node_id,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from(mock_metastore),
            replication_factor,
        );
        let delete_source_request = DeleteSourceRequest {
            index_uid: "test-index:0".to_string(),
            source_id: "test-source".to_string(),
        };
        control_plane_mailbox
            .ask_for_res(delete_source_request)
            .await
            .unwrap();

        // TODO: Test that delete index event is properly sent to ingest controller.

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_get_open_shards() {
        let universe = Universe::with_accelerated_time();

        let cluster_id = "test-cluster".to_string();
        let self_node_id: NodeId = "test-node".into();
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();

        let mut mock_metastore = MetastoreServiceClient::mock();
        mock_metastore
            .expect_list_indexes_metadata()
            .returning(|_| {
                let mut index_metadata = IndexMetadata::for_test("test-index", "ram:///test-index");
                let source_config = SourceConfig::for_test(INGEST_SOURCE_ID, SourceParams::void());
                index_metadata.add_source(source_config).unwrap();
                Ok(
                    ListIndexesMetadataResponse::try_from_indexes_metadata(vec![index_metadata])
                        .unwrap(),
                )
            });
        mock_metastore.expect_list_shards().returning(|request| {
            assert_eq!(request.subrequests.len(), 1);

            let subrequest = &request.subrequests[0];
            assert_eq!(subrequest.index_uid, "test-index:0");
            assert_eq!(subrequest.source_id, INGEST_SOURCE_ID);

            let subresponses = vec![ListShardsSubresponse {
                index_uid: "test-index:0".to_string(),
                source_id: INGEST_SOURCE_ID.to_string(),
                shards: vec![Shard {
                    index_uid: "test-index:0".to_string(),
                    source_id: INGEST_SOURCE_ID.to_string(),
                    shard_id: 1,
                    ..Default::default()
                }],
                next_shard_id: 2,
            }];
            let response = ListShardsResponse { subresponses };
            Ok(response)
        });
        let replication_factor = 1;

        let (control_plane_mailbox, _control_plane_handle) = ControlPlane::spawn(
            &universe,
            cluster_id,
            self_node_id,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from(mock_metastore),
            replication_factor,
        );
        let get_open_shards_request = GetOrCreateOpenShardsRequest {
            subrequests: vec![GetOrCreateOpenShardsSubrequest {
                index_id: "test-index".to_string(),
                source_id: INGEST_SOURCE_ID.to_string(),
                closed_shards: Vec::new(),
            }],
            unavailable_ingesters: Vec::new(),
        };
        let get_open_shards_response = control_plane_mailbox
            .ask_for_res(get_open_shards_request)
            .await
            .unwrap();
        assert_eq!(get_open_shards_response.subresponses.len(), 1);

        let subresponse = &get_open_shards_response.subresponses[0];
        assert_eq!(subresponse.index_uid, "test-index:0");
        assert_eq!(subresponse.source_id, INGEST_SOURCE_ID);
        assert_eq!(subresponse.open_shards.len(), 1);
        assert_eq!(subresponse.open_shards[0].shard_id, 1);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_control_plane_close_shards() {
        // TODO: Write test when the RPC is actually called by ingesters.
    }

    #[tokio::test]
    async fn test_control_plane_supervision_reload_from_metastore() {
        let universe = Universe::default();
        let node_id = NodeId::new("test_node".to_string());
        let indexer_pool = IndexerPool::default();
        let ingester_pool = IngesterPool::default();
        let mut mock_metastore = MetastoreServiceClient::mock();

        let mut index_0 = IndexMetadata::for_test("test-index-0", "ram:///test-index-0");
        let source = SourceConfig::ingest_default();
        index_0.add_source(source.clone()).unwrap();

        mock_metastore
            .expect_list_indexes_metadata()
            .times(2) // 1 for the first initialization, 1 after the respawn of the control plane.
            .returning(|list_indexes_request: ListIndexesMetadataRequest| {
                assert_eq!(list_indexes_request, ListIndexesMetadataRequest::all());
                Ok(ListIndexesMetadataResponse::empty())
            });
        mock_metastore.expect_list_shards().return_once(
            |_list_shards_request: ListShardsRequest| {
                let list_shards_resp = ListShardsResponse {
                    subresponses: Vec::new(),
                };
                Ok(list_shards_resp)
            },
        );
        let index_uid = IndexUid::new("test-index");
        let index_uid_string = index_uid.to_string();
        mock_metastore.expect_create_index().times(1).return_once(
            |_create_index_request: CreateIndexRequest| {
                Ok(CreateIndexResponse {
                    index_uid: index_uid_string,
                })
            },
        );
        mock_metastore.expect_create_index().times(1).return_once(
            |create_index_request: CreateIndexRequest| {
                Err(MetastoreError::AlreadyExists(EntityKind::Index {
                    index_id: create_index_request
                        .deserialize_index_config()
                        .unwrap()
                        .index_id,
                }))
            },
        );
        mock_metastore.expect_create_index().times(1).return_once(
            |_create_index_request: CreateIndexRequest| {
                Err(MetastoreError::Connection {
                    message: "Fake connection error.".to_string(),
                })
            },
        );

        let (control_plane_mailbox, control_plane_handle) = ControlPlane::spawn(
            &universe,
            "cluster".to_string(),
            node_id,
            indexer_pool,
            ingester_pool,
            MetastoreServiceClient::from(mock_metastore),
            1,
        );

        let index_config = IndexConfig::for_test("test-index", "ram:///test-index");
        let create_index_request = CreateIndexRequest {
            index_config_json: serde_json::to_string(&index_config).unwrap(),
        };

        // A happy path: we simply create the index.
        assert!(control_plane_mailbox
            .ask_for_res(create_index_request.clone())
            .await
            .is_ok());

        // Now let's see what happens if we attempt to create the same index a second time.
        let control_plane_error: ControlPlaneError = control_plane_mailbox
            .ask(create_index_request.clone())
            .await
            .unwrap()
            .unwrap_err();

        // That kind of error clearly indicates that the transaction has failed.
        // The control plane does not need to be restarted.
        assert!(
            matches!(control_plane_error, ControlPlaneError::Metastore(MetastoreError::AlreadyExists(entity)) if entity == EntityKind::Index { index_id: "test-index".to_string() })
        );

        control_plane_mailbox.ask(Observe).await.unwrap();

        assert_eq!(
            control_plane_handle
                .process_pending_and_observe()
                .await
                .metrics,
            SupervisorMetrics {
                num_panics: 0,
                num_errors: 0,
                num_kills: 0
            }
        );

        // Now let's see what happens with a grayer type of error.
        let control_plane_error: AskError<ControlPlaneError> = control_plane_mailbox
            .ask_for_res(create_index_request)
            .await
            .unwrap_err();
        assert!(matches!(control_plane_error, AskError::ProcessMessageError));

        // This time, the control plane is restarted.
        control_plane_mailbox.ask(Observe).await.unwrap();
        assert_eq!(
            control_plane_handle
                .process_pending_and_observe()
                .await
                .metrics,
            SupervisorMetrics {
                num_panics: 0,
                num_errors: 1,
                num_kills: 0
            }
        );

        universe.assert_quit().await;
    }
}
