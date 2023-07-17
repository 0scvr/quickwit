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

mod doc_processor;
mod index_serializer;
mod indexer;
mod indexing_controller_agent;
mod indexing_pipeline;
mod indexing_pipeline_manager;
mod merge_executor;
mod merge_pipeline;
mod merge_planner;
mod merge_split_downloader;
mod packager;
mod publisher;
mod sequencer;
mod uploader;

#[cfg(feature = "vrl")]
mod vrl_processing;

pub use indexing_controller_agent::IndexingControllerAgent;
pub use indexing_pipeline::{IndexingPipeline, IndexingPipelineHandles, IndexingPipelineParams};
pub use indexing_pipeline_manager::{
    IndexingPipelineManager, IndexingServiceCounters, IndexingServiceError, MergePipelineId,
    INDEXING_DIR_NAME,
};
pub use quickwit_proto::indexing::IndexingError;
pub use sequencer::Sequencer;

pub use self::doc_processor::{DocProcessor, DocProcessorCounters};
pub use self::index_serializer::IndexSerializer;
pub use self::indexer::{Indexer, IndexerCounters};
pub use self::merge_executor::{combine_partition_ids, merge_split_attrs, MergeExecutor};
pub use self::merge_pipeline::MergePipeline;
pub use self::merge_planner::MergePlanner;
pub use self::merge_split_downloader::MergeSplitDownloader;
pub use self::packager::Packager;
pub use self::publisher::{Publisher, PublisherCounters, PublisherType};
pub use self::uploader::{SplitsUpdateMailbox, Uploader, UploaderCounters, UploaderType};
