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

use bytes::Bytes;

use super::types::{NodeId, ShardId, SourceId};
use super::{IndexUid, ServiceError, ServiceErrorCode};
use crate::control_plane::ControlPlaneError;

pub mod ingester;
pub mod router;

include!("../codegen/quickwit/quickwit.ingest.rs");

pub type IngestV2Result<T> = std::result::Result<T, IngestV2Error>;

#[derive(Debug, Clone, thiserror::Error)]
pub enum IngestV2Error {
    #[error("an internal error occurred: {0}")]
    Internal(String),
    #[error("failed to connect to ingester `{ingester_id}`")]
    IngesterUnavailable { ingester_id: NodeId },
    #[error(
        "ingest service is currently unavailable with {num_ingesters} in the cluster and a \
         replication factor of {replication_factor}"
    )]
    ServiceUnavailable {
        num_ingesters: usize,
        replication_factor: usize,
    },
    // #[error("Could not find shard.")]
    // ShardNotFound {
    //     index_uid: IndexUid,
    //     source_id: SourceId,
    //     shard_id: ShardId,
    // },
    #[error("failed to open or write to shard")]
    ShardUnavailable {
        leader_id: NodeId,
        index_uid: IndexUid,
        source_id: SourceId,
        shard_id: ShardId,
    },
    #[error("request timed out")]
    Timeout,
}

impl From<ControlPlaneError> for IngestV2Error {
    fn from(error: ControlPlaneError) -> Self {
        Self::Internal(error.to_string())
    }
}

impl From<IngestV2Error> for tonic::Status {
    fn from(error: IngestV2Error) -> tonic::Status {
        let code = match &error {
            IngestV2Error::IngesterUnavailable { .. } => tonic::Code::Unavailable,
            IngestV2Error::Internal(_) => tonic::Code::Internal,
            IngestV2Error::ServiceUnavailable { .. } => tonic::Code::Unavailable,
            IngestV2Error::ShardUnavailable { .. } => tonic::Code::Unavailable,
            IngestV2Error::Timeout { .. } => tonic::Code::DeadlineExceeded,
        };
        let message = error.to_string();
        tonic::Status::new(code, message)
    }
}

impl From<tonic::Status> for IngestV2Error {
    fn from(status: tonic::Status) -> Self {
        IngestV2Error::Internal(status.message().to_string())
    }
}

impl ServiceError for IngestV2Error {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::Internal { .. } => ServiceErrorCode::Internal,
            Self::IngesterUnavailable { .. } => ServiceErrorCode::Unavailable,
            Self::ShardUnavailable { .. } => ServiceErrorCode::Unavailable,
            Self::ServiceUnavailable { .. } => ServiceErrorCode::Unavailable,
            Self::Timeout { .. } => ServiceErrorCode::Timeout,
        }
    }
}

impl DocBatchV2 {
    pub fn docs(&self) -> impl Iterator<Item = Bytes> + '_ {
        self.doc_lengths.iter().scan(0, |start_offset, doc_length| {
            let start = *start_offset;
            let end = start + *doc_length as usize;
            *start_offset = end;
            Some(self.doc_buffer.slice(start..end))
        })
    }

    pub fn is_empty(&self) -> bool {
        self.doc_lengths.is_empty()
    }

    pub fn num_bytes(&self) -> usize {
        self.doc_buffer.len()
    }

    pub fn num_docs(&self) -> usize {
        self.doc_lengths.len()
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(docs: impl IntoIterator<Item = &'static str>) -> Self {
        let mut doc_buffer = Vec::new();
        let mut doc_lengths = Vec::new();

        for doc in docs {
            doc_buffer.extend(doc.as_bytes());
            doc_lengths.push(doc.len() as u32);
        }
        Self {
            doc_lengths,
            doc_buffer: Bytes::from(doc_buffer),
        }
    }
}

impl MRecordBatch {
    pub fn encoded_mrecords(&self) -> impl Iterator<Item = Bytes> + '_ {
        self.mrecord_lengths
            .iter()
            .scan(0, |start_offset, mrecord_length| {
                let start = *start_offset;
                let end = start + *mrecord_length as usize;
                *start_offset = end;
                Some(self.mrecord_buffer.slice(start..end))
            })
    }

    pub fn is_empty(&self) -> bool {
        self.mrecord_lengths.is_empty()
    }

    pub fn num_bytes(&self) -> usize {
        self.mrecord_buffer.len()
    }

    pub fn num_mrecords(&self) -> usize {
        self.mrecord_lengths.len()
    }
}

impl Shard {
    pub fn is_open(&self) -> bool {
        self.shard_state() == ShardState::Open
    }

    pub fn is_fenced(&self) -> bool {
        self.shard_state() == ShardState::Fenced
    }

    pub fn is_closed(&self) -> bool {
        self.shard_state() == ShardState::Closed
    }

    pub fn is_deletable(&self) -> bool {
        self.is_closed() && !self.has_unpublished_docs()
    }

    pub fn is_indexable(&self) -> bool {
        !self.is_closed() || self.has_unpublished_docs()
    }

    pub fn has_unpublished_docs(&self) -> bool {
        self.publish_position_inclusive.parse::<u64>().ok() < self.replication_position_inclusive
    }

    pub fn queue_id(&self) -> super::types::QueueId {
        super::types::queue_id(&self.index_uid, &self.source_id, self.shard_id)
    }
}

impl ShardState {
    pub fn is_open(&self) -> bool {
        *self == ShardState::Open
    }

    pub fn is_fenced(&self) -> bool {
        *self == ShardState::Fenced
    }

    pub fn is_closed(&self) -> bool {
        *self == ShardState::Closed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_as_unpublished_docs() {
        let shard = Shard {
            publish_position_inclusive: "".to_string(),
            replication_position_inclusive: None,
            ..Default::default()
        };
        assert!(!shard.has_unpublished_docs());

        let shard = Shard {
            publish_position_inclusive: "".to_string(),
            replication_position_inclusive: Some(0),
            ..Default::default()
        };
        assert!(shard.has_unpublished_docs());

        let shard = Shard {
            publish_position_inclusive: "0".to_string(),
            replication_position_inclusive: Some(0),
            ..Default::default()
        };
        assert!(!shard.has_unpublished_docs());
    }
}
