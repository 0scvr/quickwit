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

use std::{fmt};

use ulid::Ulid;
use serde::Serialize;
use serde::Deserialize;

/// Index identifiers that uniquely identify not only the index, but also
/// its incarnation allowing to distinguish between deleted and recreated indexes.
/// It is represented as a stiring in index_id:incarnation_id format.
#[derive(Clone, Debug, Default, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct IndexUid(String, Ulid);

impl IndexUid {
    /// Creates a new index UID from an `index_id` and `incarnation_id`.
    pub fn new(index_id: impl Into<String>, incarnation_id: Ulid) -> Self {
        Self(index_id.into(), incarnation_id)
    }

    // pub fn from_parts(index_id: impl Into<String>, incarnation_id: impl Into<String>) -> Self {
    //     let incarnation_id = incarnation_id.into();
    //     let index_id = index_id.into();
    //     if incarnation_id.is_empty() {
    //         Self(index_id)
    //     } else {
    //         Self(format!("{index_id}:{incarnation_id}"))
    //     }
    // }

    pub fn index_id(&self) -> &str {
        &self.0
    }

    pub fn incarnation_id(&self) -> Ulid {
        self.1
    }
}

impl fmt::Display for IndexUid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.0, self.1)
    }
}

impl Serialize for IndexUid {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for IndexUid {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // let index_uid = String::deserialize(deserializer)?;
        // let mut parts = index_uid.split(':');
        // let index_id = parts.next().ok_or_else(|| serde::de::Error::custom("missing index id"))?;
        // let incarnation_id = parts
        //     .next()
        //     .ok_or_else(|| serde::de::Error::custom("missing incarnation id"))?;
        // if parts.next().is_some() {
        //     return Err(serde::de::Error::custom("invalid index uid"));
        // }
        // let incarnation_id = Ulid::parse_str(incarnation_id)
        //     .map_err(|_| serde::de::Error::custom("invalid incarnation id"))?;
        // Ok(Self(index_id.to_string(), incarnation_id))
        Ok(IndexUid::default())
    }
}

/// Encodes an index UID into a protobuf message composed of one string and two u64 integers called `index_id`, `ulid_high` and `ulid_low`.
impl ::prost::Message for IndexUid {
    fn encode_raw<B>(&self, buf: &mut B)
    where
        B: ::prost::bytes::BufMut,
    {
        if self.0 != "" {
            ::prost::encoding::string::encode(1u32, &self.0, buf);
        }
        let (ulid_high, ulid_low): (u64, u64) = self.1.into();

        if ulid_high != 0u64 {
            ::prost::encoding::uint64::encode(2u32, &ulid_high, buf);
        }
        if ulid_low != 0u64 {
            ::prost::encoding::uint64::encode(3u32, &ulid_low, buf);
        }
    }

    fn merge_field<B>(
        &mut self,
        tag: u32,
        wire_type: ::prost::encoding::WireType,
        buf: &mut B,
        ctx: ::prost::encoding::DecodeContext,
    ) -> ::core::result::Result<(), ::prost::DecodeError>
    where
            B: ::prost::bytes::Buf,
        {
            const STRUCT_NAME: &'static str = "IndexUid";

            match tag {
                1u32 => {
                    let value = &mut self.0;
                    ::prost::encoding::string::merge(wire_type, value, buf, ctx)
                        .map_err(|mut error| {
                            error.push(STRUCT_NAME, "index_id");
                            error
                        })
                }
                2u32 => {
                    let (mut ulid_high, ulid_low) = self.1.into();
                    ::prost::encoding::uint64::merge(wire_type, &mut ulid_high, buf, ctx)
                        .map_err(|mut error| {
                            error.push(STRUCT_NAME, "ulid_high");
                            error
                        })?;
                    self.1 = (ulid_high, ulid_low).into();
                    Ok(())
                }
                3u32 => {
                    let (ulid_high, mut ulid_low) = self.1.into();
                    ::prost::encoding::uint64::merge(wire_type, &mut ulid_low, buf, ctx)
                        .map_err(|mut error| {
                            error.push(STRUCT_NAME, "ulid_low");
                            error
                        })?;
                    self.1 = (ulid_high, ulid_low).into();
                    Ok(())
                }
                _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx),
            }
        }

    #[inline]
    fn encoded_len(&self) -> usize {
        let mut len = 0;

        if self.0 != "" {
            len += ::prost::encoding::string::encoded_len(1u32, &self.0);
        }
        let (ulid_high, ulid_low): (u64, u64) = self.1.into();

        if ulid_high != 0u64 {
            len += ::prost::encoding::uint64::encoded_len(2u32, &ulid_high);
        }
        if ulid_low != 0u64 {
            len += ::prost::encoding::uint64::encoded_len(3u32, &ulid_low);
        }
        len
    }

    fn clear(&mut self) {
        self.0.clear();
        self.1 = Ulid::nil();
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;

    #[test]
    fn test_index_uid_serde() {
        let index_uid = IndexUid::new("test-index", Ulid::new());
        let json_serialized_index_uid = serde_json::to_string(&index_uid).unwrap();
        println!("{}", json_serialized_index_uid);
        let deserialized_index_uid: IndexUid = serde_json::from_str(&json_serialized_index_uid).unwrap();
        assert_eq!(index_uid, deserialized_index_uid);
    }

    #[test]
    fn test_index_uid_proto() {
        let index_uid = IndexUid::new("test-index", Ulid::new());

        let mut buffer = Vec::new();
        index_uid.encode_raw(&mut buffer);

        let decoded_index_uid = IndexUid::decode(&buffer[..]).unwrap();

        assert_eq!(index_uid, decoded_index_uid);
    }
}
