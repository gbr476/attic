//! Chunked uploads.
//!
//! With a chunked upload, the client splits the NAR into chunks itself, using
//! the same content-defined chunking parameters as the server, and only sends
//! the chunks that the server does not have yet. Compared to
//! `PUT /_api/v1/upload-path`, which always streams the whole NAR, this saves
//! the transfer of deduplicated data, and an interrupted upload can be resumed:
//! chunks that already reached the server are not sent again.
//!
//! The flow is:
//!
//! 1. `POST /_api/v1/upload-path/chunked` with the [`UploadPathNarInfo`] starts
//!    an upload session. If the NAR already exists on the server, the path is
//!    added to the cache right away and no session is created.
//! 2. `POST /_api/v1/upload-path/chunked/{upload_id}/chunks` with a batch of
//!    chunk hashes attaches every chunk the server already has and returns the
//!    sequence numbers of the missing ones.
//! 3. `PUT /_api/v1/upload-path/chunked/{upload_id}/chunk/{seq}` uploads one
//!    missing chunk. Repeating it is harmless.
//! 4. `POST /_api/v1/upload-path/chunked/{upload_id}/finish` checks that every
//!    chunk is present, verifies the NAR, and adds the path to the cache.
//!
//! A server only offers chunked uploads (see `CacheConfig::chunked_upload`)
//! when it does not require proof of possession, since a chunk query reveals
//! whether the server holds data with a given hash.

use serde::{Deserialize, Serialize};

use super::upload_path::{UploadPathNarInfo, UploadPathResult};
use crate::hash::Hash;

/// Header containing the hash of an uploaded chunk.
pub const ATTIC_CHUNK_HASH: &str = "X-Attic-Chunk-Hash";

/// Chunking parameters that a server accepting chunked uploads uses.
///
/// The client must split NARs with exactly these parameters, otherwise its
/// chunks will not match the ones already on the server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkedUploadConfig {
    /// NARs smaller than this are not chunked and must be uploaded with
    /// `PUT /_api/v1/upload-path`.
    pub nar_size_threshold: usize,

    /// Minimum chunk size, in bytes.
    pub min_size: usize,

    /// Average chunk size, in bytes.
    pub avg_size: usize,

    /// Maximum chunk size, in bytes.
    pub max_size: usize,

    /// Maximum number of chunks in one `chunks` request.
    pub max_chunks_per_request: usize,
}

/// Request to start a chunked upload.
#[derive(Debug, Serialize, Deserialize)]
pub struct BeginChunkedUploadRequest {
    pub nar_info: UploadPathNarInfo,
}

/// Response to starting a chunked upload.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum BeginChunkedUploadResponse {
    /// An upload session was created.
    Session {
        /// Opaque identifier of the session.
        upload_id: String,
    },

    /// Nothing needs to be uploaded: the path is now in the cache.
    Done { result: UploadPathResult },
}

/// A chunk of a NAR.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkInfo {
    /// The zero-indexed position of the chunk in the NAR.
    pub seq: u32,

    /// The SHA-256 hash of the (uncompressed) chunk.
    pub hash: Hash,

    /// The size of the chunk, in bytes.
    pub size: usize,
}

/// Request to attach chunks the server already has.
#[derive(Debug, Serialize, Deserialize)]
pub struct AttachChunksRequest {
    pub chunks: Vec<ChunkInfo>,
}

/// Response to attaching chunks.
#[derive(Debug, Serialize, Deserialize)]
pub struct AttachChunksResponse {
    /// Sequence numbers of the chunks the server does not have.
    ///
    /// These must be uploaded with `PUT .../chunk/{seq}`.
    pub missing: Vec<u32>,
}

/// Response to uploading a chunk.
#[derive(Debug, Serialize, Deserialize)]
pub struct UploadChunkResponse {
    /// Whether the server already had the chunk.
    pub deduplicated: bool,
}

/// Request to finish a chunked upload.
#[derive(Debug, Serialize, Deserialize)]
pub struct FinishChunkedUploadRequest {
    /// The total number of chunks in the NAR.
    pub num_chunks: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn begin_response_is_tagged() {
        let session = BeginChunkedUploadResponse::Session {
            upload_id: "abc".to_string(),
        };
        let json = serde_json::to_string(&session).unwrap();
        assert_eq!(json, r#"{"kind":"Session","upload_id":"abc"}"#);

        let parsed: BeginChunkedUploadResponse = serde_json::from_str(&json).unwrap();
        assert!(matches!(
            parsed,
            BeginChunkedUploadResponse::Session { upload_id } if upload_id == "abc"
        ));
    }

    #[test]
    fn chunk_info_round_trip() {
        let chunk = ChunkInfo {
            seq: 3,
            hash: Hash::sha256_from_bytes(b"chunk"),
            size: 5,
        };
        let json = serde_json::to_string(&chunk).unwrap();
        let parsed: ChunkInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.seq, 3);
        assert_eq!(parsed.hash, chunk.hash);
        assert_eq!(parsed.size, 5);
    }
}
