//! Chunked uploads.
//!
//! See `attic::api::v1::chunked_upload` for the protocol. On the server, a
//! session owns a NAR in the `PendingUpload` state. Every chunk the client
//! sends or the server already has is attached to that NAR with a `chunkref`
//! right away, which protects it from garbage collection: an interrupted
//! upload keeps its chunks, and when the client retries, the chunks are found
//! and attached again instead of being sent again. Garbage collection removes
//! sessions older than `chunked-upload.pending-upload-ttl`.

use std::collections::HashSet;

use anyhow::anyhow;
use async_compression::tokio::bufread::{BrotliDecoder, XzDecoder, ZstdDecoder};
use axum::{
    body::Body,
    extract::{Extension, Json, Path},
    http::HeaderMap,
};
use chrono::Utc;
use sea_orm::ActiveValue::Set;
use sea_orm::entity::prelude::*;
use sea_orm::{QueryOrder, QuerySelect, TransactionTrait};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tracing::instrument;
use uuid::Uuid;

use super::upload_path::{ChunkData, UploadPathNarInfoExt, upload_chunk};
use crate::database::AtticDatabase;
use crate::database::entity::cache::CacheModel;
use crate::database::entity::chunk::{self, ChunkState, Entity as Chunk};
use crate::database::entity::chunkref::{self, Entity as ChunkRef};
use crate::database::entity::nar::{self, Entity as Nar, NarState};
use crate::database::entity::object::{Entity as Object, InsertExt};
use crate::database::entity::upload_session::{self, Entity as UploadSession, UploadSessionModel};
use crate::error::{ErrorKind, ServerError, ServerResult};
use crate::narinfo::Compression;
use crate::storage::{Download, StorageBackend};
use crate::{RequestState, State};
use attic::api::v1::chunked_upload::{
    ATTIC_CHUNK_HASH, AttachChunksRequest, AttachChunksResponse, BeginChunkedUploadRequest,
    BeginChunkedUploadResponse, FinishChunkedUploadRequest, UploadChunkResponse,
};
use attic::api::v1::upload_path::{UploadPathNarInfo, UploadPathResult, UploadPathResultKind};
use attic::hash::Hash;

fn request_error(message: String) -> ServerError {
    ErrorKind::RequestError(anyhow!(message)).into()
}

fn ensure_available(state: &State) -> ServerResult<()> {
    if state.config.chunked_upload_available() {
        Ok(())
    } else {
        Err(request_error(
            "This server does not accept chunked uploads".to_string(),
        ))
    }
}

/// Starts a chunked upload.
#[instrument(skip_all)]
pub(crate) async fn begin_chunked_upload(
    Extension(state): Extension<State>,
    Extension(req_state): Extension<RequestState>,
    Json(payload): Json<BeginChunkedUploadRequest>,
) -> ServerResult<Json<BeginChunkedUploadResponse>> {
    ensure_available(&state)?;

    let upload_info = payload.nar_info;
    let database = state.database().await?;
    let cache = req_state
        .auth
        .auth_cache(database, &upload_info.cache, |cache, permission| {
            permission.require_push()?;
            Ok(cache)
        })
        .await?;
    let username = req_state.auth.username().map(str::to_string);

    let nar_size_threshold = state.config.chunking.nar_size_threshold;
    if upload_info.nar_size < nar_size_threshold {
        return Err(request_error(format!(
            "NARs smaller than {nar_size_threshold} bytes are not chunked; \
             upload them with PUT /_api/v1/upload-path"
        )));
    }

    // The NAR may exist already. Without proof of possession (a precondition
    // for chunked uploads), knowing the NAR hash is enough to use it.
    if let Some(existing_nar) = database.find_and_lock_nar(&upload_info.nar_hash).await? {
        let missing_chunk = ChunkRef::find()
            .filter(chunkref::Column::NarId.eq(existing_nar.id))
            .filter(chunkref::Column::ChunkId.is_null())
            .limit(1)
            .one(database)
            .await
            .map_err(ServerError::database_error)?;

        if missing_chunk.is_none() {
            Object::insert({
                let mut new_object = upload_info.to_active_model();
                new_object.cache_id = Set(cache.id);
                new_object.nar_id = Set(existing_nar.id);
                new_object.created_at = Set(Utc::now());
                new_object.created_by = Set(username);
                new_object
            })
            .on_conflict_do_update()
            .exec(database)
            .await
            .map_err(ServerError::database_error)?;

            drop(existing_nar);

            return Ok(Json(BeginChunkedUploadResponse::Done {
                result: UploadPathResult {
                    kind: UploadPathResultKind::Deduplicated,
                    file_size: None,
                    frac_deduplicated: None,
                },
            }));
        }
    }

    let compression: Compression = state.config.compression.r#type.into();
    let nar_size_db = i64::try_from(upload_info.nar_size).map_err(ServerError::request_error)?;
    let nar_info_json = serde_json::to_string(&upload_info).map_err(ServerError::request_error)?;

    // 244 random bits
    let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let now = Utc::now();

    let txn = database
        .begin()
        .await
        .map_err(ServerError::database_error)?;

    let nar_id = Nar::insert(nar::ActiveModel {
        state: Set(NarState::PendingUpload),
        compression: Set(compression.to_string()),
        nar_hash: Set(upload_info.nar_hash.to_typed_base16()),
        nar_size: Set(nar_size_db),
        num_chunks: Set(0),
        created_at: Set(now),
        ..Default::default()
    })
    .exec(&txn)
    .await
    .map_err(ServerError::database_error)?
    .last_insert_id;

    UploadSession::insert(upload_session::ActiveModel {
        token: Set(token.clone()),
        nar_id: Set(nar_id),
        cache_id: Set(cache.id),
        nar_info: Set(nar_info_json),
        created_by: Set(username),
        created_at: Set(now),
        ..Default::default()
    })
    .exec(&txn)
    .await
    .map_err(ServerError::database_error)?;

    txn.commit().await.map_err(ServerError::database_error)?;

    Ok(Json(BeginChunkedUploadResponse::Session {
        upload_id: token,
    }))
}

/// Attaches the chunks the server already has, returning the missing ones.
#[instrument(skip_all)]
pub(crate) async fn attach_chunks(
    Extension(state): Extension<State>,
    Extension(req_state): Extension<RequestState>,
    Path(upload_id): Path<String>,
    Json(payload): Json<AttachChunksRequest>,
) -> ServerResult<Json<AttachChunksResponse>> {
    let (session, _, _) = find_session(&state, &req_state, &upload_id).await?;
    let database = state.database().await?;

    let max_chunks = state.config.chunked_upload.max_chunks_per_request;
    if payload.chunks.len() > max_chunks {
        return Err(request_error(format!(
            "At most {max_chunks} chunks can be submitted at once"
        )));
    }

    let max_size = state.config.chunking.max_size;
    let mut seqs = Vec::with_capacity(payload.chunks.len());
    for chunk in &payload.chunks {
        if chunk.size == 0 || chunk.size > max_size {
            return Err(request_error(format!(
                "Chunk {} has an invalid size of {} bytes",
                chunk.seq, chunk.size
            )));
        }
        seqs.push(seq_to_db(chunk.seq)?);
    }

    let mut attached: HashSet<i32> = ChunkRef::find()
        .filter(chunkref::Column::NarId.eq(session.nar_id))
        .filter(chunkref::Column::Seq.is_in(seqs))
        .all(database)
        .await
        .map_err(ServerError::database_error)?
        .into_iter()
        .map(|chunkref| chunkref.seq)
        .collect();

    let compression: Compression = state.config.compression.r#type.into();
    let mut missing = Vec::new();

    for chunk in payload.chunks {
        let seq = seq_to_db(chunk.seq)?;
        if attached.contains(&seq) {
            continue;
        }

        match database
            .find_and_lock_chunk(&chunk.hash, compression)
            .await?
        {
            Some(existing) if existing.chunk_size as usize == chunk.size => {
                // The chunk stays locked until the chunkref protects it
                ChunkRef::insert(chunkref::ActiveModel {
                    nar_id: Set(session.nar_id),
                    seq: Set(seq),
                    chunk_id: Set(Some(existing.id)),
                    ..Default::default()
                })
                .exec(database)
                .await
                .map_err(ServerError::database_error)?;

                drop(existing);
                attached.insert(seq);
            }
            _ => missing.push(chunk.seq),
        }
    }

    Ok(Json(AttachChunksResponse { missing }))
}

/// Uploads one chunk of a chunked upload.
#[instrument(skip_all, fields(seq))]
pub(crate) async fn upload_session_chunk(
    Extension(state): Extension<State>,
    Extension(req_state): Extension<RequestState>,
    Path((upload_id, seq)): Path<(String, u32)>,
    headers: HeaderMap,
    body: Body,
) -> ServerResult<Json<UploadChunkResponse>> {
    let (session, _, _) = find_session(&state, &req_state, &upload_id).await?;
    let database = state.database().await?;
    let seq_db = seq_to_db(seq)?;

    let claimed_hash = headers
        .get(ATTIC_CHUNK_HASH)
        .ok_or_else(|| request_error(format!("{ATTIC_CHUNK_HASH} must be set")))?
        .to_str()
        .map_err(ServerError::request_error)?;
    let claimed_hash = Hash::from_typed(claimed_hash)?;

    let max_size = state.config.chunking.max_size;
    let bytes = axum::body::to_bytes(body, max_size).await.map_err(|e| {
        request_error(format!(
            "Invalid chunk body (at most {max_size} bytes): {e}"
        ))
    })?;

    if bytes.is_empty() {
        return Err(request_error("The chunk is empty".to_string()));
    }

    if Hash::sha256_from_bytes(&bytes) != claimed_hash {
        return Err(request_error(format!(
            "Chunk {seq} does not match its hash"
        )));
    }

    // Uploading a chunk twice is harmless
    let already_attached = ChunkRef::find()
        .filter(chunkref::Column::NarId.eq(session.nar_id))
        .filter(chunkref::Column::Seq.eq(seq_db))
        .one(database)
        .await
        .map_err(ServerError::database_error)?
        .is_some();

    if already_attached {
        return Ok(Json(UploadChunkResponse { deduplicated: true }));
    }

    let compression_config = &state.config.compression;
    let chunk = upload_chunk(
        ChunkData::Bytes(bytes),
        compression_config.r#type,
        compression_config.level(),
        database.clone(),
        state.clone(),
        state.config.require_proof_of_possession,
    )
    .await?;

    ChunkRef::insert(chunkref::ActiveModel {
        nar_id: Set(session.nar_id),
        seq: Set(seq_db),
        chunk_id: Set(Some(chunk.guard.id)),
        ..Default::default()
    })
    .exec(database)
    .await
    .map_err(ServerError::database_error)?;

    Ok(Json(UploadChunkResponse {
        deduplicated: chunk.deduplicated,
    }))
}

/// Finishes a chunked upload.
#[instrument(skip_all)]
pub(crate) async fn finish_chunked_upload(
    Extension(state): Extension<State>,
    Extension(req_state): Extension<RequestState>,
    Path(upload_id): Path<String>,
    Json(payload): Json<FinishChunkedUploadRequest>,
) -> ServerResult<Json<UploadPathResult>> {
    let (session, cache, upload_info) = find_session(&state, &req_state, &upload_id).await?;
    let database = state.database().await?;

    let num_chunks = payload.num_chunks as usize;

    // Chunks that are not valid (any more) drop out of the join and are
    // reported as missing below.
    let chunkrefs: Vec<(chunkref::Model, Option<chunk::Model>)> = ChunkRef::find()
        .filter(chunkref::Column::NarId.eq(session.nar_id))
        .find_also_related(Chunk)
        .filter(chunk::Column::State.eq(ChunkState::Valid))
        .order_by_asc(chunkref::Column::Seq)
        .all(database)
        .await
        .map_err(ServerError::database_error)?;

    let mut chunks = Vec::with_capacity(num_chunks);
    for (chunkref, chunk) in chunkrefs {
        let expected = chunks.len();
        match chunk {
            Some(chunk) if chunkref.seq as usize == expected => chunks.push(chunk),
            _ => break,
        }
    }

    if chunks.len() != num_chunks {
        return Err(request_error(format!(
            "Chunk {} is missing ({} of {} chunks are present)",
            chunks.len(),
            chunks.len(),
            num_chunks
        )));
    }

    let nar_size: u64 = chunks.iter().map(|c| c.chunk_size as u64).sum();
    if nar_size != upload_info.nar_size as u64 {
        return Err(request_error(format!(
            "The chunks add up to {nar_size} bytes, but the NAR has {} bytes",
            upload_info.nar_size
        )));
    }

    if state.config.chunked_upload.verify_nar_hash {
        let nar_hash = hash_stored_chunks(&state, &chunks).await?;
        if nar_hash != upload_info.nar_hash {
            // Start over: the chunks become orphans
            Nar::delete_by_id(session.nar_id)
                .exec(database)
                .await
                .map_err(ServerError::database_error)?;

            return Err(request_error(
                "The chunks do not match the NAR hash".to_string(),
            ));
        }
    }

    let file_size: i64 = chunks.iter().filter_map(|c| c.file_size).sum();

    let txn = database
        .begin()
        .await
        .map_err(ServerError::database_error)?;

    Nar::update(nar::ActiveModel {
        id: Set(session.nar_id),
        state: Set(NarState::Valid),
        num_chunks: Set(chunks.len() as i32),
        ..Default::default()
    })
    .exec(&txn)
    .await
    .map_err(ServerError::database_error)?;

    Object::insert({
        let mut new_object = upload_info.to_active_model();
        new_object.cache_id = Set(cache.id);
        new_object.nar_id = Set(session.nar_id);
        new_object.created_at = Set(Utc::now());
        new_object.created_by = Set(session.created_by.clone());
        new_object
    })
    .on_conflict_do_update()
    .exec(&txn)
    .await
    .map_err(ServerError::database_error)?;

    UploadSession::delete_by_id(session.id)
        .exec(&txn)
        .await
        .map_err(ServerError::database_error)?;

    txn.commit().await.map_err(ServerError::database_error)?;

    Ok(Json(UploadPathResult {
        kind: UploadPathResultKind::Uploaded,
        file_size: Some(file_size as usize),
        frac_deduplicated: None,
    }))
}

/// Looks up a session and checks that the requester may continue it.
async fn find_session(
    state: &State,
    req_state: &RequestState,
    upload_id: &str,
) -> ServerResult<(UploadSessionModel, CacheModel, UploadPathNarInfo)> {
    ensure_available(state)?;

    let database = state.database().await?;
    let session = UploadSession::find()
        .filter(upload_session::Column::Token.eq(upload_id))
        .one(database)
        .await
        .map_err(ServerError::database_error)?
        .ok_or_else(|| request_error("No such upload (it may have expired)".to_string()))?;

    let upload_info: UploadPathNarInfo =
        serde_json::from_str(&session.nar_info).map_err(ServerError::database_error)?;

    let cache = req_state
        .auth
        .auth_cache(database, &upload_info.cache, |cache, permission| {
            permission.require_push()?;
            Ok(cache)
        })
        .await?;

    if cache.id != session.cache_id || session.created_by.as_deref() != req_state.auth.username() {
        return Err(ErrorKind::Unauthorized.into());
    }

    Ok((session, cache, upload_info))
}

fn seq_to_db(seq: u32) -> ServerResult<i32> {
    i32::try_from(seq).map_err(|_| request_error(format!("Invalid chunk number {seq}")))
}

/// Reads the chunks back from storage and returns the hash of the NAR.
async fn hash_stored_chunks(state: &State, chunks: &[chunk::Model]) -> ServerResult<Hash> {
    let storage = state.storage().await?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];

    for chunk in chunks {
        let compression: Compression = chunk.compression.parse()?;
        let stream = match storage.download_file_db(&chunk.remote_file.0, true).await? {
            Download::AsyncRead(stream) => BufReader::new(stream),
            Download::Url(_) => {
                return Err(ErrorKind::StorageError(anyhow!(
                    "The storage backend returned a URL; the NAR cannot be verified"
                ))
                .into());
            }
        };

        let mut reader: Box<dyn AsyncRead + Unpin + Send> = match compression {
            Compression::None => Box::new(stream),
            Compression::Zstd => Box::new(ZstdDecoder::new(stream)),
            Compression::Xz => Box::new(XzDecoder::new(stream)),
            Compression::Brotli => Box::new(BrotliDecoder::new(stream)),
            Compression::Bzip2 => {
                return Err(ErrorKind::StorageError(anyhow!(
                    "Chunks compressed with bzip2 cannot be verified"
                ))
                .into());
            }
        };

        loop {
            let n = reader
                .read(&mut buf)
                .await
                .map_err(ServerError::storage_error)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
    }

    let hash = hasher.finalize();
    Ok(Hash::Sha256((&hash[..]).try_into().unwrap()))
}
