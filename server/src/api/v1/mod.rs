mod cache_config;
mod chunked_upload;
mod get_missing_paths;
mod upload_path;

use axum::{
    Router,
    routing::{delete, get, patch, post, put},
};

pub(crate) fn get_router() -> Router {
    Router::new()
        .route(
            "/_api/v1/get-missing-paths",
            post(get_missing_paths::get_missing_paths),
        )
        .route("/_api/v1/upload-path", put(upload_path::upload_path))
        .route(
            "/_api/v1/upload-path/chunked",
            post(chunked_upload::begin_chunked_upload),
        )
        .route(
            "/_api/v1/upload-path/chunked/{upload_id}/chunks",
            post(chunked_upload::attach_chunks),
        )
        .route(
            "/_api/v1/upload-path/chunked/{upload_id}/chunk/{seq}",
            put(chunked_upload::upload_session_chunk),
        )
        .route(
            "/_api/v1/upload-path/chunked/{upload_id}/finish",
            post(chunked_upload::finish_chunked_upload),
        )
        .route(
            "/{cache}/attic-cache-info",
            get(cache_config::get_cache_config),
        )
        .route(
            "/_api/v1/cache-config/{cache}",
            get(cache_config::get_cache_config),
        )
        .route(
            "/_api/v1/cache-config/{cache}",
            post(cache_config::create_cache),
        )
        .route(
            "/_api/v1/cache-config/{cache}",
            patch(cache_config::configure_cache),
        )
        .route(
            "/_api/v1/cache-config/{cache}",
            delete(cache_config::destroy_cache),
        )
}
