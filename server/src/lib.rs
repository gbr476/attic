#![deny(
    asm_sub_register,
    deprecated,
    missing_abi,
    unsafe_code,
    unused_macros,
    unused_must_use,
    unused_unsafe
)]
#![deny(clippy::from_over_into, clippy::needless_question_mark)]
#![cfg_attr(
    not(debug_assertions),
    deny(unused_imports, unused_mut, unused_variables,)
)]

pub mod access;
mod api;
mod compression;
pub mod config;
pub mod database;
pub mod error;
pub mod gc;
mod middleware;
mod narinfo;
pub mod nix_manifest;
pub mod oobe;
mod storage;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use axum::{
    Router,
    extract::Extension,
    http::{Uri, uri::Scheme},
};
use sea_orm::sqlx::sqlite::{SqliteJournalMode, SqliteSynchronous};
use sea_orm::{ConnectOptions, ConnectionTrait, Database, DatabaseConnection, query::Statement};
use tokio::net::TcpListener;
use tokio::sync::OnceCell;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::trace::TraceLayer;

use access::http::{AuthState, apply_auth};
use attic::cache::CacheName;
use config::{Config, StorageConfig};
use database::migration::{Migrator, MigratorTrait};
use error::{ErrorKind, ServerError, ServerResult};
use middleware::{init_request_state, restrict_host, set_visibility_header};
use storage::{LocalBackend, S3Backend, StorageBackendImpl};

type State = Arc<StateInner>;
type RequestState = Arc<RequestStateInner>;

/// Global server state.
#[derive(Debug)]
pub struct StateInner {
    /// The Attic Server configuration.
    config: Config,

    /// Handle to the database.
    database: OnceCell<DatabaseConnection>,

    /// Handle to the storage backend.
    storage: OnceCell<Arc<StorageBackendImpl>>,
}

/// Request state.
#[derive(Debug)]
struct RequestStateInner {
    /// Auth state.
    auth: AuthState,

    /// The canonical API endpoint.
    api_endpoint: Option<String>,

    /// The canonical substituter endpoint.
    substituter_endpoint: Option<String>,

    /// The potentially-invalid Host header supplied by the client.
    host: String,

    /// Whether the client claims the connection is HTTPS or not.
    client_claims_https: bool,

    /// Whether the cache the client's interacting with is public.
    ///
    /// This is purely informational and used to add the `X-Attic-Cache-Visibility`.
    /// header in responses.
    public_cache: AtomicBool,
}

impl StateInner {
    async fn new(config: Config) -> State {
        Arc::new(Self {
            config,
            database: OnceCell::new(),
            storage: OnceCell::new(),
        })
    }

    /// Returns a handle to the database.
    async fn database(&self) -> ServerResult<&DatabaseConnection> {
        self.database
            .get_or_try_init(|| async {
                Database::connect(database_connect_options(&self.config.database.url))
                    .await
                    .map_err(ServerError::database_error)
            })
            .await
    }

    /// Returns a handle to the storage backend.
    async fn storage(&self) -> ServerResult<&Arc<StorageBackendImpl>> {
        self.storage
            .get_or_try_init(|| async {
                match &self.config.storage {
                    StorageConfig::Local(local_config) => {
                        let local = LocalBackend::new(local_config.clone()).await?;
                        Ok(Arc::new(local.into()))
                    }
                    StorageConfig::S3(s3_config) => {
                        let s3 = S3Backend::new(s3_config.clone()).await?;
                        Ok(Arc::new(s3.into()))
                    }
                }
            })
            .await
    }

    /// Sends periodic heartbeat queries to the database.
    async fn run_db_heartbeat(&self) -> ServerResult<()> {
        let db = self.database().await?;
        let stmt =
            Statement::from_string(db.get_database_backend(), "SELECT 'heartbeat';".to_string());

        loop {
            let _ = db.execute_raw(stmt.clone()).await;
            time::sleep(Duration::from_secs(60)).await;
        }
    }
}

impl RequestStateInner {
    /// Returns the base API endpoint for clients.
    ///
    /// The APIs encompass both the Attic API and the Nix binary
    /// cache API.
    fn api_endpoint(&self) -> ServerResult<String> {
        if let Some(endpoint) = &self.api_endpoint {
            Ok(endpoint.to_owned())
        } else {
            // Naively synthesize from client's Host header
            // For convenience and shouldn't be used in production!
            let uri = Uri::builder()
                .scheme(if self.client_claims_https {
                    Scheme::HTTPS
                } else {
                    Scheme::HTTP
                })
                .authority(self.host.to_owned())
                .path_and_query("/")
                .build()
                .map_err(ServerError::request_error)?;

            Ok(uri.to_string())
        }
    }

    /// Returns the Nix binary cache endpoint for clients.
    ///
    /// The binary cache endpoint may live on another host than
    /// the canonical API endpoint.
    fn substituter_endpoint(&self, cache: CacheName) -> ServerResult<String> {
        if let Some(substituter_endpoint) = &self.substituter_endpoint {
            Ok(format!("{}{}", substituter_endpoint, cache.as_str()))
        } else {
            Ok(format!("{}{}", self.api_endpoint()?, cache.as_str()))
        }
    }

    /// Indicates whether the cache the client is interacting with is public.
    fn set_public_cache(&self, public: bool) {
        self.public_cache.store(public, Ordering::Relaxed);
    }
}

/// The fallback route.
#[axum_macros::debug_handler]
async fn fallback(_: Uri) -> ServerResult<()> {
    Err(ErrorKind::NotFound.into())
}

/// Runs the API server until shutdown is requested.
pub async fn run_api_server(
    cli_listen: Option<SocketAddr>,
    config: Config,
    shutdown: CancellationToken,
) -> Result<()> {
    eprintln!("Starting API server...");

    let state = StateInner::new(config).await;

    let listen = if let Some(cli_listen) = cli_listen {
        cli_listen
    } else {
        state.config.listen.to_owned()
    };

    let rest = Router::new()
        .merge(api::get_router())
        .fallback(fallback)
        // middlewares
        .layer(axum::middleware::from_fn(apply_auth))
        .layer(axum::middleware::from_fn(set_visibility_header))
        .layer(axum::middleware::from_fn(init_request_state))
        .layer(axum::middleware::from_fn(restrict_host))
        .layer(Extension(state.clone()))
        .layer(TraceLayer::new_for_http())
        .layer(CatchPanicLayer::new());

    eprintln!("Listening on {:?}...", listen);

    let listener = TcpListener::bind(&listen).await?;

    let server = axum::serve(listener, rest);

    let heartbeat_handle = if state.config.database.heartbeat {
        let state_clone = state.clone();
        Some(tokio::spawn(async move {
            let _ = state_clone.run_db_heartbeat().await;
        }))
    } else {
        None
    };

    let server_result = server
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await;

    if let Some(handle) = heartbeat_handle {
        handle.abort();
        let _ = handle.await;
    }

    server_result?;

    Ok(())
}

/// Runs database migrations.
pub async fn run_migrations(config: Config) -> Result<()> {
    eprintln!("Running migrations...");

    let state = StateInner::new(config).await;
    let db = state.database().await?;
    Migrator::up(db, None).await?;

    Ok(())
}

/// Connection options for the server database.
fn database_connect_options(url: &str) -> ConnectOptions {
    let mut options = ConnectOptions::new(url);

    // SQLite-specific performance tuning, see
    // https://phiresky.github.io/blog/2020/sqlite-performance-tuning/
    //
    // All but journal_mode are per-connection settings, so they are set on
    // every connection the pool opens. Running them once after connecting only
    // reached the connection open at that moment: sea-orm gives SQLite a pool
    // of one connection, and sqlx replaces it after 10 min idle or 30 min of
    // age, so within half an hour the server ran on SQLite's default
    // synchronous=FULL, an fsync per commit. On slow storage that caps write
    // throughput (an upload commits several times per chunk), every request
    // queues behind the single connection, and they fail with "Connection pool
    // timed out". synchronous=NORMAL is safe in WAL mode: the database cannot
    // be corrupted, only the latest commits can be lost on power failure.
    options
        .map_sqlx_sqlite_opts(|opts| {
            opts.journal_mode(SqliteJournalMode::Wal)
                .synchronous(SqliteSynchronous::Normal)
                .pragma("temp_store", "memory")
                .pragma("mmap_size", "30000000000")
                // sqlx's default is 5 s; wait rather than fail under bursts
                .busy_timeout(Duration::from_secs(30))
        })
        // sqlx's default is 30 s
        .acquire_timeout(Duration::from_secs(120));

    options
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every connection the pool opens must come up tuned, not just the first
    /// one: sqlx replaces pooled connections after an idle period or a maximum
    /// lifetime, so pragmas executed once after connecting are lost.
    #[tokio::test]
    async fn sqlite_connections_are_tuned() {
        let path = std::env::temp_dir().join(format!("attic-db-test-{}.db", std::process::id()));
        let url = format!("sqlite://{}?mode=rwc", path.display());

        let db = Database::connect(database_connect_options(&url))
            .await
            .unwrap();

        async fn pragma(db: &DatabaseConnection, name: &str) -> String {
            let stmt = Statement::from_string(db.get_database_backend(), format!("PRAGMA {name}"));
            let row = db.query_one_raw(stmt).await.unwrap().unwrap();
            row.try_get_by_index::<i64>(0)
                .map(|v| v.to_string())
                .or_else(|_| row.try_get_by_index::<String>(0))
                .unwrap()
        }

        assert_eq!(pragma(&db, "journal_mode").await, "wal");
        assert_eq!(pragma(&db, "synchronous").await, "1"); // NORMAL
        assert_eq!(pragma(&db, "temp_store").await, "2"); // MEMORY
        assert_eq!(pragma(&db, "busy_timeout").await, "30000");

        db.close().await.unwrap();
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }
}
