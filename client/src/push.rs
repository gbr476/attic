//! Store path uploader.
//!
//! There are two APIs: `Pusher` and `PushSession`.
//!
//! A `Pusher` simply dispatches `ValidPathInfo`s for workers to push. Use this
//! when you know all store paths to push beforehand. The push plan (closure, missing
//! paths, all path metadata) should be computed prior to pushing.
//!
//! A `PushSession`, on the other hand, accepts a stream of `StorePath`s and
//! takes care of retrieving the closure and path metadata. It automatically
//! batches expensive operations (closure computation, querying missing paths).
//! Use this when the list of store paths is streamed from some external
//! source (e.g., FS watcher, Unix Domain Socket) and a push plan cannot be
//! created statically.
//!
//! TODO: Refactor out progress reporting and support a simple output style without progress bars

use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use async_channel as channel;
use bytes::Bytes;
use futures::future::join_all;
use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use tokio::sync::{Mutex, mpsc};
use tokio::task::{JoinHandle, spawn};
use tokio::time;
use tokio_util::io::StreamReader;

use crate::api::{ApiClient, ApiError};
use attic::api::v1::cache_config::CacheConfig;
use attic::api::v1::chunked_upload::{BeginChunkedUploadResponse, ChunkInfo, ChunkedUploadConfig};
use attic::api::v1::upload_path::{UploadPathNarInfo, UploadPathResult, UploadPathResultKind};
use attic::cache::CacheName;
use attic::chunking::chunk_stream;
use attic::error::AtticResult;
use attic::hash::Hash;
use attic::nix_store::{NixStore, StorePath, StorePathHash, ValidPathInfo};

type JobSender = channel::Sender<ValidPathInfo>;
type JobReceiver = channel::Receiver<ValidPathInfo>;

/// Maximum number of chunks whose hashes are submitted at once in a chunked upload.
const CHUNK_BATCH_SIZE: usize = 256;

/// Maximum amount of chunk data held in memory per path in a chunked upload.
const CHUNK_BATCH_BYTES: usize = 32 * 1024 * 1024;

/// Number of attempts for an idempotent chunked-upload request.
const CHUNK_REQUEST_ATTEMPTS: u32 = 4;

/// Configuration for pushing store paths.
#[derive(Clone, Copy, Debug)]
pub struct PushConfig {
    /// The number of workers to spawn.
    pub num_workers: usize,

    /// Whether to always include the upload info in the PUT payload.
    pub force_preamble: bool,

    /// Whether to use chunked uploads when the server supports them.
    pub chunked_upload: bool,

    /// The number of chunks to upload in parallel, per path.
    pub chunk_jobs: usize,
}

/// Configuration for a push session.
#[derive(Clone, Copy, Debug)]
pub struct PushSessionConfig {
    /// Push the specified paths only and do not compute closures.
    pub no_closure: bool,

    /// Ignore the upstream cache filter.
    pub ignore_upstream_cache_filter: bool,
}

/// A handle to push store paths to a cache.
///
/// The caller is responsible for computing closures and
/// checking for paths that already exist on the remote
/// cache.
pub struct Pusher {
    api: ApiClient,
    store: Arc<NixStore>,
    cache: CacheName,
    cache_config: CacheConfig,
    workers: Vec<JoinHandle<HashMap<StorePath, Result<()>>>>,
    sender: JobSender,
}

/// A wrapper over a `Pusher` that accepts a stream of `StorePath`s.
///
/// Unlike a `Pusher`, a `PushSession` takes a stream of `StorePath`s
/// instead of `ValidPathInfo`s, taking care of retrieving the closure
/// and path metadata.
///
/// This is useful when the list of store paths is streamed from some
/// external source (e.g., FS watcher, Unix Domain Socket) and a push
/// plan cannot be computed statically.
///
/// ## Batching
///
/// Many store paths can be built in a short period of time, with each
/// having a big closure. It can be very inefficient if we were to compute
/// closure and query for missing paths for each individual path. This is
/// especially true if we have a lot of remote builders (e.g., `attic watch-store`
/// running alongside a beefy Hydra instance).
///
/// `PushSession` batches operations in order to minimize the number of
/// closure computations and API calls. It also remembers which paths already
/// exist on the remote cache. By default, it submits a batch if it's been 2
/// seconds since the last path is queued or it's been 10 seconds in total.
pub struct PushSession {
    /// Sender to the batching future.
    sender: channel::Sender<SessionQueueCommand>,

    /// Receiver of results.
    result_receiver: mpsc::Receiver<Result<HashMap<StorePath, Result<()>>>>,
}

enum SessionQueueCommand {
    Paths(Vec<StorePath>),
    Flush,
    Terminate,
}

enum SessionQueuePoll {
    Paths(Vec<StorePath>),
    Flush,
    Terminate,
    Closed,
    TimedOut,
}

#[derive(Debug)]
pub struct PushPlan {
    /// Store paths to push.
    pub store_path_map: HashMap<StorePathHash, ValidPathInfo>,

    /// The number of paths in the original full closure.
    pub num_all_paths: usize,

    /// Number of paths that have been filtered out because they are already cached.
    pub num_already_cached: usize,

    /// Number of paths that have been filtered out because they are signed by an upstream cache.
    pub num_upstream: usize,
}

/// Wrapper to update a progress bar as a NAR is streamed.
struct NarStreamProgress<S> {
    stream: S,
    bar: ProgressBar,
}

impl Pusher {
    pub fn new(
        store: Arc<NixStore>,
        api: ApiClient,
        cache: CacheName,
        cache_config: CacheConfig,
        mp: MultiProgress,
        config: PushConfig,
    ) -> Self {
        let (sender, receiver) = channel::unbounded();
        let mut workers = Vec::new();

        let chunked = if config.chunked_upload {
            cache_config.chunked_upload.clone()
        } else {
            None
        };

        for _ in 0..config.num_workers {
            workers.push(spawn(Self::worker(
                receiver.clone(),
                store.clone(),
                api.clone(),
                cache.clone(),
                mp.clone(),
                config,
                chunked.clone(),
            )));
        }

        Self {
            api,
            store,
            cache,
            cache_config,
            workers,
            sender,
        }
    }

    /// Queues a store path to be pushed.
    pub async fn queue(&self, path_info: ValidPathInfo) -> Result<()> {
        self.sender.send(path_info).await.map_err(|e| anyhow!(e))
    }

    /// Waits for all workers to terminate, returning all results.
    ///
    /// TODO: Stream the results with another channel
    pub async fn wait(self) -> HashMap<StorePath, Result<()>> {
        drop(self.sender);

        join_all(self.workers)
            .await
            .into_iter()
            .map(|joinresult| joinresult.unwrap())
            .fold(HashMap::new(), |mut acc, results| {
                acc.extend(results);
                acc
            })
    }

    /// Creates a push plan.
    pub async fn plan(
        &self,
        roots: Vec<StorePath>,
        no_closure: bool,
        ignore_upstream_filter: bool,
    ) -> Result<PushPlan> {
        PushPlan::plan(
            self.store.clone(),
            &self.api,
            &self.cache,
            &self.cache_config,
            roots,
            no_closure,
            ignore_upstream_filter,
        )
        .await
    }

    /// Converts the pusher into a `PushSession`.
    ///
    /// This is useful when the list of store paths is streamed from some
    /// external source (e.g., FS watcher, Unix Domain Socket) and a push
    /// plan cannot be computed statically.
    pub fn into_push_session(self, config: PushSessionConfig) -> PushSession {
        PushSession::with_pusher(self, config)
    }

    async fn worker(
        receiver: JobReceiver,
        store: Arc<NixStore>,
        api: ApiClient,
        cache: CacheName,
        mp: MultiProgress,
        config: PushConfig,
        chunked: Option<ChunkedUploadConfig>,
    ) -> HashMap<StorePath, Result<()>> {
        let mut results = HashMap::new();

        loop {
            let path_info = match receiver.recv().await {
                Ok(path_info) => path_info,
                Err(_) => {
                    // channel is closed - we are done
                    break;
                }
            };

            let store_path = path_info.path.clone();

            let r = upload_path(
                path_info,
                store.clone(),
                api.clone(),
                &cache,
                mp.clone(),
                config.force_preamble,
                chunked.as_ref(),
                config.chunk_jobs,
            )
            .await;

            results.insert(store_path, r);
        }

        results
    }
}

impl PushSession {
    pub fn with_pusher(pusher: Pusher, config: PushSessionConfig) -> Self {
        let (sender, receiver) = channel::unbounded();
        let (result_sender, result_receiver) = mpsc::channel(1);

        let known_paths_mutex = Arc::new(Mutex::new(HashSet::new()));

        spawn(async move {
            if let Err(e) = Self::worker(
                pusher,
                config,
                known_paths_mutex.clone(),
                receiver.clone(),
                result_sender.clone(),
            )
            .await
            {
                let _ = result_sender.send(Err(e)).await;
            }
        });

        Self {
            sender,
            result_receiver,
        }
    }

    async fn worker(
        pusher: Pusher,
        config: PushSessionConfig,
        known_paths_mutex: Arc<Mutex<HashSet<StorePathHash>>>,
        receiver: channel::Receiver<SessionQueueCommand>,
        result_sender: mpsc::Sender<Result<HashMap<StorePath, Result<()>>>>,
    ) -> Result<()> {
        let mut roots = HashSet::new();

        loop {
            // Get outstanding paths in queue
            let done = tokio::select! {
                // 2 seconds since last queued path
                done = async {
                    loop {
                        let poll = tokio::select! {
                            r = receiver.recv() => match r {
                                Ok(SessionQueueCommand::Paths(paths)) => SessionQueuePoll::Paths(paths),
                                Ok(SessionQueueCommand::Flush) => SessionQueuePoll::Flush,
                                Ok(SessionQueueCommand::Terminate) => SessionQueuePoll::Terminate,
                                _ => SessionQueuePoll::Closed,
                            },
                            _ = time::sleep(Duration::from_secs(2)) => SessionQueuePoll::TimedOut,
                        };

                        match poll {
                            SessionQueuePoll::Paths(store_paths) => {
                                roots.extend(store_paths.into_iter());
                            }
                            SessionQueuePoll::Closed | SessionQueuePoll::Terminate => {
                                break true;
                            }
                            SessionQueuePoll::Flush | SessionQueuePoll::TimedOut => {
                                break false;
                            }
                        }
                    }
                } => done,

                // 10 seconds
                _ = time::sleep(Duration::from_secs(10)) => {
                    false
                },
            };

            // Compute push plan
            let roots_vec: Vec<StorePath> = {
                let known_paths = known_paths_mutex.lock().await;
                roots
                    .drain()
                    .filter(|root| !known_paths.contains(&root.to_hash()))
                    .collect()
            };

            let mut plan = pusher
                .plan(
                    roots_vec,
                    config.no_closure,
                    config.ignore_upstream_cache_filter,
                )
                .await?;

            let mut known_paths = known_paths_mutex.lock().await;
            plan.store_path_map
                .retain(|sph, _| !known_paths.contains(sph));

            // Push everything
            for (store_path_hash, path_info) in plan.store_path_map.into_iter() {
                pusher.queue(path_info).await?;
                known_paths.insert(store_path_hash);
            }

            drop(known_paths);

            if done {
                let result = pusher.wait().await;
                result_sender.send(Ok(result)).await?;
                return Ok(());
            }
        }
    }

    /// Waits for all workers to terminate, returning all results.
    pub async fn wait(mut self) -> Result<HashMap<StorePath, Result<()>>> {
        self.flush()?;

        // The worker might have died
        let _ = self.sender.send(SessionQueueCommand::Terminate).await;

        self.result_receiver
            .recv()
            .await
            .expect("Nothing in result channel")
    }

    /// Queues multiple store paths to be pushed.
    pub fn queue_many(&self, store_paths: Vec<StorePath>) -> Result<()> {
        self.sender
            .send_blocking(SessionQueueCommand::Paths(store_paths))
            .map_err(|e| anyhow!(e))
    }

    /// Flushes the worker queue.
    pub fn flush(&self) -> Result<()> {
        self.sender
            .send_blocking(SessionQueueCommand::Flush)
            .map_err(|e| anyhow!(e))
    }
}

impl PushPlan {
    /// Creates a plan.
    async fn plan(
        store: Arc<NixStore>,
        api: &ApiClient,
        cache: &CacheName,
        cache_config: &CacheConfig,
        roots: Vec<StorePath>,
        no_closure: bool,
        ignore_upstream_filter: bool,
    ) -> Result<Self> {
        // Compute closure
        let closure = if no_closure {
            roots
        } else {
            store
                .compute_fs_closure_multi(roots, false, false, false)
                .await?
        };

        let mut store_path_map: HashMap<StorePathHash, ValidPathInfo> = {
            let futures = closure
                .iter()
                .map(|path| {
                    let store = store.clone();
                    let path = path.clone();
                    let path_hash = path.to_hash();

                    async move {
                        let path_info = store.query_path_info(path).await?;
                        Ok((path_hash, path_info))
                    }
                })
                .collect::<Vec<_>>();

            join_all(futures).await.into_iter().collect::<Result<_>>()?
        };

        let num_all_paths = store_path_map.len();
        if store_path_map.is_empty() {
            return Ok(Self {
                store_path_map,
                num_all_paths,
                num_already_cached: 0,
                num_upstream: 0,
            });
        }

        if !ignore_upstream_filter {
            // Filter out paths signed by upstream caches
            let upstream_cache_key_names = cache_config
                .upstream_cache_key_names
                .as_ref()
                .map_or([].as_slice(), |v| v.as_slice());
            store_path_map.retain(|_, pi| {
                for sig in &pi.sigs {
                    if let Some((name, _)) = sig.split_once(':')
                        && upstream_cache_key_names.iter().any(|u| name == u)
                    {
                        return false;
                    }
                }

                true
            });
        }

        let num_filtered_paths = store_path_map.len();
        if store_path_map.is_empty() {
            return Ok(Self {
                store_path_map,
                num_all_paths,
                num_already_cached: 0,
                num_upstream: num_all_paths - num_filtered_paths,
            });
        }

        // Query missing paths
        let missing_path_hashes: HashSet<StorePathHash> = {
            let store_path_hashes = store_path_map.keys().map(|sph| sph.to_owned()).collect();
            let res = api.get_missing_paths(cache, store_path_hashes).await?;
            res.missing_paths.into_iter().collect()
        };
        store_path_map.retain(|sph, _| missing_path_hashes.contains(sph));
        let num_missing_paths = store_path_map.len();

        Ok(Self {
            store_path_map,
            num_all_paths,
            num_already_cached: num_filtered_paths - num_missing_paths,
            num_upstream: num_all_paths - num_filtered_paths,
        })
    }
}

/// Uploads a single path to a cache.
///
/// With `chunked` (the server's chunked upload parameters), paths above the
/// chunking threshold are uploaded chunk by chunk, sending only the chunks
/// the server does not have.
#[allow(clippy::too_many_arguments)]
pub async fn upload_path(
    path_info: ValidPathInfo,
    store: Arc<NixStore>,
    api: ApiClient,
    cache: &CacheName,
    mp: MultiProgress,
    force_preamble: bool,
    chunked: Option<&ChunkedUploadConfig>,
    chunk_jobs: usize,
) -> Result<()> {
    let path = &path_info.path;
    let upload_info = {
        let full_path = store
            .get_full_path(path)
            .to_str()
            .ok_or_else(|| anyhow!("Path contains non-UTF-8"))?
            .to_string();

        let references = path_info
            .references
            .into_iter()
            .map(|pb| {
                pb.to_str()
                    .ok_or_else(|| anyhow!("Reference contains non-UTF-8"))
                    .map(|s| s.to_owned())
            })
            .collect::<Result<Vec<String>, anyhow::Error>>()?;

        UploadPathNarInfo {
            cache: cache.to_owned(),
            store_path_hash: path.to_hash(),
            store_path: full_path,
            references,
            system: None,  // TODO
            deriver: None, // TODO
            sigs: path_info.sigs,
            ca: path_info.ca,
            nar_hash: path_info.nar_hash.to_owned(),
            nar_size: path_info.nar_size as usize,
        }
    };

    let template = format!(
        "{{spinner}} {: <20.20} {{bar:40.green/blue}} {{human_bytes:10}} ({{average_speed}})",
        path.name(),
    );
    let style = ProgressStyle::with_template(&template)
        .unwrap()
        .tick_chars("🕛🕐🕑🕒🕓🕔🕕🕖🕗🕘🕙🕚✅")
        .progress_chars("██ ")
        .with_key("human_bytes", |state: &ProgressState, w: &mut dyn Write| {
            write!(w, "{}", HumanBytes(state.pos())).unwrap();
        })
        // Adapted from
        // <https://github.com/console-rs/indicatif/issues/394#issuecomment-1309971049>
        .with_key(
            "average_speed",
            |state: &ProgressState, w: &mut dyn Write| match (state.pos(), state.elapsed()) {
                (pos, elapsed) if elapsed > Duration::ZERO => {
                    write!(w, "{}", average_speed(pos, elapsed)).unwrap();
                }
                _ => write!(w, "-").unwrap(),
            },
        );
    let bar = mp.add(ProgressBar::new(path_info.nar_size));
    bar.set_style(style);
    let nar_stream = NarStreamProgress::new(store.nar_from_path(path.to_owned()), bar.clone());

    let chunked = chunked.filter(|c| {
        c.nar_size_threshold != 0 && path_info.nar_size as usize >= c.nar_size_threshold
    });

    let start = Instant::now();
    let result = match chunked {
        Some(chunking) => upload_path_chunked(&api, upload_info, nar_stream, chunking, chunk_jobs)
            .await
            .map(|(r, sent)| (r, Some(sent))),
        None => api
            .upload_path(upload_info, nar_stream.map_ok(Bytes::from), force_preamble)
            .await
            .map(|r| {
                let r = r.unwrap_or(UploadPathResult {
                    kind: UploadPathResultKind::Uploaded,
                    file_size: None,
                    frac_deduplicated: None,
                });
                (r, None)
            }),
    };

    match result {
        Ok((r, sent)) => {
            let info_string: String = match r.kind {
                UploadPathResultKind::Deduplicated => "deduplicated".to_string(),
                _ => {
                    let elapsed = start.elapsed();
                    let seconds = elapsed.as_secs_f64();
                    let speed = (path_info.nar_size as f64 / seconds) as u64;

                    let mut s = format!("{}/s", HumanBytes(speed));

                    if let Some(sent) = sent {
                        // Chunked upload: what actually crossed the network
                        s += &format!(", sent {}", HumanBytes(sent));
                        let frac_deduplicated = 1.0 - sent as f64 / path_info.nar_size as f64;
                        if frac_deduplicated > 0.01f64 {
                            s += &format!(", {:.1}% deduplicated", frac_deduplicated * 100.0);
                        }
                    } else if let Some(frac_deduplicated) = r.frac_deduplicated
                        && frac_deduplicated > 0.01f64
                    {
                        s += &format!(", {:.1}% deduplicated", frac_deduplicated * 100.0);
                    }

                    s
                }
            };

            mp.suspend(|| {
                eprintln!(
                    "✅ {} ({})",
                    path.as_os_str().to_string_lossy(),
                    info_string
                );
            });
            bar.finish_and_clear();

            Ok(())
        }
        Err(e) => {
            mp.suspend(|| {
                eprintln!("❌ {}: {}", path.as_os_str().to_string_lossy(), e);
            });
            bar.finish_and_clear();
            Err(e)
        }
    }
}

/// Uploads a path with a chunked upload, returning the result and the number
/// of chunk bytes sent.
///
/// The NAR is split with the server's chunking parameters. For each batch of
/// chunks, the server attaches the ones it already has and names the missing
/// ones, which are then uploaded. Chunks that reached the server in an earlier,
/// interrupted attempt count as present, so a retry only sends the rest.
async fn upload_path_chunked<S>(
    api: &ApiClient,
    upload_info: UploadPathNarInfo,
    nar_stream: S,
    chunking: &ChunkedUploadConfig,
    chunk_jobs: usize,
) -> Result<(UploadPathResult, u64)>
where
    S: Stream<Item = AtticResult<Vec<u8>>> + Unpin + Send,
{
    let upload_id = match api.begin_chunked_upload(upload_info).await? {
        BeginChunkedUploadResponse::Session { upload_id } => upload_id,
        BeginChunkedUploadResponse::Done { result } => return Ok((result, 0)),
    };

    let reader =
        StreamReader::new(nar_stream.map(|r| r.map(Bytes::from).map_err(io::Error::other)));
    let mut chunks = chunk_stream(
        reader,
        chunking.min_size,
        chunking.avg_size,
        chunking.max_size,
    );

    let batch_size = CHUNK_BATCH_SIZE.min(chunking.max_chunks_per_request).max(1);
    let mut batch: Vec<(ChunkInfo, Bytes)> = Vec::with_capacity(batch_size);
    let mut batch_bytes = 0;
    let mut num_chunks: u32 = 0;
    let mut sent = 0;

    while let Some(data) = chunks.next().await {
        let data = data?;
        batch_bytes += data.len();
        batch.push((
            ChunkInfo {
                seq: num_chunks,
                hash: Hash::sha256_from_bytes(&data),
                size: data.len(),
            },
            data,
        ));
        num_chunks += 1;

        if batch.len() >= batch_size || batch_bytes >= CHUNK_BATCH_BYTES {
            sent += upload_chunk_batch(api, &upload_id, &mut batch, chunk_jobs).await?;
            batch_bytes = 0;
        }
    }

    if !batch.is_empty() {
        sent += upload_chunk_batch(api, &upload_id, &mut batch, chunk_jobs).await?;
    }

    let result = api.finish_chunked_upload(&upload_id, num_chunks).await?;
    Ok((result, sent))
}

/// Submits a batch of chunks, uploads the missing ones and empties the batch.
///
/// Returns the number of bytes uploaded.
async fn upload_chunk_batch(
    api: &ApiClient,
    upload_id: &str,
    batch: &mut Vec<(ChunkInfo, Bytes)>,
    chunk_jobs: usize,
) -> Result<u64> {
    let infos: Vec<ChunkInfo> = batch.iter().map(|(info, _)| info.clone()).collect();
    let missing: HashSet<u32> = with_retries(|| api.attach_chunks(upload_id, infos.clone()))
        .await?
        .missing
        .into_iter()
        .collect();

    let uploads = batch
        .drain(..)
        .filter(|(info, _)| missing.contains(&info.seq))
        .map(|(info, data)| async move {
            let len = data.len() as u64;
            with_retries(|| api.upload_chunk(upload_id, info.seq, &info.hash, data.clone()))
                .await?;
            Ok::<u64, anyhow::Error>(len)
        });

    stream::iter(uploads)
        .buffer_unordered(chunk_jobs.max(1))
        .try_fold(0, |sent, len| async move { Ok(sent + len) })
        .await
}

/// Runs an idempotent request, retrying it on network and server errors.
async fn with_retries<T, F, Fut>(mut request: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut delay = Duration::from_secs(1);
    let mut attempt = 1;

    loop {
        match request().await {
            Ok(r) => return Ok(r),
            Err(e) => {
                // Client errors (bad request, unauthorized, ...) won't go away
                let retryable = e
                    .downcast_ref::<ApiError>()
                    .is_none_or(ApiError::is_server_error);

                if !retryable || attempt >= CHUNK_REQUEST_ATTEMPTS {
                    return Err(e);
                }
            }
        }

        time::sleep(delay).await;
        delay *= 2;
        attempt += 1;
    }
}

impl<S: Stream<Item = AtticResult<Vec<u8>>>> NarStreamProgress<S> {
    fn new(stream: S, bar: ProgressBar) -> Self {
        Self { stream, bar }
    }
}

impl<S: Stream<Item = AtticResult<Vec<u8>>> + Unpin> Stream for NarStreamProgress<S> {
    type Item = AtticResult<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).as_mut().poll_next(cx) {
            Poll::Ready(Some(data)) => {
                if let Ok(data) = &data {
                    self.bar.inc(data.len() as u64);
                }

                Poll::Ready(Some(data))
            }
            other => other,
        }
    }
}

// Just the average, no fancy sliding windows that cause wild fluctuations
// <https://github.com/console-rs/indicatif/issues/394>
fn average_speed(bytes: u64, duration: Duration) -> String {
    let speed = bytes as f64 * 1000_f64 / duration.as_millis() as f64;
    format!("{}/s", HumanBytes(speed as u64))
}
