use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock, watch};
use tracing::{debug, info, warn};

use crate::config::RaftConfig;
use crate::error::{ProxyError, Result};
use crate::writer::DqMismatchSample;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeStatusMeta {
    pub node_id: u64,
    pub raft_addr: String,
    pub flight_addr: String,
    pub active_streams: i64,
    pub in_flight_records: i64,
    pub total_records_written: u64,
    pub aerospike_connected: bool,
    pub last_updated_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobLoadStatus {
    pub job_id: String,
    pub namespace: String,
    pub set_name: String,
    #[serde(default)]
    pub is_dq_validation: bool,
    pub state: JobState,
    pub total_rows_written: u64,
    pub total_batches_processed: u64,
    pub total_overload_retries: u64,
    pub total_other_retries: u64,
    #[serde(default)]
    pub total_rows_validated: u64,
    #[serde(default)]
    pub total_matched_rows: u64,
    #[serde(default)]
    pub total_mismatched_rows: u64,
    #[serde(default)]
    pub total_missing_keys: u64,
    #[serde(default)]
    pub total_bin_mismatch_rows: u64,
    #[serde(default)]
    pub dq_passed: Option<bool>,
    #[serde(default)]
    pub dq_mismatch_samples: Vec<DqMismatchSample>,
    pub per_node_rows: BTreeMap<u64, u64>,
    pub error_message: Option<String>,
    pub started_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterMetadataState {
    pub nodes: BTreeMap<u64, NodeStatusMeta>,
    pub jobs: BTreeMap<String, JobLoadStatus>,
}

impl ClusterMetadataState {
    pub fn apply(&mut self, cmd: &MetadataCommand) {
        match cmd {
            MetadataCommand::UpsertNodeStatus(meta) => {
                self.nodes.insert(meta.node_id, meta.clone());
            }
            MetadataCommand::RecordJobBatch {
                job_id,
                node_id,
                namespace,
                set_name,
                is_dq_validation,
                rows_written,
                overload_retries,
                other_retries,
                rows_validated,
                matched_rows,
                mismatched_rows,
                missing_keys,
                bin_mismatch_rows,
                mismatch_samples,
                timestamp_ms,
            } => {
                let entry = self
                    .jobs
                    .entry(job_id.clone())
                    .or_insert_with(|| JobLoadStatus {
                        job_id: job_id.clone(),
                        namespace: namespace.clone(),
                        set_name: set_name.clone(),
                        is_dq_validation: *is_dq_validation,
                        state: JobState::Running,
                        total_rows_written: 0,
                        total_batches_processed: 0,
                        total_overload_retries: 0,
                        total_other_retries: 0,
                        total_rows_validated: 0,
                        total_matched_rows: 0,
                        total_mismatched_rows: 0,
                        total_missing_keys: 0,
                        total_bin_mismatch_rows: 0,
                        dq_passed: None,
                        dq_mismatch_samples: Vec::new(),
                        per_node_rows: BTreeMap::new(),
                        error_message: None,
                        started_at_ms: *timestamp_ms,
                        updated_at_ms: *timestamp_ms,
                    });
                if *is_dq_validation {
                    entry.is_dq_validation = true;
                }
                entry.total_rows_written = entry.total_rows_written.saturating_add(*rows_written);
                entry.total_batches_processed = entry.total_batches_processed.saturating_add(1);
                entry.total_overload_retries = entry
                    .total_overload_retries
                    .saturating_add(*overload_retries);
                entry.total_other_retries =
                    entry.total_other_retries.saturating_add(*other_retries);
                entry.total_rows_validated =
                    entry.total_rows_validated.saturating_add(*rows_validated);
                entry.total_matched_rows = entry.total_matched_rows.saturating_add(*matched_rows);
                entry.total_mismatched_rows =
                    entry.total_mismatched_rows.saturating_add(*mismatched_rows);
                entry.total_missing_keys = entry.total_missing_keys.saturating_add(*missing_keys);
                entry.total_bin_mismatch_rows = entry
                    .total_bin_mismatch_rows
                    .saturating_add(*bin_mismatch_rows);
                if entry.is_dq_validation {
                    entry.dq_passed = Some(entry.total_mismatched_rows == 0);
                }
                for sample in mismatch_samples {
                    if entry.dq_mismatch_samples.len() < 20 {
                        entry.dq_mismatch_samples.push(sample.clone());
                    }
                }
                let node_delta = if *is_dq_validation {
                    *rows_validated
                } else {
                    *rows_written
                };
                let node_rows = entry.per_node_rows.entry(*node_id).or_insert(0);
                *node_rows = node_rows.saturating_add(node_delta);
                entry.updated_at_ms = (*timestamp_ms).max(entry.updated_at_ms);
            }
            MetadataCommand::SetJobState {
                job_id,
                namespace,
                set_name,
                state,
                error_message,
                timestamp_ms,
            } => {
                let entry = self
                    .jobs
                    .entry(job_id.clone())
                    .or_insert_with(|| JobLoadStatus {
                        job_id: job_id.clone(),
                        namespace: namespace.clone(),
                        set_name: set_name.clone(),
                        is_dq_validation: false,
                        state: *state,
                        total_rows_written: 0,
                        total_batches_processed: 0,
                        total_overload_retries: 0,
                        total_other_retries: 0,
                        total_rows_validated: 0,
                        total_matched_rows: 0,
                        total_mismatched_rows: 0,
                        total_missing_keys: 0,
                        total_bin_mismatch_rows: 0,
                        dq_passed: None,
                        dq_mismatch_samples: Vec::new(),
                        per_node_rows: BTreeMap::new(),
                        error_message: None,
                        started_at_ms: *timestamp_ms,
                        updated_at_ms: *timestamp_ms,
                    });
                entry.state = *state;
                if error_message.is_some() {
                    entry.error_message.clone_from(error_message);
                }
                entry.updated_at_ms = (*timestamp_ms).max(entry.updated_at_ms);
            }
            MetadataCommand::PurgeExpiredJobs { cutoff_ms } => {
                self.jobs.retain(|_, job| job.updated_at_ms >= *cutoff_ms);
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MetadataCommand {
    UpsertNodeStatus(NodeStatusMeta),
    RecordJobBatch {
        job_id: String,
        node_id: u64,
        namespace: String,
        set_name: String,
        #[serde(default)]
        is_dq_validation: bool,
        rows_written: u64,
        overload_retries: u64,
        other_retries: u64,
        #[serde(default)]
        rows_validated: u64,
        #[serde(default)]
        matched_rows: u64,
        #[serde(default)]
        mismatched_rows: u64,
        #[serde(default)]
        missing_keys: u64,
        #[serde(default)]
        bin_mismatch_rows: u64,
        #[serde(default)]
        mismatch_samples: Vec<DqMismatchSample>,
        timestamp_ms: u64,
    },
    SetJobState {
        job_id: String,
        namespace: String,
        set_name: String,
        state: JobState,
        error_message: Option<String>,
        timestamp_ms: u64,
    },
    PurgeExpiredJobs {
        cutoff_ms: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub command: MetadataCommand,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSnapshot {
    pub node_id: u64,
    pub role: RaftRole,
    pub current_term: u64,
    pub leader_id: Option<u64>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub state: ClusterMetadataState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum RaftRpcRequest {
    RequestVote {
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
    },
    AppendEntries {
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    Propose {
        command: MetadataCommand,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum RaftRpcResponse {
    RequestVote {
        term: u64,
        vote_granted: bool,
    },
    AppendEntries {
        term: u64,
        success: bool,
        match_index: u64,
    },
    Propose {
        success: bool,
        leader_id: Option<u64>,
        error: Option<String>,
    },
}

struct RaftVolatileCore {
    current_term: u64,
    voted_for: Option<u64>,
    role: RaftRole,
    leader_id: Option<u64>,
    log: Vec<LogEntry>, // 1-indexed via LogEntry.index
    commit_index: u64,
    last_applied: u64,
    next_index: BTreeMap<u64, u64>,
    match_index: BTreeMap<u64, u64>,
    last_leader_contact_ms: u64,
}

impl RaftVolatileCore {
    fn new(single_node: bool, node_id: u64) -> Self {
        Self {
            current_term: if single_node { 1 } else { 0 },
            voted_for: if single_node { Some(node_id) } else { None },
            role: if single_node {
                RaftRole::Leader
            } else {
                RaftRole::Follower
            },
            leader_id: if single_node { Some(node_id) } else { None },
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            last_leader_contact_ms: now_ms(),
        }
    }

    fn last_log_index_and_term(&self) -> (u64, u64) {
        self.log
            .last()
            .map(|e| (e.index, e.term))
            .unwrap_or((0, 0))
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        self.log
            .get((index - 1) as usize)
            .map(|e| e.term)
    }
}

/// Embedded Raft consensus node for replicating cluster and Spark job metadata across equal proxy nodes.
pub struct RaftNode {
    config: RaftConfig,
    core: Mutex<RaftVolatileCore>,
    state_machine: RwLock<ClusterMetadataState>,
    shutdown_tx: watch::Sender<bool>,
}

impl RaftNode {
    pub fn new(config: RaftConfig) -> Arc<Self> {
        let has_remote_peers = config
            .peers
            .iter()
            .any(|p| p.node_id != config.node_id);
        let core = RaftVolatileCore::new(!has_remote_peers, config.node_id);
        let (shutdown_tx, _) = watch::channel(false);

        Arc::new(Self {
            config,
            core: Mutex::new(core),
            state_machine: RwLock::new(ClusterMetadataState::default()),
            shutdown_tx,
        })
    }

    pub fn node_id(&self) -> u64 {
        self.config.node_id
    }

    /// Start the Raft TCP server on `config.listen_addr` and spawn background election / replication tasks.
    /// Returns the bound `SocketAddr` (useful when binding to `:0` in tests).
    pub async fn start(self: &Arc<Self>) -> Result<SocketAddr> {
        let listener = TcpListener::bind(&self.config.listen_addr)
            .await
            .map_err(|e| {
                ProxyError::Config(format!(
                    "Failed to bind Raft listener on {}: {e}",
                    self.config.listen_addr
                ))
            })?;
        let bound_addr = listener
            .local_addr()
            .map_err(|e| ProxyError::Internal(format!("Failed to read local Raft addr: {e}")))?;

        info!(
            node_id = self.config.node_id,
            raft_addr = %bound_addr,
            peers = self.config.peers.len(),
            "Raft metadata server listening"
        );

        let node_for_accept = Arc::clone(self);
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    accept_res = listener.accept() => {
                        match accept_res {
                            Ok((stream, _)) => {
                                let node = Arc::clone(&node_for_accept);
                                tokio::spawn(async move {
                                    let _ = node.handle_connection(stream).await;
                                });
                            }
                            Err(e) => {
                                warn!("Raft listener accept error: {e}");
                            }
                        }
                    }
                }
            }
        });

        let has_remote_peers = self
            .config
            .peers
            .iter()
            .any(|p| p.node_id != self.config.node_id);

        if has_remote_peers {
            let node_for_ticker = Arc::clone(self);
            let mut ticker_shutdown = self.shutdown_tx.subscribe();
            tokio::spawn(async move {
                node_for_ticker.run_ticker_loop(&mut ticker_shutdown).await;
            });
        }

        if self.config.job_stats_ttl_secs > 0 {
            let node_for_ttl = Arc::clone(self);
            let mut ttl_shutdown = self.shutdown_tx.subscribe();
            let check_interval_ms = (self.config.job_stats_ttl_secs * 1000 / 2).clamp(200, 10_000);
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = ttl_shutdown.changed() => {
                            if *ttl_shutdown.borrow() {
                                break;
                            }
                        }
                        () = tokio::time::sleep(Duration::from_millis(check_interval_ms)) => {
                            let is_leader = {
                                let core = node_for_ttl.core.lock().await;
                                core.role == RaftRole::Leader
                            };
                            if is_leader {
                                let _ = node_for_ttl.purge_expired_jobs().await;
                            }
                        }
                    }
                }
            });
        }

        Ok(bound_addr)
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    fn job_stats_cutoff_ms(&self) -> Option<u64> {
        if self.config.job_stats_ttl_secs == 0 {
            None
        } else {
            Some(now_ms().saturating_sub(self.config.job_stats_ttl_secs.saturating_mul(1000)))
        }
    }

    /// Evict expired job statistics from the Raft state machine if any entries are older than `job_stats_ttl_secs`.
    pub async fn purge_expired_jobs(self: &Arc<Self>) -> Result<()> {
        let Some(cutoff_ms) = self.job_stats_cutoff_ms() else {
            return Ok(());
        };
        let has_expired = {
            let sm = self.state_machine.read().await;
            sm.jobs.values().any(|job| job.updated_at_ms < cutoff_ms)
        };
        if has_expired {
            self.propose(MetadataCommand::PurgeExpiredJobs { cutoff_ms })
                .await?;
        }
        Ok(())
    }

    /// Propose a metadata command to the Raft cluster.
    /// - If this node is the Leader, appends to log, replicates to peers, and applies once committed.
    /// - If this node is a Follower, forwards the command to the current Leader over the Raft port.
    pub async fn propose(self: &Arc<Self>, command: MetadataCommand) -> Result<()> {
        if !self.config.enabled {
            self.state_machine.write().await.apply(&command);
            return Ok(());
        }

        for _attempt in 0..8 {
            let (role, leader_id) = {
                let core = self.core.lock().await;
                (core.role, core.leader_id)
            };

            if role == RaftRole::Leader {
                return self.append_and_replicate_as_leader(command).await;
            }

            if let Some(lid) = leader_id {
                if let Some(peer) = self.config.peers.iter().find(|p| p.node_id == lid) {
                    let req = RaftRpcRequest::Propose {
                        command: command.clone(),
                    };
                    if let Ok(RaftRpcResponse::Propose {
                        success: true, ..
                    }) = send_rpc(&peer.raft_addr, &req, 1500).await
                    {
                        return Ok(());
                    }
                }
            }

            // If no leader is known yet or forwarding failed during a leader transition, wait briefly
            tokio::time::sleep(Duration::from_millis(60)).await;
        }

        // Fallback: if quorum is temporarily unreachable, still apply locally so local queries work
        warn!(
            node_id = self.config.node_id,
            "Raft leader unreachable after retries; applying metadata update locally"
        );
        self.state_machine.write().await.apply(&command);
        Ok(())
    }

    pub async fn get_job_status(&self, job_id: &str) -> Option<JobLoadStatus> {
        let cutoff_ms = self.job_stats_cutoff_ms().unwrap_or(0);
        self.state_machine
            .read()
            .await
            .jobs
            .get(job_id)
            .filter(|job| job.updated_at_ms >= cutoff_ms)
            .cloned()
    }

    pub async fn get_cluster_snapshot(&self) -> ClusterSnapshot {
        let (role, current_term, leader_id, commit_index, last_applied) = {
            let core = self.core.lock().await;
            (
                core.role,
                core.current_term,
                core.leader_id,
                core.commit_index,
                core.last_applied,
            )
        };
        let mut state = self.state_machine.read().await.clone();
        if let Some(cutoff_ms) = self.job_stats_cutoff_ms() {
            state.jobs.retain(|_, job| job.updated_at_ms >= cutoff_ms);
        }
        ClusterSnapshot {
            node_id: self.config.node_id,
            role,
            current_term,
            leader_id,
            commit_index,
            last_applied,
            state,
        }
    }

    async fn append_and_replicate_as_leader(
        self: &Arc<Self>,
        command: MetadataCommand,
    ) -> Result<()> {
        let remote_peers: Vec<_> = self
            .config
            .peers
            .iter()
            .filter(|p| p.node_id != self.config.node_id)
            .cloned()
            .collect();

        let entry_index = {
            let mut core = self.core.lock().await;
            if core.role != RaftRole::Leader {
                return Err(ProxyError::Internal("Node stepped down from Leader".into()));
            }
            let next_idx = core.log.len() as u64 + 1;
            let term = core.current_term;
            core.log.push(LogEntry {
                index: next_idx,
                term,
                command,
            });

            if remote_peers.is_empty() {
                core.commit_index = next_idx;
            }
            next_idx
        };

        if remote_peers.is_empty() {
            self.apply_committed_entries().await;
            return Ok(());
        }

        self.replicate_to_peers().await;
        self.apply_committed_entries().await;

        let committed = self.core.lock().await.commit_index >= entry_index;
        if committed {
            // Propagate the updated leader_commit index to followers so follower reads are immediately consistent
            self.replicate_to_peers().await;
        } else {
            debug!(
                node_id = self.config.node_id,
                entry_index, "Entry appended; waiting for async heartbeat replication"
            );
        }
        Ok(())
    }

    async fn run_ticker_loop(self: &Arc<Self>, shutdown_rx: &mut watch::Receiver<bool>) {
        let hb_interval = Duration::from_millis(self.config.heartbeat_interval_ms.max(20));

        loop {
            let sleep_dur = {
                let core = self.core.lock().await;
                if core.role == RaftRole::Leader {
                    hb_interval
                } else {
                    let min_ms = self.config.election_timeout_min_ms.max(80);
                    let max_ms = self.config.election_timeout_max_ms.max(min_ms + 40);
                    let timeout_ms = rand::rng().random_range(min_ms..=max_ms);
                    Duration::from_millis(timeout_ms)
                }
            };

            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
                () = tokio::time::sleep(sleep_dur) => {
                    let is_leader = {
                        let core = self.core.lock().await;
                        core.role == RaftRole::Leader
                    };

                    if is_leader {
                        self.replicate_to_peers().await;
                        self.apply_committed_entries().await;
                    } else {
                        let should_elect = {
                            let core = self.core.lock().await;
                            let elapsed = now_ms().saturating_sub(core.last_leader_contact_ms);
                            elapsed >= self.config.election_timeout_min_ms
                        };
                        if should_elect {
                            self.start_election().await;
                        }
                    }
                }
            }
        }
    }

    async fn start_election(self: &Arc<Self>) {
        let (term, last_log_index, last_log_term) = {
            let mut core = self.core.lock().await;
            core.role = RaftRole::Candidate;
            core.current_term += 1;
            core.voted_for = Some(self.config.node_id);
            core.leader_id = None;
            core.last_leader_contact_ms = now_ms();
            let (lli, llt) = core.last_log_index_and_term();
            (core.current_term, lli, llt)
        };

        let remote_peers: Vec<_> = self
            .config
            .peers
            .iter()
            .filter(|p| p.node_id != self.config.node_id)
            .cloned()
            .collect();

        let total_nodes = remote_peers.len() + 1;
        let majority = (total_nodes / 2) + 1;
        let mut votes_granted = 1usize; // voted for self

        let mut futs = Vec::with_capacity(remote_peers.len());
        for peer in remote_peers {
            let req = RaftRpcRequest::RequestVote {
                term,
                candidate_id: self.config.node_id,
                last_log_index,
                last_log_term,
            };
            futs.push(async move { send_rpc(&peer.raft_addr, &req, 200).await });
        }

        let responses = futures::future::join_all(futs).await;
        let mut max_seen_term = term;

        for resp in responses.into_iter().flatten() {
            if let RaftRpcResponse::RequestVote {
                term: peer_term,
                vote_granted,
            } = resp
            {
                if peer_term > max_seen_term {
                    max_seen_term = peer_term;
                }
                if vote_granted && peer_term == term {
                    votes_granted += 1;
                }
            }
        }

        let became_leader = {
            let mut core = self.core.lock().await;
            if max_seen_term > core.current_term {
                core.current_term = max_seen_term;
                core.role = RaftRole::Follower;
                core.voted_for = None;
                false
            } else if core.role == RaftRole::Candidate
                && core.current_term == term
                && votes_granted >= majority
            {
                core.role = RaftRole::Leader;
                core.leader_id = Some(self.config.node_id);
                let next_idx = core.log.len() as u64 + 1;
                for peer in &self.config.peers {
                    if peer.node_id != self.config.node_id {
                        core.next_index.insert(peer.node_id, next_idx);
                        core.match_index.insert(peer.node_id, 0);
                    }
                }
                info!(
                    node_id = self.config.node_id,
                    term,
                    votes_granted,
                    total_nodes,
                    "Node elected Raft Leader"
                );
                true
            } else {
                false
            }
        };

        if became_leader {
            self.replicate_to_peers().await;
            self.apply_committed_entries().await;
        }
    }

    async fn replicate_to_peers(self: &Arc<Self>) {
        let remote_peers: Vec<_> = self
            .config
            .peers
            .iter()
            .filter(|p| p.node_id != self.config.node_id)
            .cloned()
            .collect();

        if remote_peers.is_empty() {
            return;
        }

        let mut rpc_tasks = Vec::with_capacity(remote_peers.len());
        {
            let core = self.core.lock().await;
            if core.role != RaftRole::Leader {
                return;
            }
            let term = core.current_term;
            let leader_id = self.config.node_id;
            let leader_commit = core.commit_index;

            for peer in remote_peers {
                let next_idx = core
                    .next_index
                    .get(&peer.node_id)
                    .copied()
                    .unwrap_or(1)
                    .max(1);
                let prev_log_index = next_idx - 1;
                let prev_log_term = core.term_at(prev_log_index).unwrap_or(0);
                let entries: Vec<LogEntry> = if (next_idx as usize) <= core.log.len() {
                    core.log[(next_idx as usize - 1)..].to_vec()
                } else {
                    Vec::new()
                };

                let req = RaftRpcRequest::AppendEntries {
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                };

                rpc_tasks.push(async move {
                    let res = send_rpc(&peer.raft_addr, &req, 250).await;
                    (peer.node_id, next_idx, res)
                });
            }
        }

        let results = futures::future::join_all(rpc_tasks).await;
        let mut core = self.core.lock().await;
        if core.role != RaftRole::Leader {
            return;
        }

        for (peer_id, attempted_next_idx, res) in results {
            if let Ok(RaftRpcResponse::AppendEntries {
                term: peer_term,
                success,
                match_index,
            }) = res
            {
                if peer_term > core.current_term {
                    core.current_term = peer_term;
                    core.role = RaftRole::Follower;
                    core.voted_for = None;
                    core.leader_id = None;
                    return;
                }
                if success {
                    core.match_index.insert(peer_id, match_index);
                    core.next_index.insert(peer_id, match_index + 1);
                } else {
                    let decremented = attempted_next_idx.saturating_sub(1).max(1);
                    core.next_index.insert(peer_id, decremented);
                }
            }
        }

        // Advance commit_index if a majority of cluster nodes have replicated up to N
        let total_nodes = self
            .config
            .peers
            .iter()
            .filter(|p| p.node_id != self.config.node_id)
            .count()
            + 1;
        let majority = (total_nodes / 2) + 1;
        let max_log_idx = core.log.len() as u64;

        for n in ((core.commit_index + 1)..=max_log_idx).rev() {
            if core.term_at(n) == Some(core.current_term) {
                let mut replicated_count = 1usize; // self
                for &m_idx in core.match_index.values() {
                    if m_idx >= n {
                        replicated_count += 1;
                    }
                }
                if replicated_count >= majority {
                    core.commit_index = n;
                    break;
                }
            }
        }
    }

    async fn apply_committed_entries(&self) {
        let entries_to_apply: Vec<MetadataCommand> = {
            let mut core = self.core.lock().await;
            if core.commit_index <= core.last_applied {
                return;
            }
            let start = core.last_applied as usize;
            let end = (core.commit_index as usize).min(core.log.len());
            let cmds = core.log[start..end]
                .iter()
                .map(|e| e.command.clone())
                .collect();
            core.last_applied = end as u64;
            cmds
        };

        if !entries_to_apply.is_empty() {
            let mut sm = self.state_machine.write().await;
            for cmd in &entries_to_apply {
                sm.apply(cmd);
            }
        }
    }

    async fn handle_connection(self: &Arc<Self>, mut stream: TcpStream) -> Result<()> {
        let len = match stream.read_u32().await {
            Ok(l) => l as usize,
            Err(_) => return Ok(()),
        };
        if len == 0 || len > 16 * 1024 * 1024 {
            return Ok(());
        }

        let mut buf = vec![0u8; len];
        stream
            .read_exact(&mut buf)
            .await
            .map_err(|e| ProxyError::Internal(format!("Raft read error: {e}")))?;

        let req: RaftRpcRequest = serde_json::from_slice(&buf)
            .map_err(|e| ProxyError::Internal(format!("Raft decode error: {e}")))?;

        let resp = self.process_rpc(req).await;
        let resp_bytes = serde_json::to_vec(&resp)
            .map_err(|e| ProxyError::Internal(format!("Raft encode error: {e}")))?;

        stream
            .write_u32(resp_bytes.len() as u32)
            .await
            .map_err(|e| ProxyError::Internal(format!("Raft write len error: {e}")))?;
        stream
            .write_all(&resp_bytes)
            .await
            .map_err(|e| ProxyError::Internal(format!("Raft write body error: {e}")))?;

        Ok(())
    }

    async fn process_rpc(self: &Arc<Self>, req: RaftRpcRequest) -> RaftRpcResponse {
        match req {
            RaftRpcRequest::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                let mut core = self.core.lock().await;
                if term > core.current_term {
                    core.current_term = term;
                    core.role = RaftRole::Follower;
                    core.voted_for = None;
                    core.leader_id = None;
                }

                let (my_last_idx, my_last_term) = core.last_log_index_and_term();
                let log_ok = (last_log_term > my_last_term)
                    || (last_log_term == my_last_term && last_log_index >= my_last_idx);

                let vote_ok =
                    core.voted_for.is_none() || core.voted_for == Some(candidate_id);

                if term == core.current_term && log_ok && vote_ok {
                    core.voted_for = Some(candidate_id);
                    core.last_leader_contact_ms = now_ms();
                    RaftRpcResponse::RequestVote {
                        term: core.current_term,
                        vote_granted: true,
                    }
                } else {
                    RaftRpcResponse::RequestVote {
                        term: core.current_term,
                        vote_granted: false,
                    }
                }
            }

            RaftRpcRequest::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                let resp = {
                    let mut core = self.core.lock().await;
                    if term < core.current_term {
                        return RaftRpcResponse::AppendEntries {
                            term: core.current_term,
                            success: false,
                            match_index: 0,
                        };
                    }

                    if term > core.current_term || core.role != RaftRole::Follower {
                        core.current_term = term;
                        core.role = RaftRole::Follower;
                        core.voted_for = None;
                    }
                    core.leader_id = Some(leader_id);
                    core.last_leader_contact_ms = now_ms();

                    if prev_log_index > 0 {
                        match core.term_at(prev_log_index) {
                            Some(t) if t == prev_log_term => {}
                            _ => {
                                return RaftRpcResponse::AppendEntries {
                                    term: core.current_term,
                                    success: false,
                                    match_index: core.log.len() as u64,
                                };
                            }
                        }
                    }

                    for entry in entries {
                        let idx = entry.index as usize;
                        if idx <= core.log.len() {
                            if core.log[idx - 1].term != entry.term {
                                core.log.truncate(idx - 1);
                                core.log.push(entry);
                            }
                        } else {
                            core.log.push(entry);
                        }
                    }

                    if leader_commit > core.commit_index {
                        core.commit_index = leader_commit.min(core.log.len() as u64);
                    }

                    RaftRpcResponse::AppendEntries {
                        term: core.current_term,
                        success: true,
                        match_index: core.log.len() as u64,
                    }
                };

                self.apply_committed_entries().await;
                resp
            }

            RaftRpcRequest::Propose { command } => {
                let (role, leader_id) = {
                    let core = self.core.lock().await;
                    (core.role, core.leader_id)
                };
                if role == RaftRole::Leader {
                    match self.append_and_replicate_as_leader(command).await {
                        Ok(()) => RaftRpcResponse::Propose {
                            success: true,
                            leader_id: Some(self.config.node_id),
                            error: None,
                        },
                        Err(e) => RaftRpcResponse::Propose {
                            success: false,
                            leader_id,
                            error: Some(e.to_string()),
                        },
                    }
                } else {
                    RaftRpcResponse::Propose {
                        success: false,
                        leader_id,
                        error: Some("Not Leader".to_string()),
                    }
                }
            }
        }
    }
}

async fn send_rpc(
    addr: &str,
    req: &RaftRpcRequest,
    timeout_ms: u64,
) -> std::result::Result<RaftRpcResponse, String> {
    let payload = serde_json::to_vec(req).map_err(|e| e.to_string())?;
    let fut = async {
        let mut stream = TcpStream::connect(addr)
            .await
            .map_err(|e| format!("connect {addr}: {e}"))?;
        stream
            .write_u32(payload.len() as u32)
            .await
            .map_err(|e| e.to_string())?;
        stream
            .write_all(&payload)
            .await
            .map_err(|e| e.to_string())?;

        let resp_len = stream.read_u32().await.map_err(|e| e.to_string())? as usize;
        let mut resp_buf = vec![0u8; resp_len];
        stream
            .read_exact(&mut resp_buf)
            .await
            .map_err(|e| e.to_string())?;
        serde_json::from_slice::<RaftRpcResponse>(&resp_buf).map_err(|e| e.to_string())
    };

    tokio::time::timeout(Duration::from_millis(timeout_ms), fut)
        .await
        .map_err(|_| format!("RPC timeout to {addr}"))?
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
