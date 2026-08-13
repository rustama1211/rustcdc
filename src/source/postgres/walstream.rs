//! [`PgOutputMessageProvider`] over the [`pg_walstream`] crate.
//!
//! An alternative to [`super::streaming`], which does the same job over rustcdc's own
//! [`wire`](super::wire) client. Both speak `START_REPLICATION ... LOGICAL`; they differ
//! only in whose socket, TLS and framing code carries it.
//!
//! # Why the decoder is still rustcdc's
//!
//! `pg_walstream` has a full pgoutput parser, and this module deliberately does not use
//! it. It reads through [`LogicalReplicationStream::next_raw_event`], which hands back the
//! undecoded pgoutput payload, and passes those bytes to the same decoder the `wire`
//! transport feeds. Two reasons, and the second is the important one:
//!
//! * Decoding twice would be strictly slower — `pg_walstream` would parse into its types
//!   and this module would immediately re-shape them into rustcdc's.
//! * A benchmark between the two transports is only meaningful if everything downstream
//!   is identical. Sharing the decoder makes the difference between them *be* the
//!   transport, rather than the transport plus two parsers' worth of differing opinions
//!   about what a message means.
//!
//! The same argument the `streaming` module makes about byte-for-byte fidelity therefore
//! applies here unchanged.
//!
//! # Cancellation, and why the budget is a `tokio::time::timeout`
//!
//! [`PgOutputMessageProvider::poll_xlog_data`] must return within its budget.
//! `pg_walstream` documents *"cancel via `cancellation_token` rather than by dropping
//! this future"*, and this module originally followed that literally: a child
//! [`CancellationToken`] per blocking wait, cancelled by a timer task.
//!
//! **That deadlocks the stream, and it is not a rare race.** Measured on a cold start
//! (snapshot and stream running together, so the stream times out on nearly every poll),
//! three runs in four wedged: one `Backend worker error: already streaming`, then no
//! further progress, with the process still holding its slot until killed.
//!
//! The mechanism is in the crate's threaded driver. On cancellation the *caller* side
//! returns immediately and clears its `batch_rx` — commented there as "stream is ending"
//! — while the worker is still inside `stream_copy` awaiting the socket, unaware. The
//! next poll therefore sees `batch_rx.is_none()`, sends a second `Command::StreamCopy`,
//! and the still-running worker rejects it as `already streaming`. The crate's
//! cancellation contract assumes a cancel is *terminal*; a per-poll timeout is not.
//!
//! So the budget is a [`tokio::time::timeout`] and the token is never cancelled while
//! polling. Dropping the read future is safe on both drivers, and for a stronger reason
//! than the earlier note gave: every piece of state a partial read could have advanced
//! lives on the connection rather than in the future.
//!
//! * Threaded: the only await is a `select!` over `batch_rx.recv()`, which is
//!   cancel-safe, and the batch it would have produced is still queued. `pending` and
//!   `batch_rx` are untouched, so nothing re-sends `StreamCopy` — the stream is never
//!   torn down at all, which is why this also fixes the wedge rather than hiding it.
//! * Inline: the await is an `AsyncReadExt::read_buf` into the connection's own
//!   `read_buf`, so a dropped poll loses no bytes.
//!
//! Between receiving a batch and storing it there is no await at all (`*pending = batch`
//! then `pop_front`), so a timeout cannot land in the middle and lose one.
//!
//! # What this transport does not do
//!
//! * **Protocol versions above 1.** `pg_walstream` implements v1–v4 and this negotiates
//!   v1, because rustcdc's decoder understands v1. Raising it here without teaching the
//!   decoder `Stream*` / `*Prepared` would turn real messages into
//!   [`PgOutputMessage::Unknown`](super::decoder).
//! * **mTLS.** `pg_walstream` parses `sslmode` and `sslrootcert` from the connection
//!   string and has no client-certificate equivalent, so a config carrying one is
//!   rejected rather than quietly downgraded to server-auth-only TLS.
//! * **Injected `rustls::ClientConfig`.** Same reason: the connection string is the only
//!   way in, and a `ClientConfig` cannot be spelled in one.

use std::time::Duration;

use async_trait::async_trait;
use pg_walstream::{
    LogicalReplicationStream, ReplicationError, ReplicationStreamConfig, StreamingMode,
};
use tokio_util::sync::CancellationToken;

use crate::core::{Error, Result, TransportConfig};

use super::decoder::{PgOutputMessageProvider, PgOutputXLogData, PollOutcome};

/// How long the drain phase waits for an *already-buffered* message.
///
/// `pg_walstream` exposes no non-blocking read: the only way to ask for a message is to
/// await one, and a pre-cancelled token returns `Cancelled` without consulting the buffer
/// at all. So "take what is already there" has to be spelled as a very short wait.
///
/// The cost is bounded and paid once per batch, not once per message: the drain loop stops
/// on the first timeout, so a batch that ends after `n` messages spends this budget once.
/// It is deliberately far below any realistic `poll_timeout` so that a full batch of
/// buffered messages is still assembled without a syscall each, which is the whole point
/// of batching. `wire` gets this for free by passing `Duration::ZERO` to its own `recv`.
const DRAIN_BUDGET: Duration = Duration::from_millis(1);

/// pgoutput protocol version negotiated with the server.
///
/// One, matching what [`super::decoder`] implements — see the module docs. This is a
/// constant rather than configuration precisely so that raising it is a change to the
/// decoder, not a knob an operator can turn into `Unknown` messages.
const PROTOCOL_VERSION: u32 = 1;

/// Reads the WAL stream over `pg_walstream`'s replication client.
pub(super) struct WalstreamPgOutputProvider {
    stream: LogicalReplicationStream,
    slot_name: String,
    /// Parent token for every per-poll child. Cancelling it aborts an in-flight read.
    cancel: CancellationToken,
    /// Highest LSN handed to [`confirm_lsn`](Self::confirm_lsn).
    ///
    /// Tracked here rather than read back from `shared_lsn_feedback` because that is a
    /// *feedback* value: it is what will be sent to the server, and it is clamped against
    /// `last_received_lsn` on the way out. Lag arithmetic needs what the pipeline has
    /// actually acknowledged, which is this.
    applied_lsn: u64,
}

impl WalstreamPgOutputProvider {
    /// Open a replication stream, resuming from `start_lsn`.
    ///
    /// A `start_lsn` of zero is passed through as `None`, which asks the server to resume
    /// from the slot's own `confirmed_flush_lsn` — the same meaning zero carries in
    /// [`super::streaming::StreamingPgOutputProvider::connect`], so a stream with no
    /// checkpoint begins in the same place on either transport.
    pub(super) async fn connect(
        config: &super::PostgresSourceConfig,
        start_lsn: u64,
    ) -> Result<Self> {
        // `resolve`, not `expose_secret`, for the same reason the `wire` transport does it:
        // a deferred secret must be fetched on every reconnect so AWS IAM database auth
        // gets a freshly minted token rather than an expired one.
        let password = config.password.resolve()?;
        let conninfo = build_conninfo(config, &password)?;

        let stream_config = ReplicationStreamConfig::builder(
            config.replication_slot_name.clone(),
            config.publication_name.clone(),
        )
        .with_protocol_version(PROTOCOL_VERSION)
        .with_connection_timeout(Duration::from_secs(config.conn_timeout_secs))
        // Streaming of in-progress transactions is a protocol v2 feature. Asking for it
        // while negotiating v1 would be incoherent, and the server would ignore it.
        .with_streaming_mode(StreamingMode::Off);

        let mut stream = LogicalReplicationStream::new(&conninfo, stream_config)
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "pg_walstream failed to open a replication connection to \
                     {}:{}: {error}",
                    config.host, config.port
                ))
            })?;

        // `start` runs `ensure_replication_slot` first, which is a no-op when the slot
        // exists. rustcdc has already created it by this point (see
        // `create_replication_slot_if_missing`), so this neither duplicates that work nor
        // fights it.
        stream
            .start(if start_lsn == 0 {
                None
            } else {
                Some(start_lsn)
            })
            .await
            .map_err(|error| {
                Error::SourceError(format!(
                    "pg_walstream failed to start replication on slot '{}': {error}",
                    config.replication_slot_name
                ))
            })?;

        Ok(Self {
            stream,
            slot_name: config.replication_slot_name.clone(),
            cancel: CancellationToken::new(),
            applied_lsn: start_lsn,
        })
    }

    /// Await one raw XLogData, giving up after `budget`.
    ///
    /// Returns `Ok(None)` on timeout, which is the caller's signal to stop batching.
    ///
    /// The budget is a `timeout` and `self.cancel` is deliberately never cancelled here.
    /// Cancelling it per poll is what wedged the stream with `already streaming` — see
    /// the module docs. The token still exists because the API takes one, and because
    /// `Drop` uses it to stop the worker when the provider really is going away.
    async fn next_within(&mut self, budget: Duration) -> Result<Option<PgOutputXLogData>> {
        // Cloned so the token borrow does not collide with the `&mut self.stream` one.
        let token = self.cancel.clone();

        let result = match tokio::time::timeout(budget, self.stream.next_raw_event(&token)).await {
            // Budget expired. The stream is left exactly as it was: still streaming,
            // still buffering, nothing to restart.
            Err(_elapsed) => return Ok(None),
            Ok(result) => result,
        };

        match result {
            Ok(raw) => Ok(Some(PgOutputXLogData {
                lsn: raw.wal_start.0,
                // The one copy on this path. `raw.data` is a zero-copy slice of the
                // connection's read buffer and is only valid until the next call, whereas
                // `PgOutputXLogData` owns its bytes — as it must, because a batch outlives
                // the reads that produced it. `wire` allocates a `Vec` here too, so this
                // costs the comparison nothing.
                data: raw.data.to_vec(),
            })),
            // Only reachable if something cancels `self.cancel`, which now happens solely
            // on `Drop`. Treated as "no data" rather than an error for that reason.
            Err(ReplicationError::Cancelled(_)) => Ok(None),
            Err(error) => Err(Error::SourceError(format!(
                "pg_walstream read failed on slot '{}': {error}",
                self.slot_name
            ))),
        }
    }
}

#[async_trait]
impl PgOutputMessageProvider for WalstreamPgOutputProvider {
    async fn poll_xlog_data(
        &mut self,
        max_messages: usize,
        poll_timeout: Duration,
    ) -> Result<PollOutcome> {
        let deadline = tokio::time::Instant::now() + poll_timeout;
        let mut messages = Vec::new();

        while messages.len() < max_messages.max(1) {
            // Same shape as the `wire` transport: block the real budget for the *first*
            // record, then take only what is already buffered. Waiting the full budget
            // again once data has arrived would make every record wait for the last one.
            let budget = if messages.is_empty() {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                remaining
            } else {
                DRAIN_BUDGET
            };

            match self.next_within(budget).await? {
                Some(message) => messages.push(message),
                // Budget spent. On an empty batch this means the slot is caught up, not
                // that there is backlog pressure — so it is `Data`, never `TimedOut`.
                None => break,
            }
        }

        Ok(PollOutcome::Data(messages))
    }

    fn waits_for_data(&self) -> bool {
        // Blocks on the socket, exactly like the `wire` transport. See the trait docs for
        // why conflating this with the SQL-peek meaning costs either latency or throughput.
        true
    }

    async fn confirm_lsn(&mut self, lsn: u64) -> Result<()> {
        if lsn <= self.applied_lsn {
            return Ok(());
        }
        self.applied_lsn = lsn;

        // Both, not just applied: `confirmed_flush_lsn` — the value that actually releases
        // WAL — is driven by the flushed position in the standby status update. Advancing
        // only `applied` would report progress the server does not act on, and the slot
        // would pin WAL forever.
        self.stream.shared_lsn_feedback.update_flushed_lsn(lsn);
        self.stream.shared_lsn_feedback.update_applied_lsn(lsn);

        // Sent now rather than left to the crate's own feedback interval, matching the
        // `wire` transport: a process exiting right after a commit would otherwise never
        // report it, and the cost is one 34-byte write per committed batch.
        self.stream.send_feedback().await.map_err(|error| {
            Error::SourceError(format!(
                "pg_walstream failed to send standby status update on slot '{}': {error}",
                self.slot_name
            ))
        })
    }

    async fn measure_slot_lag(&mut self) -> Result<Option<u64>> {
        // Free, for the same reason it is free on the `wire` transport: the server
        // volunteers its write position on every keepalive and XLogData header.
        // `pg_walstream` consumes keepalives internally rather than surfacing them, but it
        // records the position on the way past, and `state` is public.
        Ok(Some(
            self.stream
                .state
                .last_received_lsn
                .saturating_sub(self.applied_lsn),
        ))
    }

    async fn idle_advance(&mut self) -> Result<u64> {
        // Called only when nothing has been delivered for the idle interval, so there is
        // no unacknowledged work and the server's own write position is safe to confirm.
        // Without this a slot on a quiet database pins WAL forever.
        let server_wal_end = self.stream.state.last_received_lsn;
        let lag = server_wal_end.saturating_sub(self.applied_lsn);

        if server_wal_end > self.applied_lsn {
            self.confirm_lsn(server_wal_end).await?;
            tracing::debug!(
                target: "rustcdc::source::postgres",
                slot = %self.slot_name,
                lsn = server_wal_end,
                lag_bytes = lag,
                "postgres replication slot advanced during idle period",
            );
        }

        Ok(lag)
    }
}

impl Drop for WalstreamPgOutputProvider {
    fn drop(&mut self) {
        // Releases any read still parked on the socket. Without this a provider dropped
        // mid-poll leaves the spawned timer as the only thing holding the child token, and
        // the read waits out a budget nobody is going to collect.
        self.cancel.cancel();
    }
}

/// Render a libpq keyword/value connection string for `pg_walstream`.
///
/// Keyword form rather than a URI because the password is passed verbatim in a quoted
/// value, which avoids percent-encoding it into a URI — one more encoding step is one more
/// place for a `@` or `/` in a generated credential to change what the string means.
fn build_conninfo(config: &super::PostgresSourceConfig, password: &str) -> Result<String> {
    let sslmode = match &config.transport {
        TransportConfig::Plaintext => "disable",
        TransportConfig::Tls {
            client_cert_path,
            client_key_path,
            allow_invalid_certificates,
            allow_invalid_hostnames,
            ..
        } => {
            if client_cert_path.is_some() || client_key_path.is_some() {
                return Err(Error::ConfigError(
                    "WalTransport::PgWalstream cannot present a client certificate: \
                     pg_walstream's connection string has no sslcert/sslkey equivalent. \
                     Use WalTransport::StreamingReplication, which supports mTLS."
                        .into(),
                ));
            }
            // `verify-full` is the only mode that checks both chain and hostname, and the
            // relaxations are rejected rather than mapped: `sslmode=require` would silently
            // stop verifying the chain, which is a weaker guarantee than the caller asked
            // for by a wider margin than the flag name suggests.
            if *allow_invalid_certificates || *allow_invalid_hostnames {
                return Err(Error::ConfigError(
                    "WalTransport::PgWalstream does not support allow_invalid_certificates \
                     or allow_invalid_hostnames. Use WalTransport::StreamingReplication."
                        .into(),
                ));
            }
            "verify-full"
        }
        #[cfg(feature = "tls")]
        TransportConfig::RustlsConfig { .. } => {
            return Err(Error::ConfigError(
                "WalTransport::PgWalstream cannot use an injected rustls::ClientConfig: \
                 pg_walstream is configured through a connection string, which cannot \
                 carry one. Use WalTransport::StreamingReplication."
                    .into(),
            ));
        }
    };

    let ca_cert_path = match &config.transport {
        TransportConfig::Tls { ca_cert_path, .. } => ca_cert_path.as_deref(),
        _ => None,
    };

    // `replication=database` is what puts the backend in logical-replication mode; without
    // it START_REPLICATION is rejected as unknown SQL.
    let mut conninfo = format!(
        "host={} port={} user={} password={} dbname={} replication=database sslmode={}",
        quote_conninfo_value(&config.host),
        config.port,
        quote_conninfo_value(&config.user),
        quote_conninfo_value(password),
        quote_conninfo_value(&config.database),
        sslmode,
    );
    if let Some(path) = ca_cert_path {
        conninfo.push_str(&format!(" sslrootcert={}", quote_conninfo_value(path)));
    }

    Ok(conninfo)
}

/// Quote a libpq connection-string value.
///
/// Values are always wrapped, not conditionally: an empty value has to be `''` to parse at
/// all, and deciding per value whether quoting is needed is how a password containing a
/// space becomes a truncated one.
fn quote_conninfo_value(value: &str) -> String {
    let escaped = value.replace('\\', r"\\").replace('\'', r"\'");
    format!("'{escaped}'")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_and_escapes_conninfo_values() {
        assert_eq!(quote_conninfo_value("plain"), "'plain'");
        assert_eq!(quote_conninfo_value(""), "''");
        // A password of `it's` must not terminate the value at the apostrophe.
        assert_eq!(quote_conninfo_value("it's"), r"'it\'s'");
        assert_eq!(quote_conninfo_value(r"back\slash"), r"'back\\slash'");
        // Backslash first, so an escaped quote does not get its backslash re-escaped.
        assert_eq!(quote_conninfo_value(r"both\'"), r"'both\\\''");
    }

    fn config_with(transport: TransportConfig) -> super::super::PostgresSourceConfig {
        super::super::PostgresSourceConfig {
            transport,
            ..Default::default()
        }
    }

    #[test]
    fn plaintext_disables_ssl_and_requests_replication_mode() {
        let config = config_with(TransportConfig::Plaintext);
        let conninfo = build_conninfo(&config, "pw").unwrap();
        assert!(conninfo.contains("sslmode=disable"), "{conninfo}");
        assert!(conninfo.contains("replication=database"), "{conninfo}");
        assert!(conninfo.contains("password='pw'"), "{conninfo}");
    }

    #[test]
    fn tls_verifies_fully_and_passes_the_ca_bundle() {
        let config = config_with(TransportConfig::Tls {
            ca_cert_path: Some("/etc/ssl/rds.pem".into()),
            client_cert_path: None,
            client_key_path: None,
            allow_invalid_certificates: false,
            allow_invalid_hostnames: false,
        });
        let conninfo = build_conninfo(&config, "pw").unwrap();
        assert!(conninfo.contains("sslmode=verify-full"), "{conninfo}");
        assert!(
            conninfo.contains("sslrootcert='/etc/ssl/rds.pem'"),
            "{conninfo}"
        );
    }

    #[test]
    fn mtls_is_refused_rather_than_silently_downgraded() {
        let config = config_with(TransportConfig::Tls {
            ca_cert_path: None,
            client_cert_path: Some("/c.pem".into()),
            client_key_path: Some("/k.pem".into()),
            allow_invalid_certificates: false,
            allow_invalid_hostnames: false,
        });
        let error = build_conninfo(&config, "pw").unwrap_err().to_string();
        assert!(error.contains("client certificate"), "{error}");
    }

    #[test]
    fn certificate_relaxations_are_refused() {
        let config = config_with(TransportConfig::Tls {
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            allow_invalid_certificates: true,
            allow_invalid_hostnames: false,
        });
        assert!(build_conninfo(&config, "pw").is_err());
    }
}
