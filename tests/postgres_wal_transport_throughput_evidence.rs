//! Head-to-head capture throughput: rustcdc's own `wire` client vs the `pg_walstream` crate.
//!
//! Both transports run `START_REPLICATION ... LOGICAL` and both hand their undecoded
//! pgoutput bytes to the same rustcdc decoder (see `source::postgres::walstream` for why
//! the decoder is shared). Everything downstream of the socket is therefore identical, and
//! the difference between the two columns is the replication client and nothing else.
//!
//! # Why this harness exists
//!
//! `pg_walstream` publishes a headline figure of ~177 000 DML events/sec. That number is
//! measured reading WAL and discarding it. It is not comparable to a connector's
//! throughput, and quoting it as one is the mistake this harness exists to prevent.
//!
//! Two things are worth holding in view while reading the output:
//!
//! * **`cargo bench --bench throughput` measures rustcdc's runtime ceiling** — poll through
//!   transform, sink, ack and checkpoint, with a synthetic source and no database. On the
//!   machine this was developed on that ceiling was ~620 000 events/sec with an in-memory
//!   checkpoint and ~350 000–480 000 with an fsync-ing file checkpoint. A transport cannot
//!   make the pipeline faster than that.
//! * **A capture rate well below both numbers is bound somewhere else** — the sink, the
//!   commit batch size, or the poll configuration. Swapping the transport will not move it.
//!   Measure before migrating.
//!
//! # What is measured
//!
//! Wall time from the first poll to the last of `ROWS` inserts being captured and
//! confirmed, on a freshly created slot per transport, against a container on this
//! machine. The rows are written **before** the measured loop begins so both transports
//! read a backlog that is already durable — otherwise the figure measures how fast
//! PostgreSQL can be written to, which is the same for both and swamps the difference.
//!
//! # What is not measured
//!
//! Anything about a real deployment: network latency, TLS, server load, row width beyond
//! the one shape used here, or the protocol v2–v4 message types `pg_walstream` supports
//! and rustcdc's decoder does not yet read. Absolute numbers are hardware-dependent.
//!
//! Like the backlog harness beside it this is **evidence, not a gate**: it prints a table
//! and asserts only that both transports capture every row. A performance assertion that
//! fails on a loaded CI box teaches people to ignore failures.

#![cfg(all(feature = "postgres", feature = "pg-walstream"))]

use std::time::{Duration, Instant};

use rustcdc::{source::Source, Operation, PostgresConnection, PostgresSourceConfig, WalTransport};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};

/// Rows captured per measured run.
///
/// Large enough that per-run fixed costs — connecting, `START_REPLICATION`, the first
/// relation message — are a small fraction of the total, and small enough that the whole
/// harness stays inside a normal test timeout.
const ROWS: i64 = 50_000;

/// Rounds per transport. The first is discarded as a warm-up.
///
/// Not for the connector's benefit but for the container's: the first run pays page-cache
/// misses on freshly written WAL and, on a cold Docker volume, first-touch allocation. The
/// reported figure is the best of the scoring rounds, which is the least noisy summary
/// available without running long enough to characterise the distribution.
const ROUNDS: usize = 3;

/// Poll ceiling. Both transports batch up to this many messages per `next_events` call.
const MAX_EVENTS_PER_POLL: usize = 5_000;

struct Measurement {
    transport: &'static str,
    elapsed: Duration,
    polls: u32,
    events: usize,
}

impl Measurement {
    fn events_per_sec(&self) -> f64 {
        self.events as f64 / self.elapsed.as_secs_f64().max(f64::EPSILON)
    }

    fn events_per_poll(&self) -> f64 {
        self.events as f64 / f64::from(self.polls.max(1))
    }
}

/// Container image, overridable with `CDC_RS_PG_IMAGE` (e.g. `postgres:16`).
///
/// Configurable because this harness is meant to be *run*, and a hard-coded tag makes it
/// unrunnable on a machine that has a perfectly good PostgreSQL image cached under a
/// different one. The default matches the rest of the suite.
fn postgres_image() -> (String, String) {
    let spec =
        std::env::var("CDC_RS_PG_IMAGE").unwrap_or_else(|_| "postgres:16-alpine".to_string());
    match spec.rsplit_once(':') {
        Some((name, tag)) => (name.to_string(), tag.to_string()),
        None => (spec, "latest".to_string()),
    }
}

async fn start_postgres() -> rustcdc::Result<ContainerAsync<GenericImage>> {
    let (name, tag) = postgres_image();
    GenericImage::new(name, tag)
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "cdc")
        .with_cmd(vec![
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=32",
            "-c",
            "max_wal_senders=32",
        ])
        .start()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))
}

async fn connect(dsn: &str) -> rustcdc::Result<tokio_postgres::Client> {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

fn parse_lsn(text: &str) -> rustcdc::Result<u64> {
    let (high, low) = text
        .split_once('/')
        .ok_or_else(|| rustcdc::Error::SourceError(format!("not an LSN: {text}")))?;
    let high = u64::from_str_radix(high, 16)
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    let low = u64::from_str_radix(low, 16)
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    Ok((high << 32) | low)
}

/// Write `ROWS` inserts server-side.
///
/// `generate_series` rather than a client round trip per row: at 50 000 rows the round
/// trips would dominate the harness runtime while contributing nothing to what is being
/// measured, which is how fast the WAL is *read back*.
async fn write_rows(client: &tokio_postgres::Client) -> rustcdc::Result<()> {
    client
        .execute(
            "INSERT INTO public.measured (payload) \
             SELECT 'row-' || g FROM generate_series(1, $1::bigint) g",
            &[&ROWS],
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;
    Ok(())
}

async fn measure(
    admin: &tokio_postgres::Client,
    host: &str,
    port: u16,
    slot: &str,
    transport: WalTransport,
) -> rustcdc::Result<Measurement> {
    // Fresh slot per run so every round starts from the same position and no round inherits
    // another's `confirmed_flush_lsn`.
    admin
        .execute(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;

    let config = PostgresSourceConfig {
        host: host.to_string(),
        port,
        user: "postgres".into(),
        password: "postgres".to_string().into(),
        database: "cdc".into(),
        replication_slot_name: slot.into(),
        publication_name: "throughput_pub".into(),
        transport: rustcdc::TransportConfig::plaintext(),
        stream_poll_interval_ms: 10,
        max_events_per_poll: MAX_EVENTS_PER_POLL,
        wal_transport: transport,
        // The slot is created above; asking the connector to create it again would race the
        // statement that just ran.
        create_replication_slot_if_missing: false,
        ..PostgresSourceConfig::default()
    };

    let mut source = PostgresConnection::new(config);
    source.connect().await?;
    let mut stream = source.start_stream(None).await?;

    // Written after the stream is positioned, so the slot sees all of it, and *before* the
    // clock starts, so the measured interval is pure read-back.
    write_rows(admin).await?;

    let started = Instant::now();
    let deadline = started + Duration::from_secs(180);
    let mut events = 0usize;
    let mut polls = 0u32;

    while events < ROWS as usize && Instant::now() < deadline {
        let batch = stream.next_events(MAX_EVENTS_PER_POLL as u64).await?;
        polls += 1;
        events += batch
            .iter()
            .filter(|event| event.op == Operation::Insert && event.table == "measured")
            .count();
        // Confirm as a real consumer would — this is what advances `confirmed_flush_lsn`,
        // and on both transports it is a standby status update on the hot path.
        if let Some(last) = batch.last() {
            if let Ok(lsn) = parse_lsn(&last.source.offset) {
                stream.confirm_lsn(lsn).await?;
            }
        }
    }

    let elapsed = started.elapsed();
    drop(stream);
    source.close().await;

    // Truncate rather than delete-by-round so the next round's capture count is unambiguous.
    admin
        .batch_execute("TRUNCATE public.measured;")
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;

    Ok(Measurement {
        transport: match transport {
            WalTransport::PgWalstream => "pg_walstream",
            WalTransport::SqlPeek => "SqlPeek",
            _ => "wire (built-in)",
        },
        elapsed,
        polls,
        events,
    })
}

#[tokio::test]
async fn wire_and_pg_walstream_capture_at_comparable_rates() -> rustcdc::Result<()> {
    if std::env::var("CDC_RS_RUN_DOCKER_TESTS").as_deref() != Ok("1") {
        eprintln!(
            "skipping postgres WAL transport throughput evidence (set CDC_RS_RUN_DOCKER_TESTS=1)"
        );
        return Ok(());
    }

    let container = start_postgres().await?;
    let host = container
        .get_host()
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?
        .to_string();
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|error| rustcdc::Error::SourceError(error.to_string()))?;
    let dsn = format!(
        "host={host} port={port} user=postgres password=postgres dbname=cdc connect_timeout=30"
    );

    let admin = connect(&dsn).await?;
    admin
        .batch_execute(
            "
            CREATE TABLE public.measured (id BIGSERIAL PRIMARY KEY, payload TEXT NOT NULL);
            CREATE PUBLICATION throughput_pub FOR TABLE public.measured;
            ",
        )
        .await
        .map_err(|error| rustcdc::Error::SourceError(rustcdc::render_error_chain(&error)))?;

    let mut results: Vec<Measurement> = Vec::new();
    for round in 0..ROUNDS {
        // Interleaved, not grouped: a container that warms up or a host that gets busy
        // partway through would otherwise show up as a transport difference.
        for transport in [
            WalTransport::StreamingReplication,
            WalTransport::PgWalstream,
        ] {
            let label = match transport {
                WalTransport::PgWalstream => "walstream",
                _ => "wire",
            };
            let slot = format!("throughput_{label}_{round}");
            let measurement = measure(&admin, &host, port, &slot, transport).await?;
            if round > 0 {
                results.push(measurement);
            }
        }
    }

    println!(
        "\n  WAL transport capture throughput — {ROWS} inserts, best of {} scoring rounds\n",
        ROUNDS - 1
    );
    println!(
        "  {:<18} {:>10} {:>14} {:>8} {:>14}",
        "transport", "elapsed", "events/sec", "polls", "events/poll"
    );
    println!("  {}", "-".repeat(68));

    let mut best: Vec<(&'static str, f64)> = Vec::new();
    for transport in ["wire (built-in)", "pg_walstream"] {
        let Some(fastest) = results
            .iter()
            .filter(|m| m.transport == transport)
            .max_by(|a, b| a.events_per_sec().total_cmp(&b.events_per_sec()))
        else {
            continue;
        };
        println!(
            "  {:<18} {:>9.2}s {:>14.0} {:>8} {:>14.1}",
            fastest.transport,
            fastest.elapsed.as_secs_f64(),
            fastest.events_per_sec(),
            fastest.polls,
            fastest.events_per_poll(),
        );
        best.push((transport, fastest.events_per_sec()));
    }

    if let (Some((_, wire)), Some((_, walstream))) = (best.first(), best.get(1)) {
        println!(
            "\n  pg_walstream / wire: {:.2}×",
            walstream / wire.max(f64::EPSILON)
        );
    }
    println!(
        "\n  Runtime ceiling for comparison: `cargo bench --bench throughput`.\n  \
         A capture rate far below that ceiling is bound in the sink or the commit \n  \
         configuration, not in the transport.\n"
    );

    // The only assertion: both transports must capture everything. Throughput is reported,
    // never gated — see the module docs.
    for measurement in &results {
        assert_eq!(
            measurement.events, ROWS as usize,
            "{} captured {} of {ROWS} rows",
            measurement.transport, measurement.events,
        );
    }

    Ok(())
}
