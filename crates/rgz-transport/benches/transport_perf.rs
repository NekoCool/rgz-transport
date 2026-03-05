use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Once};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Result, bail};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use hdrhistogram::Histogram;
use rgz_msgs::StringMsg;
use rgz_transport::Node;
use tokio::runtime::Runtime;
use tokio::sync::Notify;

static OBSERVABILITY_INIT: Once = Once::new();

#[derive(Debug, Clone, Copy)]
struct PerfStats {
    published: usize,
    received: usize,
    dropped: usize,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
}

fn init_observability() {
    OBSERVABILITY_INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_target(false)
            .try_init();

        let _ = metrics_exporter_prometheus::PrometheusBuilder::new().install_recorder();
    });
}

fn unix_time_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

async fn run_pub_sub_roundtrip(msg_size: usize, messages: usize) -> Result<PerfStats> {
    let topic = format!("/bench/transport/{}", uuid::Uuid::new_v4());
    let mut sub_node = Node::new(None);
    let pub_node = Node::new(None);

    let recv_count = Arc::new(AtomicUsize::new(0));
    let recv_notify = Arc::new(Notify::new());
    let hist = Arc::new(Mutex::new(Histogram::<u64>::new_with_bounds(
        1, 60_000_000, 3,
    )?));

    {
        let recv_count = Arc::clone(&recv_count);
        let recv_notify = Arc::clone(&recv_notify);
        let hist = Arc::clone(&hist);

        sub_node.subscribe(&topic, move |msg: StringMsg| {
            let now = unix_time_nanos();
            if let Some((sent_ns, _payload)) = msg.data.split_once('|')
                && let Ok(sent_ns) = sent_ns.parse::<u128>()
            {
                let latency_us = ((now.saturating_sub(sent_ns)) / 1_000) as u64;
                if let Ok(mut guard) = hist.lock() {
                    let _ = guard.record(latency_us.max(1));
                }
                metrics::histogram!("transport_bench_latency_us").record(latency_us as f64);
            }

            let total = recv_count.fetch_add(1, Ordering::Relaxed) + 1;
            metrics::counter!("transport_bench_received_total").increment(1);
            if total >= messages {
                recv_notify.notify_one();
            }
        })?;
    }

    let publisher = pub_node.advertise::<StringMsg>(&topic, None)?;
    let ready_deadline = Instant::now() + Duration::from_secs(3);
    while !publisher.is_ready() {
        if Instant::now() > ready_deadline {
            bail!("Publisher was not ready within timeout");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // Prefix is "<send_timestamp_ns>|" and the rest is fixed payload.
    let payload_len = msg_size.saturating_sub(32);
    let payload = "x".repeat(payload_len);

    for _ in 0..messages {
        let data = format!("{}|{}", unix_time_nanos(), payload);
        publisher.publish(StringMsg {
            data,
            ..Default::default()
        })?;
        metrics::counter!("transport_bench_published_total").increment(1);
    }

    tokio::time::timeout(Duration::from_secs(10), async {
        while recv_count.load(Ordering::Relaxed) < messages {
            recv_notify.notified().await;
        }
    })
    .await?;

    let received = recv_count.load(Ordering::Relaxed);
    let dropped = messages.saturating_sub(received);
    let (p50_us, p95_us, p99_us) = {
        let guard = hist
            .lock()
            .map_err(|err| anyhow::anyhow!("latency histogram lock poisoned: {err}"))?;
        if guard.is_empty() {
            (0, 0, 0)
        } else {
            (
                guard.value_at_quantile(0.50),
                guard.value_at_quantile(0.95),
                guard.value_at_quantile(0.99),
            )
        }
    };

    metrics::gauge!("transport_bench_drop_ratio").set(if messages == 0 {
        0.0
    } else {
        dropped as f64 / messages as f64
    });
    metrics::gauge!("transport_bench_p99_us").set(p99_us as f64);
    tracing::info!(
        published = messages,
        received,
        dropped,
        p50_us,
        p95_us,
        p99_us,
        "pub/sub benchmark run finished"
    );

    Ok(PerfStats {
        published: messages,
        received,
        dropped,
        p50_us,
        p95_us,
        p99_us,
    })
}

fn bench_transport_pub_sub(c: &mut Criterion) {
    init_observability();
    let runtime = Runtime::new().expect("failed to build tokio runtime");

    let mut group = c.benchmark_group("transport_pub_sub");
    group.sample_size(10);

    let messages_per_iteration = 500;
    for msg_size in [64usize, 1024, 8192] {
        group.throughput(Throughput::Bytes(
            (msg_size * messages_per_iteration) as u64,
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(msg_size),
            &msg_size,
            |b, &size| {
                b.to_async(&runtime).iter(|| async move {
                    let stats = run_pub_sub_roundtrip(size, messages_per_iteration)
                        .await
                        .expect("failed to run pub/sub benchmark");

                    tracing::info!(
                        msg_size = size,
                        published = stats.published,
                        received = stats.received,
                        dropped = stats.dropped,
                        p50_us = stats.p50_us,
                        p95_us = stats.p95_us,
                        p99_us = stats.p99_us,
                        "benchmark stats"
                    );
                });
            },
        );
    }

    group.finish();
}

criterion_group!(transport_benches, bench_transport_pub_sub);
criterion_main!(transport_benches);
