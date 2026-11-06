//! Opt-in, worker-thread-local instrumentation for the ignored cost gate.
use std::{cell::RefCell, collections::BTreeMap, time::Instant};

#[derive(Default)]
struct Trace {
    active: bool,
    label: String,
    records: Vec<Record>,
}
thread_local! { static TRACE: RefCell<Trace> = RefCell::default(); }
pub(super) struct Record {
    class: String,
    start: Instant,
    end: Instant,
    query: bool,
}
pub(super) struct Span {
    start: Instant,
    class: Option<&'static str>,
    query: bool,
    active: bool,
}
impl Drop for Span {
    fn drop(&mut self) {
        let end = Instant::now();
        TRACE.with_borrow_mut(|trace| {
            if self.active && trace.active {
                trace.records.push(Record {
                    class: self
                        .class
                        .map(str::to_owned)
                        .unwrap_or_else(|| trace.label.clone()),
                    start: self.start,
                    end,
                    query: self.query,
                });
            }
        });
    }
}
pub(super) fn start() {
    TRACE.with_borrow_mut(|trace| {
        *trace = Trace {
            active: true,
            ..Default::default()
        }
    });
}
pub(super) fn take() -> Vec<Record> {
    TRACE.with_borrow_mut(|trace| {
        trace.active = false;
        std::mem::take(&mut trace.records)
    })
}
pub(super) fn label(name: &str) {
    TRACE.with_borrow_mut(|trace| {
        if trace.active {
            trace.label = if name.contains("::sample::") || name.contains("::accept::") {
                "accept"
            } else if name.contains("::mail_page::") {
                "feed_page"
            } else if name.contains("::observed_agents::") {
                "list_agents"
            } else if name.contains("::observed_message::") {
                "messages_id"
            } else if name.contains("::resolve::") || name.contains("::sender_label::") {
                "agent_lookup"
            } else {
                name
            }
            .to_owned();
        }
    });
}
fn span(class: Option<&'static str>, query: bool) -> Span {
    let active = TRACE.with_borrow(|trace| trace.active);
    Span {
        start: Instant::now(),
        class,
        query,
        active,
    }
}
pub(super) fn job() -> Span {
    label("other_unlabelled_job");
    span(None, false)
}
pub(super) fn maintenance(class: &'static str) -> Span {
    span(Some(class), false)
}
pub(super) fn query(class: &'static str) -> Span {
    span(Some(class), true)
}

fn ms(start: Instant, end: Instant) -> f64 {
    end.duration_since(start).as_secs_f64() * 1000.0
}
fn stats(mut values: Vec<f64>) -> (usize, f64, f64, f64, f64) {
    values.sort_by(f64::total_cmp);
    let n = values.len();
    (
        n,
        values[n / 2],
        values[n * 99 / 100],
        values[n - 1],
        values.iter().sum(),
    )
}
type Sample = (usize, Instant, Instant, Instant, f64, f64);
pub(super) fn report(records: &[Record], samples: Vec<Sample>, total_p99: f64, queue_p99: f64) {
    let start = samples
        .iter()
        .map(|s| s.1)
        .min()
        .expect("bug: accept samples");
    let end = samples
        .iter()
        .map(|s| s.3)
        .max()
        .expect("bug: accept samples");
    let elapsed = ms(start, end);
    let mut classes: BTreeMap<(&str, bool), Vec<f64>> = BTreeMap::new();
    let mut busy = Vec::new();
    for r in records.iter().filter(|r| r.end > start && r.start < end) {
        let a = r.start.max(start);
        let b = r.end.min(end);
        classes
            .entry((&r.class, r.query))
            .or_default()
            .push(ms(a, b));
        if !r.query {
            busy.push((a, b));
        }
    }
    if let Some(path) = std::env::var_os("M5_GATE_TRACE_PATH") {
        let origin = records
            .iter()
            .map(|r| r.start)
            .chain([start])
            .min()
            .expect("bug: trace origin");
        let raw = serde_json::json!({
            "window_start_ms": ms(origin, start), "window_end_ms": ms(origin, end),
            "records": records.iter().map(|r| serde_json::json!({"class":r.class,"query":r.query,"start_ms":ms(origin,r.start),"end_ms":ms(origin,r.end)})).collect::<Vec<_>>(),
            "accepts": samples.iter().map(|s| serde_json::json!({"id":s.0,"submitted_ms":ms(origin,s.1),"began_ms":ms(origin,s.2),"resumed_ms":ms(origin,s.3),"total_ms":s.4,"queue_ms":s.5})).collect::<Vec<_>>()
        });
        std::fs::write(path, serde_json::to_vec(&raw).expect("encode gate trace"))
            .expect("write gate trace");
    }
    for class in [
        "full_snapshot",
        "resume_validation",
        "feed_page",
        "feed_head",
        "mail_rare_filter",
        "mail_other",
        "messages_id",
        "agent_lookup",
        "list_agents",
        "accept",
        "post_accept_head",
        "post_accept_prune_retention",
        "explicit_wal_checkpoint",
    ] {
        if !classes.contains_key(&(class, false)) {
            println!(
                "GATE worker_class={class} detail=false count=0 p50_ms=NA p99_ms=NA max_ms=NA total_ms=0 busy_share_pct=0"
            );
        }
    }
    for ((class, query), values) in classes {
        let (n, p50, p99, max, total) = stats(values);
        println!(
            "GATE worker_class={class} detail={query} count={n} p50_ms={p50:.6} p99_ms={p99:.6} max_ms={max:.6} total_ms={total:.6} busy_share_pct={:.3}",
            total / elapsed * 100.0
        );
    }
    busy.sort_unstable();
    let mut previous = start;
    let mut gaps = Vec::new();
    let mut busy_ms = 0.0;
    for (a, b) in busy {
        if a > previous {
            gaps.push(ms(previous, a));
        }
        busy_ms += ms(a.max(previous), b);
        previous = b;
    }
    if end > previous {
        gaps.push(ms(previous, end));
    }
    if !gaps.is_empty() {
        let (n, p50, p99, max, total) = stats(gaps);
        println!(
            "GATE worker_between_jobs count={n} p50_ms={p50:.6} p99_ms={p99:.6} max_ms={max:.6} total_ms={total:.6}"
        );
    }
    println!(
        "GATE worker_window_ms={elapsed:.6} busy_ms={busy_ms:.6} busy_share_pct={:.3} closed_loop_accepts=true max_sampled_accepts_in_flight=1",
        busy_ms / elapsed * 100.0
    );
    for (id, submitted, began, _, total, queue) in samples
        .into_iter()
        .filter(|s| s.4 >= total_p99 || s.5 >= queue_p99)
    {
        let blockers = records
            .iter()
            .filter(|r| !r.query && r.class != "accept" && r.end > submitted && r.start < began)
            .map(|r| {
                format!(
                    "{}(job_ms={:.6},overlap_ms={:.6})",
                    r.class,
                    ms(r.start, r.end),
                    ms(r.start.max(submitted), r.end.min(began))
                )
            })
            .collect::<Vec<_>>();
        let accounted: f64 = records
            .iter()
            .filter(|r| !r.query && r.end > submitted && r.start < began)
            .map(|r| ms(r.start.max(submitted), r.end.min(began)))
            .sum();
        println!(
            "GATE tail_accept={id} total_ms={total:.6} queue_ms={queue:.6} unaccounted_queue_ms={:.6} queued_behind={}",
            (queue - accounted).max(0.0),
            blockers.join(" -> ")
        );
    }
    println!("GATE implicit_wal_checkpoint=inside_SQLite_commit_not_separately_observable");
}
