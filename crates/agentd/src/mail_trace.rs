//! Opt-in, worker-thread-local instrumentation for the ignored cost gate.
use std::{cell::RefCell, collections::BTreeMap, time::Instant};

#[derive(Clone, Copy, Default, serde::Serialize)]
struct Resource {
    user_us: i64,
    system_us: i64,
    cpu_ns: i64,
    voluntary: i64,
    involuntary: i64,
    minor_faults: i64,
    major_faults: i64,
}
impl Resource {
    fn cpu_ms(self) -> f64 {
        self.cpu_ns as f64 / 1_000_000.0
    }
    fn delta(self, before: Self) -> Self {
        Self {
            cpu_ns: self.cpu_ns - before.cpu_ns,
            user_us: self.user_us - before.user_us,
            system_us: self.system_us - before.system_us,
            voluntary: self.voluntary - before.voluntary,
            involuntary: self.involuntary - before.involuntary,
            minor_faults: self.minor_faults - before.minor_faults,
            major_faults: self.major_faults - before.major_faults,
        }
    }
}
fn resources() -> Resource {
    #[cfg(target_os = "linux")]
    {
        // getrusage initializes the entire C structure on success.
        let mut usage = std::mem::MaybeUninit::<nix::libc::rusage>::uninit();
        let result = unsafe { nix::libc::getrusage(nix::libc::RUSAGE_THREAD, usage.as_mut_ptr()) };
        assert_eq!(
            result,
            0,
            "getrusage for gate worker: {}",
            std::io::Error::last_os_error()
        );
        let u = unsafe { usage.assume_init() };
        let mut clock = std::mem::MaybeUninit::<nix::libc::timespec>::uninit();
        let result = unsafe {
            nix::libc::clock_gettime(nix::libc::CLOCK_THREAD_CPUTIME_ID, clock.as_mut_ptr())
        };
        assert_eq!(
            result,
            0,
            "worker CPU clock: {}",
            std::io::Error::last_os_error()
        );
        let clock = unsafe { clock.assume_init() };
        Resource {
            cpu_ns: clock.tv_sec * 1_000_000_000 + clock.tv_nsec,
            user_us: u.ru_utime.tv_sec * 1_000_000 + u.ru_utime.tv_usec,
            system_us: u.ru_stime.tv_sec * 1_000_000 + u.ru_stime.tv_usec,
            voluntary: u.ru_nvcsw,
            involuntary: u.ru_nivcsw,
            minor_faults: u.ru_minflt,
            major_faults: u.ru_majflt,
        }
    }
    #[cfg(not(target_os = "linux"))]
    Resource::default()
}
#[derive(Clone, Copy, Default, serde::Serialize)]
pub(super) struct Cache {
    hits: i32,
    misses: i32,
    writes: i32,
}
pub(super) fn cache(db: &rusqlite::Connection) -> Cache {
    if !TRACE.with_borrow(|trace| trace.active) {
        return Cache::default();
    }
    let read = |op| {
        let mut current = 0;
        let mut high = 0;
        // The worker owns the live connection; these calls only read counters.
        let result = unsafe {
            rusqlite::ffi::sqlite3_db_status(db.handle(), op, &mut current, &mut high, 0)
        };
        assert_eq!(
            result,
            rusqlite::ffi::SQLITE_OK,
            "read SQLite gate cache counter"
        );
        current
    };
    Cache {
        hits: read(rusqlite::ffi::SQLITE_DBSTATUS_CACHE_HIT),
        misses: read(rusqlite::ffi::SQLITE_DBSTATUS_CACHE_MISS),
        writes: read(rusqlite::ffi::SQLITE_DBSTATUS_CACHE_WRITE),
    }
}
pub(super) fn cache_delta(before: Cache, db: &rusqlite::Connection) {
    let after = cache(db);
    TRACE.with_borrow_mut(|trace| {
        trace.cache = Cache {
            hits: after.hits - before.hits,
            misses: after.misses - before.misses,
            writes: after.writes - before.writes,
        }
    });
}
#[derive(Default, serde::Serialize)]
struct SqlStats {
    rows: usize,
    candidate_rows: usize,
    vm_steps: i32,
    fullscan_steps: i32,
    sorts: i32,
    autoindex: i32,
    sql_wall_ms: f64,
    row_wall_ms: f64,
    cleanup_wall_ms: f64,
    plan: Option<Vec<String>>,
    expanded_sql: Option<String>,
}
pub(super) fn detailed() -> bool {
    TRACE.with_borrow(|trace| trace.active && trace.detailed)
}
pub(super) struct SqlProbe {
    timing: Span,
    stats: SqlStats,
}
impl SqlProbe {
    pub(super) fn new(phase: &'static str) -> Self {
        let mut timing = query(phase);
        timing.active &= detailed();
        Self {
            timing,
            stats: SqlStats::default(),
        }
    }
    pub(super) fn stamp(&self) -> Option<Instant> {
        self.timing.active.then(Instant::now)
    }
    pub(super) fn sql_done(&mut self, start: Option<Instant>) {
        if let Some(start) = start {
            self.stats.sql_wall_ms += start.elapsed().as_secs_f64() * 1000.0;
        }
    }
    pub(super) fn row_done(&mut self, start: Option<Instant>) {
        if let Some(start) = start {
            self.stats.row_wall_ms += start.elapsed().as_secs_f64() * 1000.0;
        }
    }
    pub(super) fn cleanup_done(&mut self, start: Option<Instant>) {
        if let Some(start) = start {
            self.stats.cleanup_wall_ms += start.elapsed().as_secs_f64() * 1000.0;
        }
    }
    pub(super) fn inspect(
        &mut self,
        db: &rusqlite::Connection,
        stmts: &[&rusqlite::Statement<'_>],
        rows: usize,
        candidate_rows: usize,
    ) -> rusqlite::Result<()> {
        use rusqlite::StatementStatus as S;
        if !self.timing.active {
            return Ok(());
        }
        self.stats.rows = rows;
        self.stats.candidate_rows = candidate_rows;
        let key = self.timing.class.clone().expect("bug: query phase label");
        let plan = TRACE.with_borrow_mut(|trace| trace.plans.insert(key));
        let mut expanded = Vec::new();
        let mut plans = Vec::new();
        for (n, stmt) in stmts.iter().enumerate() {
            self.stats.vm_steps += stmt.get_status(S::VmStep);
            self.stats.fullscan_steps += stmt.get_status(S::FullscanStep);
            self.stats.sorts += stmt.get_status(S::Sort);
            self.stats.autoindex += stmt.get_status(S::AutoIndex);
            if plan {
                let sql = stmt.expanded_sql().expect("expanded gate query");
                expanded.push(sql.clone());
                plans.push(format!("statement {n}"));
                plans.extend(
                    db.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?
                        .query_map([], |r| r.get(3))?
                        .collect::<rusqlite::Result<Vec<String>>>()?,
                );
            }
        }
        if plan {
            self.stats.plan = Some(plans);
            self.stats.expanded_sql = Some(expanded.join(";\n"));
        }
        Ok(())
    }
    pub(super) fn finish(mut self) {
        self.timing.sql = Some(self.stats);
    }
}
pub(super) fn read_rows<T>(
    db: &rusqlite::Connection,
    stmt: &mut rusqlite::Statement<'_>,
    params: impl rusqlite::Params,
    mut convert: impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    phase: &'static str,
) -> rusqlite::Result<Vec<T>> {
    let mut profile = SqlProbe::new(phase);
    let start = profile.stamp();
    let mut rows = stmt.query(params)?;
    profile.sql_done(start);
    let mut result = Vec::new();
    loop {
        let start = profile.stamp();
        let next = rows.next()?;
        profile.sql_done(start);
        let Some(row) = next else {
            break;
        };
        let start = profile.stamp();
        result.push(convert(row)?);
        profile.row_done(start);
    }
    let start = profile.stamp();
    drop(rows);
    profile.cleanup_done(start);
    profile.inspect(db, &[stmt], result.len(), 0)?;
    profile.finish();
    Ok(result)
}

#[derive(Default)]
struct Trace {
    active: bool,
    label: String,
    records: Vec<Record>,
    detailed: bool,
    plans: std::collections::BTreeSet<String>,
    cache: Cache,
    observer: bool,
    progress: Option<std::sync::Arc<Progress>>,
}
thread_local! { static TRACE: RefCell<Trace> = RefCell::default(); }
pub(super) struct Record {
    class: String,
    start: Instant,
    end: Instant,
    query: bool,
    observer: bool,
    resource: Resource,
    cache: Cache,
    sql: Option<SqlStats>,
}
pub(super) struct Span {
    start: Instant,
    class: Option<String>,
    query: bool,
    active: bool,
    resource: Resource,
    sql: Option<SqlStats>,
}
impl Drop for Span {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let resource = resources().delta(self.resource);
        let end = Instant::now();
        TRACE.with_borrow_mut(|trace| {
            if self.active && trace.active {
                let class = self.class.clone().unwrap_or_else(|| trace.label.clone());
                let observer = self.class.is_none() && trace.observer;
                if observer
                    && let Some(progress) = &trace.progress
                    && let Some(index) = OBSERVER_CLASSES.iter().position(|name| *name == class)
                {
                    progress.counts[index].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                trace.records.push(Record {
                    class,
                    start: self.start,
                    end,
                    query: self.query,
                    observer,
                    resource,
                    cache: if self.query {
                        Cache::default()
                    } else {
                        trace.cache
                    },
                    sql: self.sql.take(),
                });
            }
        });
    }
}
pub(super) fn start() -> std::sync::Arc<Progress> {
    let progress = std::sync::Arc::new(Progress::default());
    TRACE.with_borrow_mut(|trace| {
        *trace = Trace {
            active: true,
            detailed: std::env::var_os("M5_GATE_PROFILE").is_some(),
            progress: Some(progress.clone()),
            ..Default::default()
        }
    });
    progress
}
pub(super) fn mark_observer() {
    TRACE.with_borrow_mut(|trace| trace.observer = true);
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
        class: class.map(|class| {
            if query {
                TRACE.with_borrow(|trace| format!("{}.{}", trace.label, class))
            } else {
                class.to_owned()
            }
        }),
        query,
        active,
        resource: if active {
            resources()
        } else {
            Resource::default()
        },
        sql: None,
    }
}
pub(super) fn job() -> Span {
    TRACE.with_borrow_mut(|trace| {
        trace.cache = Cache::default();
        trace.observer = false;
    });
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

pub(super) const OBSERVER_CLASSES: [&str; 6] = [
    "full_snapshot",
    "feed_page",
    "list_agents",
    "mail_rare_filter",
    "mail_states_held_uncertain",
    "mail_states_queued_offered",
];
#[derive(Default)]
pub(super) struct Progress {
    counts: [std::sync::atomic::AtomicUsize; 6],
}
impl Progress {
    pub(super) fn begin_window(&self) {
        for count in &self.counts {
            count.store(0, std::sync::atomic::Ordering::Relaxed);
        }
    }
    pub(super) fn enough(&self) -> bool {
        self.counts
            .iter()
            .all(|n| n.load(std::sync::atomic::Ordering::Relaxed) >= 101)
    }
}
#[derive(Clone, Copy, Default, serde::Serialize)]
pub(super) struct Service {
    pub count: usize,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    pub total_ms: f64,
    pub cpu_p99_ms: Option<f64>,
    pub voluntary_p99: Option<f64>,
}
fn observer_services(
    records: &[Record],
    start: Instant,
    end: Instant,
) -> BTreeMap<String, Service> {
    let mut classes = OBSERVER_CLASSES
        .into_iter()
        .map(|name| (name.to_owned(), Vec::new()))
        .collect::<BTreeMap<_, _>>();
    for record in records
        .iter()
        .filter(|r| r.observer && !r.query && r.end > start && r.start < end)
    {
        classes.entry(record.class.clone()).or_default().push((
            ms(record.start, record.end),
            record.resource.cpu_ms(),
            record.resource.voluntary as f64,
        ));
    }
    classes
        .into_iter()
        .map(|(name, times)| {
            let service = if times.is_empty() {
                Service::default()
            } else {
                let cpu_p99_ms = cfg!(target_os = "linux")
                    .then(|| stats(times.iter().map(|(_, cpu, _)| *cpu).collect()).2);
                let voluntary_p99 = cfg!(target_os = "linux")
                    .then(|| stats(times.iter().map(|(_, _, switches)| *switches).collect()).2);
                let (count, p50_ms, p99_ms, max_ms, total_ms) =
                    stats(times.into_iter().map(|(wall, _, _)| wall).collect());
                Service {
                    count,
                    p50_ms,
                    p99_ms,
                    max_ms,
                    total_ms,
                    cpu_p99_ms,
                    voluntary_p99,
                }
            };
            (name, service)
        })
        .collect()
}
type Sample = (usize, Instant, Instant, Instant, f64, f64);
pub(super) fn report(
    lane: &str,
    records: &[Record],
    samples: Vec<Sample>,
    total_p99: f64,
    queue_p99: f64,
) -> BTreeMap<String, Service> {
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
    let services = observer_services(records, start, end);
    let elapsed = ms(start, end);
    let mut classes: BTreeMap<(&str, bool), Vec<f64>> = BTreeMap::new();
    let mut cpu_classes: BTreeMap<(&str, bool), Vec<f64>> = BTreeMap::new();
    let mut busy = Vec::new();
    for r in records.iter().filter(|r| r.end > start && r.start < end) {
        let a = r.start.max(start);
        let b = r.end.min(end);
        classes
            .entry((&r.class, r.query))
            .or_default()
            .push(ms(a, b));
        cpu_classes
            .entry((&r.class, r.query))
            .or_default()
            .push(r.resource.cpu_ms());
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
            "lane":lane,"observer_services":services,"window_start_ms": ms(origin, start), "window_end_ms": ms(origin, end),
            "records": records.iter().map(|r| serde_json::json!({"class":r.class,"query":r.query,"observer":r.observer,"start_ms":ms(origin,r.start),"end_ms":ms(origin,r.end),"resource":r.resource,"cache":r.cache,"sql":r.sql})).collect::<Vec<_>>(),
            "accepts": samples.iter().map(|s| serde_json::json!({"id":s.0,"submitted_ms":ms(origin,s.1),"began_ms":ms(origin,s.2),"resumed_ms":ms(origin,s.3),"total_ms":s.4,"queue_ms":s.5})).collect::<Vec<_>>()
        });
        let path = std::path::PathBuf::from(path).with_extension(format!("{lane}.json"));
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
                "GATE worker_class={class} lane={lane} detail=false count=0 p50_ms=NA p99_ms=NA max_ms=NA total_ms=0 cpu_p99_ms=NA busy_share_pct=0"
            );
        }
    }
    for ((class, query), values) in classes {
        let (n, p50, p99, max, total) = stats(values);
        let cpu = if cfg!(target_os = "linux") {
            format!(
                "{:.6}",
                stats(
                    cpu_classes
                        .remove(&(class, query))
                        .expect("bug: matching CPU samples")
                )
                .2
            )
        } else {
            "NA".into()
        };
        println!(
            "GATE worker_class={class} lane={lane} detail={query} count={n} p50_ms={p50:.6} p99_ms={p99:.6} max_ms={max:.6} total_ms={total:.6} cpu_p99_ms={cpu} busy_share_pct={:.3}",
            total / elapsed * 100.0
        );
    }
    for r in records.iter().filter(|r| {
        !r.query
            && r.end > start
            && r.start < end
            && (r.class == "full_snapshot" || r.class.starts_with("mail_"))
            && ms(r.start, r.end) >= 25.0
    }) {
        println!(
            "GATE observer_outlier class={} wall_ms={:.6} cpu_ms={:.6} voluntary_switches={} involuntary_switches={} minor_faults={} major_faults={} cache_hits={} cache_misses={} cache_writes={}",
            r.class,
            ms(r.start, r.end),
            r.resource.cpu_ms(),
            r.resource.voluntary,
            r.resource.involuntary,
            r.resource.minor_faults,
            r.resource.major_faults,
            r.cache.hits,
            r.cache.misses,
            r.cache.writes
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
            "GATE worker_between_jobs lane={lane} count={n} p50_ms={p50:.6} p99_ms={p99:.6} max_ms={max:.6} total_ms={total:.6}"
        );
    }
    println!(
        "GATE lane={lane} worker_window_ms={elapsed:.6} busy_ms={busy_ms:.6} busy_share_pct={:.3} closed_loop_accepts=true max_sampled_accepts_in_flight=1",
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
    for (class, service) in &services {
        let cpu = service
            .cpu_p99_ms
            .map_or_else(|| "NA".into(), |ms| format!("{ms:.6}"));
        let switches = service
            .voluntary_p99
            .map_or_else(|| "NA".into(), |n| format!("{n:.0}"));
        println!(
            "GATE observer_service lane={lane} class={class} count={} p50_ms={:.6} p99_ms={:.6} max_ms={:.6} total_ms={:.6} cpu_p99_ms={cpu} voluntary_p99={switches}",
            service.count, service.p50_ms, service.p99_ms, service.max_ms, service.total_ms
        );
    }
    services
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn service_time_keeps_whole_observer_job_at_window_boundary() {
        let start = Instant::now();
        let record = Record {
            class: "full_snapshot".into(),
            start,
            end: start + std::time::Duration::from_millis(30),
            query: false,
            observer: true,
            resource: Resource {
                cpu_ns: 3_000_000,
                ..Default::default()
            },
            cache: Cache::default(),
            sql: None,
        };
        let services = observer_services(
            &[record],
            start + std::time::Duration::from_millis(10),
            start + std::time::Duration::from_millis(20),
        );
        assert_eq!(services["full_snapshot"].count, 1);
        assert!(
            (services["full_snapshot"].p99_ms - 30.0).abs() < 0.000001,
            "observer service was clipped to sample window"
        );
        assert_eq!(
            services["full_snapshot"].cpu_p99_ms,
            cfg!(target_os = "linux").then_some(3.0),
            "thread CPU was replaced by wall time"
        );
    }
}
