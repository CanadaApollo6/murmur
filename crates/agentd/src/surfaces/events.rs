//! The single cursor feed used by the control socket and WebSocket.
use super::*;
use crate::{
    mailroom::Anchor,
    supervisor::{Event, TurnOutcome},
};
use std::collections::HashMap;
use tokio::sync::{
    OwnedSemaphorePermit, Semaphore,
    broadcast::error::{RecvError, TryRecvError},
};

static SUBSCRIBERS: std::sync::LazyLock<Arc<Semaphore>> =
    std::sync::LazyLock::new(|| Arc::new(Semaphore::new(8)));
pub fn subscribe() -> Result<OwnedSemaphorePermit> {
    Ok(SUBSCRIBERS
        .clone()
        .try_acquire_owned()
        .map_err(|_| "feed subscriber limit reached")?)
}

fn cost(c: char) -> usize {
    match c {
        '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
        c if c < ' ' => 6,
        c => c.len_utf8(),
    }
}
pub fn cut(s: &str, limit: usize) -> String {
    bounded(s, limit, false)
}
pub fn tail(s: &str, limit: usize) -> String {
    bounded(s, limit, true)
}
fn bounded(s: &str, limit: usize, newest: bool) -> String {
    if s.chars().map(cost).sum::<usize>() + 2 <= limit {
        return s.to_owned();
    }
    let mut size = 5;
    let mut result = String::new();
    if newest {
        let mut chars = Vec::new();
        for c in s.chars().rev() {
            size += cost(c);
            if size > limit {
                break;
            }
            chars.push(c);
        }
        result.push('…');
        result.extend(chars.into_iter().rev());
    } else {
        for c in s.chars() {
            size += cost(c);
            if size > limit {
                break;
            }
            result.push(c);
        }
        result.push('…');
    }
    result
}
pub fn command(path: &Path, verb: &str, args: &[&str]) -> Option<String> {
    let path = path.to_str()?;
    let values = std::iter::once(path)
        .chain(args.iter().copied())
        .collect::<Vec<_>>();
    if values.iter().any(|v| v.chars().any(char::is_control)) {
        return None;
    }
    let quote = |v: &str| format!("'{}'", v.replace('\'', "'\\''"));
    Some(format!(
        "agentctl --socket {} {verb} -- {}",
        quote(path),
        args.iter().map(|s| quote(s)).collect::<Vec<_>>().join(" ")
    ))
}
pub fn permission(r: PermissionRequest, path: &Path) -> Value {
    let mut value = serde_json::to_value(&r).expect("bug: permission serializes");
    value["agent"] = json!(format!("agent:{}", r.agent));
    for (field, n) in [("tool", 1024), ("title", 4096), ("detail", 8192)] {
        if let Some(s) = value[field].as_str() {
            value[field] = json!(cut(s, n));
        }
    }
    value["options_truncated"] = json!(r.options.len() > 16);
    value["options"] = json!(
        r.options
            .into_iter()
            .take(16)
            .map(|o| {
                let key = cut(&o.key, 1024);
                let cmd = if key == o.key {
                    command(path, "approve", &[&r.id.to_string(), &o.key])
                } else {
                    None
                };
                json!({"key":key,"label":cut(&o.label,1024),"effect":o.effect,"command":cmd})
            })
            .collect::<Vec<_>>()
    );
    value
}
pub fn mail(row: &mut Value, path: &Path) {
    if let Some(reason) = row["reason"].as_str() {
        row["reason"] = json!(cut(reason, 1024));
    }
    if row["state"] == "held" {
        let id = row["id"].as_str().expect("bug: mail id is string");
        row["commands"] =
            json!({"release":command(path,"release",&[id]),"drop":command(path,"drop",&[id])});
    }
}
#[async_trait::async_trait]
pub trait Sink: Send {
    async fn send(&mut self, value: Value) -> Result<()>;
}
pub struct UnixSink<W>(pub W);
#[async_trait::async_trait]
impl<W: AsyncWrite + Unpin + Send> Sink for UnixSink<W> {
    async fn send(&mut self, value: Value) -> Result<()> {
        write_frame(&mut self.0, &value).await
    }
}
async fn send(sink: &mut impl Sink, value: Value) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), sink.send(value))
        .await
        .map_err(|_| "feed send timed out")?
}

async fn agent(
    api: &ControlApi,
    id: AgentId,
    fallback: Phase,
    cache: &mut HashMap<AgentId, (String, String)>,
) -> Result<Value> {
    if let std::collections::hash_map::Entry::Vacant(entry) = cache.entry(id)
        && let Some((_, spec, _)) = api
            .mailroom
            .observed_agents()
            .await?
            .into_iter()
            .find(|(a, _, _)| *a == id)
    {
        entry.insert((format!("@{}", spec.name.as_str()), spec.kind));
    }
    let h = api.broker.handle(id).await;
    let phase = h.as_ref().map_or(fallback, |h| h.phase());
    let (name, kind) = cache.get(&id).cloned().unwrap_or_default();
    Ok(agent_row(
        id,
        &name,
        &kind,
        &phase,
        h.as_ref().map_or("none", |h| h.isolation()),
    ))
}
fn agent_row(id: AgentId, name: &str, kind: &str, phase: &Phase, isolation: &str) -> Value {
    let error = match phase {
        Phase::Failed { last } => Some(cut(last, 1024)),
        Phase::NeedsRecovery { reason } => Some(cut(reason, 1024)),
        _ => None,
    };
    json!({"id":format!("agent:{id}"),"name":cut(name,1024),"kind":cut(kind,1024),"isolation":isolation,"phase":phase_name(phase),"error":error})
}
pub(crate) async fn snapshot(
    api: &ControlApi,
    resume: Option<Anchor>,
    cursor_only: Option<i64>,
    cache: &mut HashMap<AgentId, (String, String)>,
) -> Result<Value> {
    let (cursor, mut mail_state) = if let Some(c) = cursor_only {
        (c, Value::Null)
    } else {
        api.mailroom.mail_snapshot(resume).await?
    };
    for list in ["stuck", "moving"] {
        if let Some(rows) = mail_state.get_mut(list).and_then(Value::as_array_mut) {
            for row in rows {
                mail(row, &api.control_path);
            }
        }
    }
    let specs = api.mailroom.observed_agents().await?;
    let truncated = specs.len() > 1024;
    let mut agents = Vec::new();
    for (id, spec, _) in specs.into_iter().take(1024) {
        let name = format!("@{}", spec.name.as_str());
        cache.insert(id, (name.clone(), spec.kind.clone()));
        let h = api.broker.handle(id).await;
        agents.push(agent_row(
            id,
            &name,
            &spec.kind,
            &h.as_ref().map_or(Phase::Ended, |h| h.phase()),
            h.as_ref().map_or("none", |h| h.isolation()),
        ));
    }
    let approvals = api.broker.event_hub().permissions();
    Ok(
        json!({"event":"snapshot","mail_cursor":cursor,"mail":mail_state,"agents":agents,"agents_truncated":truncated,"approvals_truncated":approvals.len()>64,"approvals":approvals.into_iter().take(64).map(|r|permission(r,&api.control_path)).collect::<Vec<_>>()}),
    )
}
async fn state(
    api: &ControlApi,
    event: Event,
    cache: &mut HashMap<AgentId, (String, String)>,
) -> Result<Option<Value>> {
    Ok(Some(match event {
        Event::PhaseChanged {
            agent: id, phase, ..
        } => {
            let mut v = agent(api, id, phase, cache).await?;
            v["event"] = json!("agent");
            v
        }
        Event::PermissionPending(r) => {
            json!({"event":"permission_pending","request":permission(r,&api.control_path)})
        }
        Event::PermissionAnswered { agent, id } => {
            json!({"event":"permission_answered","agent":format!("agent:{agent}"),"id":id})
        }
        Event::TurnEnded { agent, outcome } => {
            json!({"event":"turn_ended","agent":format!("agent:{agent}"),"outcome":match outcome {TurnOutcome::Completed=>"completed",TurnOutcome::Cancelled=>"cancelled",TurnOutcome::Refused=>"refused",TurnOutcome::MaxTokens=>"max_tokens",TurnOutcome::Failed{..}=>"failed"}})
        }
        Event::EgressDenied { agent, host, port } => {
            json!({"event":"egress_denied","agent":format!("agent:{agent}"),"host":cut(&host,1024),"port":port})
        }
        _ => return Ok(None),
    }))
}

pub async fn feed(api: &ControlApi, sink: &mut impl Sink, resume: Option<Anchor>) -> Result<()> {
    let states = api.broker.event_hub().state();
    let activity = api.broker.event_hub().activity();
    let changes = api.mailroom.changes();
    run(api, sink, resume, states, activity, changes).await
}
async fn run(
    api: &ControlApi,
    sink: &mut impl Sink,
    resume: Option<Anchor>,
    mut states: tokio::sync::broadcast::Receiver<Event>,
    mut activity: tokio::sync::broadcast::Receiver<Event>,
    mut changes: tokio::sync::watch::Receiver<i64>,
) -> Result<()> {
    let mut cache = HashMap::new();
    let first = snapshot(api, resume, None, &mut cache).await?;
    let mut cursor = first["mail_cursor"]
        .as_i64()
        .expect("bug: snapshot has cursor");
    send(sink, first).await?;
    let mut pending = true;
    let mut text: HashMap<AgentId, String> = HashMap::new();
    let mut flush = tokio::time::Instant::now() + Duration::from_millis(100);
    loop {
        for _ in 0..32 {
            match states.try_recv() {
                Ok(e) => {
                    if let Some(v) = state(api, e, &mut cache).await? {
                        send(sink, v).await?;
                    }
                }
                Err(TryRecvError::Lagged(_)) => {
                    send(sink, snapshot(api, None, Some(cursor), &mut cache).await?).await?
                }
                Err(TryRecvError::Closed) => return Ok(()),
                Err(TryRecvError::Empty) => break,
            }
        }
        for _ in 0..1024 {
            match activity.try_recv() {
                Ok(Event::Activity { agent, text: t }) => {
                    let buf = text.entry(agent).or_default();
                    buf.push_str(&t);
                    *buf = tail(buf, 2048);
                }
                Ok(_) | Err(TryRecvError::Lagged(_)) => {}
                Err(TryRecvError::Closed) => return Ok(()),
                Err(TryRecvError::Empty) => break,
            }
        }
        if tokio::time::Instant::now() >= flush {
            for (agent, text) in text.drain() {
                send(
                    sink,
                    json!({"event":"activity","agent":format!("agent:{agent}"),"text":text}),
                )
                .await?;
            }
            flush = tokio::time::Instant::now() + Duration::from_millis(100);
        }
        if pending {
            changes.mark_unchanged();
            let (floor, rows) = api.mailroom.mail_page(cursor).await?;
            if floor.is_some_and(|n| n > cursor + 1) {
                let snap = snapshot(api, None, None, &mut cache).await?;
                cursor = snap["mail_cursor"]
                    .as_i64()
                    .expect("bug: snapshot has cursor");
                send(sink, snap).await?;
                continue;
            }
            pending = rows.len() == 256;
            for mut row in rows {
                cursor = row["seq"].as_i64().expect("bug: mail has seq");
                mail(&mut row, &api.control_path);
                send(sink, row).await?;
            }
            if pending {
                continue;
            }
        }
        tokio::select! {
            changed=changes.changed()=>{if changed.is_err(){return Ok(());} pending=true;},
            event=states.recv()=>match event {
                Ok(e)=>if let Some(v)=state(api,e,&mut cache).await? {send(sink,v).await?;},
                Err(RecvError::Lagged(_))=>send(sink,snapshot(api,None,Some(cursor),&mut cache).await?).await?,
                Err(RecvError::Closed)=>return Ok(())
            },
            event=activity.recv()=>match event {
                Ok(Event::Activity{agent,text:t})=>{let buf=text.entry(agent).or_default();buf.push_str(&t);*buf=tail(buf,2048);},
                Ok(_)|Err(RecvError::Lagged(_))=>{},Err(RecvError::Closed)=>return Ok(())
            },
            _=tokio::time::sleep_until(flush),if !text.is_empty()=>{}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mailroom::{Config, NewMessage};
    use std::sync::atomic::{AtomicUsize, Ordering};
    async fn api() -> (tempfile::TempDir, Arc<ControlApi>, AgentId) {
        let dir = tempfile::tempdir().expect("dir");
        let room = Arc::new(
            Mailroom::open(dir.path().join("mail.db"), Config::default())
                .await
                .expect("room"),
        );
        let id = room
            .register(AgentName::parse("a").expect("name"))
            .await
            .expect("agent");
        let broker =
            Arc::new(Broker::new(room.clone(), vec![], Default::default()).expect("broker"));
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let bindings = Arc::new(Bindings::new(
            dir.path().join("mcp.sock"),
            std::env::current_exe().expect("exe"),
            tx,
        ));
        (
            dir,
            Arc::new(ControlApi {
                broker,
                mailroom: room,
                bindings,
                control_path: "/tmp/control socket".into(),
                ui_url: None,
            }),
            id,
        )
    }
    async fn accept(api: &ControlApi, to: AgentId, n: usize) {
        api.mailroom
            .accept(NewMessage {
                from: Sender::Human,
                to,
                body: Body::new("body").expect("body"),
                reply_to: None,
                client_key: ClientKey(format!("key-{n}")),
                act: Act::Inform,
                task: None,
            })
            .await
            .expect("accept");
    }
    struct Record {
        tx: tokio::sync::mpsc::UnboundedSender<Value>,
        pause: Option<Arc<tokio::sync::Notify>>,
    }
    #[async_trait::async_trait]
    impl Sink for Record {
        async fn send(&mut self, v: Value) -> Result<()> {
            self.tx.send(v).map_err(|_| "receiver closed")?;
            if let Some(pause) = self.pause.take() {
                pause.notified().await;
            }
            Ok(())
        }
    }
    async fn recv(rx: &mut tokio::sync::mpsc::UnboundedReceiver<Value>) -> Value {
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("frame deadline")
            .expect("frame")
    }
    #[tokio::test]
    async fn lag_keeps_cursor_and_retention_resets_once() {
        let (_dir, api, id) = api().await;
        accept(&api, id, 0).await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let pause = Arc::new(tokio::sync::Notify::new());
        let p = pause.clone();
        let a = api.clone();
        let task =
            tokio::spawn(async move { feed(&a, &mut Record { tx, pause: Some(p) }, None).await });
        let first = recv(&mut rx).await;
        let cursor = first["mail_cursor"].as_i64().expect("cursor");
        for _ in 0..1100 {
            api.broker.event_hub().emit(Event::TurnEnded {
                agent: id,
                outcome: TurnOutcome::Completed,
            });
        }
        accept(&api, id, 1).await;
        pause.notify_one();
        let lag = recv(&mut rx).await;
        assert_eq!(lag["event"], "snapshot");
        assert!(lag["mail"].is_null());
        assert_eq!(lag["mail_cursor"], cursor);
        let mut saw = false;
        while !saw {
            let v = recv(&mut rx).await;
            saw = v["event"] == "mail";
        }
        // Establish a sequence gap atomically with a later accept on the same worker.
        api.mailroom
            .observe(|s| {
                s.db.execute(
                    "UPDATE sqlite_sequence SET seq=1000 WHERE name='mail_events'",
                    [],
                )?;
                s.db.execute("DELETE FROM mail_events", [])?;
                Ok(())
            })
            .await
            .expect("prune fixture");
        accept(&api, id, 2).await;
        loop {
            let v = recv(&mut rx).await;
            if v["event"] == "snapshot" {
                assert!(!v["mail"].is_null());
                assert_eq!(v["mail_cursor"], 1001);
                break;
            }
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(50), rx.recv())
                .await
                .is_err(),
            "one reset then quiet"
        );
        task.abort();
        assert!(task.await.expect_err("aborted").is_cancelled());
    }
    #[tokio::test]
    async fn interleaves_state_and_activity_with_full_mail_pages() {
        let (_dir, api, id) = api().await;
        accept(&api, id, 0).await;
        let (_, snap) = api.mailroom.mail_snapshot(None).await.expect("snapshot");
        let resume = serde_json::from_value(snap["anchor"].clone()).expect("anchor");
        for n in 1..800 {
            accept(&api, id, n).await;
        }
        struct Emit {
            api: Arc<ControlApi>,
            id: AgentId,
            frames: Vec<Value>,
            emitted: Option<usize>,
        }
        #[async_trait::async_trait]
        impl Sink for Emit {
            async fn send(&mut self, v: Value) -> Result<()> {
                if self.emitted.is_none() && v["event"] == "mail" {
                    self.emitted = Some(self.frames.len());
                    self.api.broker.event_hub().emit(Event::TurnEnded {
                        agent: self.id,
                        outcome: TurnOutcome::Completed,
                    });
                    self.api.broker.event_hub().emit(Event::Activity {
                        agent: self.id,
                        text: "newest".into(),
                    });
                }
                self.frames.push(v);
                Ok(())
            }
        }
        let mut sink = Emit {
            api: api.clone(),
            id,
            frames: vec![],
            emitted: None,
        };
        assert!(
            tokio::time::timeout(
                Duration::from_millis(200),
                feed(&api, &mut sink, Some(resume))
            )
            .await
            .is_err()
        );
        let at = sink.emitted.expect("emitted");
        let position = sink
            .frames
            .iter()
            .position(|v| v["event"] == "turn_ended")
            .expect("state event");
        assert!(
            sink.frames[at..position]
                .iter()
                .filter(|v| v["event"] == "mail")
                .count()
                <= 256
        );
        assert!(sink.frames.iter().any(|v| v["event"] == "activity"));
        assert_eq!(
            sink.frames.iter().filter(|v| v["event"] == "mail").count(),
            799
        );
    }
    #[tokio::test]
    async fn closed_inputs_end_subscriber_without_spinning() {
        let (_dir, api, _) = api().await;
        for closed in 0..3 {
            let (state_tx, state_rx) = tokio::sync::broadcast::channel(1);
            let (activity_tx, activity_rx) = tokio::sync::broadcast::channel(1);
            let (changes_tx, changes_rx) = tokio::sync::watch::channel(0);
            let mut owners = (Some(state_tx), Some(activity_tx), Some(changes_tx));
            match closed {
                0 => drop(owners.0.take()),
                1 => drop(owners.1.take()),
                _ => drop(owners.2.take()),
            };
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            tokio::time::timeout(
                Duration::from_secs(1),
                run(
                    &api,
                    &mut Record { tx, pause: None },
                    None,
                    state_rx,
                    activity_rx,
                    changes_rx,
                ),
            )
            .await
            .expect("closed channel terminates")
            .expect("feed");
        }
    }
    #[tokio::test(start_paused = true)]
    async fn blocked_sink_times_out_without_blocking_delivery() {
        let (_dir, api, id) = api().await;
        struct Block;
        #[async_trait::async_trait]
        impl Sink for Block {
            async fn send(&mut self, _: Value) -> Result<()> {
                std::future::pending().await
            }
        }
        let a = api.clone();
        let task = tokio::spawn(async move { feed(&a, &mut Block, None).await });
        accept(&api, id, 0).await;
        tokio::time::advance(Duration::from_secs(11)).await;
        assert!(
            task.await
                .expect("task")
                .expect_err("timeout")
                .to_string()
                .contains("timed out")
        );
    }
    #[tokio::test]
    async fn observer_permit_stays_with_worker_and_cancelled_reads_skip() {
        let (_dir, api, _) = api().await;
        let room = api.mailroom.clone();
        let entered = Arc::new(AtomicUsize::new(0));
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let marker = entered.clone();
        let r = room.clone();
        let first = tokio::spawn(async move {
            r.observe(move |_| {
                marker.fetch_add(1, Ordering::SeqCst);
                release_rx.recv().expect("release");
                Ok(())
            })
            .await
        });
        while entered.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        first.abort();
        assert!(first.await.expect_err("abort").is_cancelled());
        let mut requests = Vec::new();
        for _ in 0..32 {
            let r = room.clone();
            let count = entered.clone();
            requests.push(tokio::spawn(async move {
                r.observe(move |_| {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
            }));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(entered.load(Ordering::SeqCst), 1);
        for request in requests {
            request.abort();
            assert!(request.await.expect_err("aborted").is_cancelled());
        }
        release_tx.send(()).expect("release");
        room.observed_agents().await.expect("drain");
        assert_eq!(entered.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn encoded_bounds_and_maximum_shape() {
        let hostile = "\u{0}\n\"\\'".repeat(8192);
        for n in [1024, 2048, 4096, 8192] {
            for text in [cut(&hostile, n), tail(&hostile, n)] {
                assert!(serde_json::to_vec(&text).expect("encoded").len() <= n);
            }
        }
        let path = Path::new("/tmp/control socket");
        let agent = AgentId::new();
        let approval = permission(
            PermissionRequest {
                id: PermissionId(ulid::Ulid::new()),
                agent,
                incarnation: Incarnation(1),
                tool: hostile.clone(),
                title: hostile.clone(),
                detail: hostile.clone(),
                options: (0..17)
                    .map(|_| PermissionOption {
                        key: "'".repeat(1022),
                        label: hostile.clone(),
                        effect: PermissionEffect::AllowOnce,
                    })
                    .collect(),
            },
            path,
        );
        let edge = json!({"from":format!("agent:{agent}"),"to":format!("agent:{agent}"),"queued":i64::MAX,"offered":i64::MAX,"consumed":i64::MAX,"uncertain":i64::MAX,"held":i64::MAX,"dead":i64::MAX});
        let mut row = json!({"msg_seq":i64::MAX,"id":MessageId::new(),"from":format!("agent:{agent}"),"to":format!("agent:{agent}"),"conversation":ConversationId::new(),"in_reply_to":MessageId::new(),"hop":65535,"act":"request","task":TaskId::new(),"exposure":{"sender":format!("agent:{agent}"),"epoch":u64::MAX,"seq":u64::MAX},"state":"held","turn_started":true,"incarnation":u64::MAX,"attempts":u32::MAX,"reason":hostile});
        mail(&mut row, path);
        let snapshot = json!({"event":"snapshot","mail_cursor":i64::MAX,"agents":vec![agent_row(agent,&"'".repeat(1024),&"'".repeat(1024),&Phase::Failed{last:"\0".repeat(1024)},"bwrap+proxy");1024],"agents_truncated":true,"approvals":vec![approval;64],"approvals_truncated":true,"mail":{"anchor":{"seq":i64::MAX,"nonce":"a".repeat(32)},"edges":vec![edge;65536],"edges_truncated":true,"stuck":vec![row.clone();1000],"moving":vec![row;1000],"stuck_truncated":true,"moving_truncated":true}});
        let size = serde_json::to_vec(&snapshot).expect("snapshot").len();
        assert!(size < 64 * 1024 * 1024, "snapshot bytes {size}");
        println!("M5 maximum snapshot bytes={size}");
    }
}

#[cfg(test)]
mod quiet_tests {
    use super::*;
    #[tokio::test]
    async fn idle_subscriber_runs_no_worker_jobs() {
        let dir = tempfile::tempdir().expect("dir");
        let room = Arc::new(
            Mailroom::open(dir.path().join("mail.db"), Default::default())
                .await
                .expect("room"),
        );
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let api = Arc::new(ControlApi {
            broker: Arc::new(
                Broker::new(room.clone(), vec![], Default::default()).expect("broker"),
            ),
            mailroom: room.clone(),
            bindings: Arc::new(Bindings::new(
                dir.path().join("mcp.sock"),
                std::env::current_exe().expect("exe"),
                tx,
            )),
            control_path: dir.path().join("control.sock"),
            ui_url: None,
        });
        struct Discard;
        #[async_trait::async_trait]
        impl Sink for Discard {
            async fn send(&mut self, _: Value) -> Result<()> {
                Ok(())
            }
        }
        let task = tokio::spawn(async move { feed(&api, &mut Discard, None).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let before = room.jobs_run.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(before, 3, "snapshot, specs, then initial page");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            room.jobs_run.load(std::sync::atomic::Ordering::SeqCst),
            before,
            "idle feed polled worker"
        );
        task.abort();
        assert!(task.await.expect_err("abort").is_cancelled());
    }
}
