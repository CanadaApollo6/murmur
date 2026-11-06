use super::*;
use serde_json::{Value, json};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Anchor {
    pub seq: i64,
    pub nonce: String,
}

#[derive(Clone, Default, Debug)]
pub struct MailQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    pub states: Vec<String>,
    pub before_msg_seq: Option<i64>,
    pub limit: usize,
}

pub(super) fn head(db: &Connection) -> Result<i64> {
    Ok(db.query_row(
        "SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name='mail_events'),0)",
        [],
        |r| r.get(0),
    )?)
}

pub(super) fn prune(db: &mut Connection) -> Result<()> {
    let tx = db.transaction()?;
    tx.execute("DELETE FROM mail_events WHERE seq IN (SELECT seq FROM mail_events WHERE seq < (SELECT seq FROM mail_events ORDER BY seq DESC LIMIT 1 OFFSET 65535) ORDER BY seq LIMIT 4096)", [])?;
    tx.commit()?;
    Ok(())
}

const META: &str = "m.seq,m.id,m.sender,m.recipient,m.conversation,m.in_reply_to,m.hop,CASE WHEN json_type(m.act)='text' THEN json_extract(m.act,'$') ELSE (SELECT key FROM json_each(m.act) LIMIT 1) END,m.task,m.exposure";
const DELIVERY: &str = "d.state,d.turn_started,d.incarnation,d.attempts,d.reason";

fn metadata(r: &Row<'_>) -> rusqlite::Result<Value> {
    let sender: Sender = json_col(r, 2)?;
    let exposure: Option<ExposureRef> = optional_json_col(r, 9)?;
    Ok(json!({
        "msg_seq":r.get::<_,i64>(0)?, "id":json_col::<MessageId>(r,1)?,
        "from":match sender { Sender::Human=>"human".into(), Sender::Agent(a)=>format!("agent:{a}") },
        "to":format!("agent:{}",json_col::<AgentId>(r,3)?),
        "conversation":json_col::<ConversationId>(r,4)?,
        "in_reply_to":optional_json_col::<MessageId>(r,5)?, "hop":r.get::<_,u16>(6)?,
        "act":r.get::<_,String>(7)?.to_ascii_lowercase(), "task":optional_json_col::<TaskId>(r,8)?,
        "exposure":exposure.map(|e|json!({"sender":format!("agent:{}",e.sender),"epoch":e.epoch,"seq":e.seq})),
        "state":r.get::<_,String>(10)?, "turn_started":r.get::<_,bool>(11)?,
        "incarnation":r.get::<_,Option<u64>>(12)?, "attempts":r.get::<_,u32>(13)?,
        "reason":r.get::<_,Option<String>>(14)?
    }))
}

pub(crate) fn query(db: &Connection, filter: &MailQuery) -> Result<Value> {
    let states = &filter.states;
    if states.iter().any(|s| {
        !["queued", "offered", "consumed", "uncertain", "held", "dead"].contains(&s.as_str())
    }) {
        return Err(Error::Rejected("invalid mail state".into()));
    }
    let before = filter.before_msg_seq.unwrap_or(i64::MAX);
    let limit = filter.limit.clamp(1, 1000);
    // State names above are a closed vocabulary. Each branch walks its state index.
    let candidates = if states.is_empty() {
        "SELECT seq FROM messages WHERE seq<?1 ORDER BY seq DESC LIMIT 10000".to_owned()
    } else {
        let branches = states.iter().map(|s|format!("SELECT msg_seq AS seq FROM (SELECT msg_seq FROM deliveries WHERE state='{s}' AND msg_seq<?1 ORDER BY msg_seq DESC LIMIT 10000)")).collect::<Vec<_>>().join(" UNION ");
        format!("SELECT seq FROM ({branches}) ORDER BY seq DESC LIMIT 10000")
    };
    let from = filter
        .from
        .as_ref()
        .map(|s| match s.strip_prefix("agent:") {
            Some(id) => json(&Sender::Agent(AgentId(
                id.parse()
                    .map_err(|_| Error::Rejected("invalid sender".into()))?,
            ))),
            None if s == "human" => json(&Sender::Human),
            None => Ok("no matching sender".into()),
        })
        .transpose()?;
    let to = filter
        .to
        .as_ref()
        .map(|s| match s.strip_prefix("agent:") {
            Some(id) => json(&AgentId(
                id.parse()
                    .map_err(|_| Error::Rejected("invalid recipient".into()))?,
            )),
            None => Ok("no matching recipient".into()),
        })
        .transpose()?;
    let sql = format!(
        "WITH candidates AS MATERIALIZED ({candidates}) SELECT {META},{DELIVERY} FROM candidates c JOIN messages m ON m.seq=c.seq JOIN deliveries d ON d.message_id=m.id WHERE (?2 IS NULL OR m.sender=?2) AND (?3 IS NULL OR m.recipient=?3) ORDER BY m.seq DESC LIMIT {limit}"
    );
    let mut stmt = db.prepare(&sql)?;
    let result = stmt
        .query_map(params![before, from, to], metadata)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let next = if result.len() == limit {
        result.last().and_then(|r| r["msg_seq"].as_i64())
    } else {
        let (count, last): (usize, Option<i64>) = db.query_row(
            &format!("SELECT COUNT(*),MIN(seq) FROM ({candidates})"),
            [before],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if count == 10000 { last } else { None }
    };
    Ok(json!({"rows":result,"next_before_msg_seq":next}))
}

fn window(db: &Connection, states: &[&str]) -> Result<(Vec<Value>, bool)> {
    let branches = states.iter().map(|s| format!("SELECT msg_seq FROM (SELECT msg_seq FROM deliveries WHERE state='{s}' ORDER BY msg_seq DESC LIMIT 1001)")).collect::<Vec<_>>().join(" UNION ALL ");
    let sql = format!(
        "WITH candidates AS MATERIALIZED (SELECT msg_seq FROM ({branches}) ORDER BY msg_seq DESC LIMIT 1001) SELECT {META},{DELIVERY} FROM candidates c JOIN messages m ON m.seq=c.msg_seq JOIN deliveries d ON d.message_id=m.id ORDER BY m.seq DESC"
    );
    let mut stmt = db.prepare(&sql)?;
    let mut rows = stmt
        .query_map([], metadata)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let truncated = rows.len() > 1000;
    rows.truncate(1000);
    Ok((rows, truncated))
}

pub(crate) fn snapshot(db: &Connection, resume: Option<Anchor>) -> Result<(i64, Value)> {
    if let Some(a) = resume {
        let valid: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM mail_events WHERE seq=? AND nonce=?)",
            params![a.seq, a.nonce],
            |r| r.get(0),
        )?;
        if valid {
            return Ok((a.seq, Value::Null));
        }
    }
    let cursor = head(db)?;
    let anchor = db
        .query_row(
            "SELECT seq,nonce FROM mail_events ORDER BY seq DESC LIMIT 1",
            [],
            |r| {
                Ok(Anchor {
                    seq: r.get(0)?,
                    nonce: r.get(1)?,
                })
            },
        )
        .optional()?;
    let mut edges = Vec::new();
    let mut stmt = db.prepare(
        "SELECT sender,recipient,state,n FROM mail_edges WHERE n>0 ORDER BY sender,recipient,state",
    )?;
    let mut rows = stmt.query([])?;
    let mut pair = None;
    let mut truncated = false;
    while let Some(r) = rows.next()? {
        let from: Sender = json_col(r, 0)?;
        let to: AgentId = json_col(r, 1)?;
        let key = (from.clone(), to);
        if pair.as_ref() != Some(&key) {
            if edges.len() == 65536 {
                truncated = true;
                break;
            }
            edges.push(json!({"from":match from {Sender::Human=>"human".into(),Sender::Agent(a)=>format!("agent:{a}")},"to":format!("agent:{to}"),"queued":0,"offered":0,"consumed":0,"uncertain":0,"held":0,"dead":0}));
            pair = Some(key);
        }
        let state: String = r.get(2)?;
        edges.last_mut().expect("bug: edge pair inserted")[state] = json!(r.get::<_, i64>(3)?);
    }
    let (stuck, stuck_truncated) = window(db, &["held", "uncertain"])?;
    let (moving, moving_truncated) = window(db, &["queued", "offered"])?;
    Ok((
        cursor,
        json!({"anchor":anchor,"edges":edges,"edges_truncated":truncated,"stuck":stuck,"moving":moving,"stuck_truncated":stuck_truncated,"moving_truncated":moving_truncated}),
    ))
}

pub(crate) fn page(db: &Connection, cursor: i64) -> Result<(Option<i64>, Vec<Value>)> {
    let floor = db.query_row("SELECT MIN(seq) FROM mail_events", [], |r| r.get(0))?;
    let sql = format!(
        "SELECT {META},e.state,e.turn_started,e.incarnation,e.attempts,e.reason,e.seq,e.nonce,e.prev_state,e.at FROM (SELECT * FROM mail_events WHERE seq>? ORDER BY seq LIMIT 256) e JOIN messages m ON m.id=e.message_id ORDER BY e.seq"
    );
    let mut stmt = db.prepare(&sql)?;
    let rows = stmt
        .query_map([cursor], |r| {
            let mut v = metadata(r)?;
            v["event"] = json!("mail");
            v["seq"] = json!(r.get::<_, i64>(15)?);
            v["nonce"] = json!(r.get::<_, String>(16)?);
            v["prev_state"] = json!(r.get::<_, Option<String>>(17)?);
            v["at"] = json!(r.get::<_, String>(18)?);
            Ok(v)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok((floor, rows))
}

pub(crate) fn agents(db: &Connection) -> Result<Vec<(AgentId, crate::supervisor::AgentSpec, i64)>> {
    let mut stmt=db.prepare("SELECT s.agent,s.spec,COALESCE(q.n,0) FROM agent_specs s JOIN agents a ON a.id=s.agent LEFT JOIN (SELECT recipient,SUM(n) n FROM mail_edges WHERE state='queued' GROUP BY recipient) q ON q.recipient=s.agent WHERE a.retired=0 ORDER BY a.name")?;
    Ok(stmt
        .query_map([], |r| Ok((json_col(r, 0)?, json_col(r, 1)?, r.get(2)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

impl Mailroom {
    pub async fn mail_query(&self, filter: MailQuery) -> Result<Value> {
        self.observe(move |s| query(&s.db, &filter)).await
    }
    pub async fn mail_snapshot(&self, resume: Option<Anchor>) -> Result<(i64, Value)> {
        self.observe(move |s| snapshot(&s.db, resume)).await
    }
    pub async fn mail_page(&self, cursor: i64) -> Result<(Option<i64>, Vec<Value>)> {
        self.observe(move |s| page(&s.db, cursor)).await
    }
    pub async fn observed_agents(
        &self,
    ) -> Result<Vec<(AgentId, crate::supervisor::AgentSpec, i64)>> {
        self.observe(|s| agents(&s.db)).await
    }
    pub async fn observed_message(&self, id: MessageId) -> Result<Value> {
        self.observe(move |s| {
            let env = message(&s.db, id)?;
            let size = env.body().as_str().len();
            let mut v = serde_json::to_value(env)?;
            v["bytes"] = json!(size);
            Ok(v)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    fn edge_key(v: &Value) -> (String, String) {
        (
            v["from"].as_str().expect("from").into(),
            v["to"].as_str().expect("to").into(),
        )
    }
    fn lists(mail: &Value) -> BTreeMap<String, Value> {
        mail["stuck"]
            .as_array()
            .expect("stuck")
            .iter()
            .chain(mail["moving"].as_array().expect("moving"))
            .map(|v| (v["id"].as_str().expect("id").into(), v.clone()))
            .collect()
    }
    fn edges(mail: &Value) -> BTreeMap<(String, String, String), i64> {
        let mut result = BTreeMap::new();
        for e in mail["edges"].as_array().expect("edges") {
            for state in ["queued", "offered", "consumed", "uncertain", "held", "dead"] {
                let n = e[state].as_i64().expect("count");
                if n > 0 {
                    let (from, to) = edge_key(e);
                    result.insert((from, to, state.into()), n);
                }
            }
        }
        result
    }
    struct Hold;
    impl Interceptor for Hold {
        fn name(&self) -> &'static str {
            "test hold"
        }
        fn inspect(&self, e: &Envelope, _: &ConversationView) -> Verdict {
            if e.body().as_str() == "hold" {
                Verdict::Hold {
                    reason: "test".into(),
                }
            } else {
                Verdict::Deliver
            }
        }
    }
    fn msg(to: AgentId, n: usize) -> NewMessage {
        NewMessage {
            from: Sender::Human,
            to,
            body: Body::new(if n.is_multiple_of(3) { "hold" } else { "hello" }).expect("body"),
            reply_to: None,
            client_key: ClientKey(format!("key-{n}")),
            act: Act::Inform,
            task: None,
        }
    }
    #[tokio::test]
    async fn replay_invariant_every_operation() {
        let dir = tempfile::tempdir().expect("dir");
        let room = Mailroom::open(
            dir.path().join("mail.db"),
            Config {
                interceptors: vec![Box::new(Hold)],
                ..Default::default()
            },
        )
        .await
        .expect("open");
        let mut agent = room
            .register(AgentName::parse("a").expect("name"))
            .await
            .expect("register");
        // This row intentionally has no delivery. Subsequent delivery rowids differ from messages.seq.
        room.accept(msg(agent, 0)).await.expect("accept");
        room.call(|s|{s.db.execute("INSERT INTO messages(id,sender,client_key,recipient,conversation,hop,act,body,sent_at) SELECT ?,sender,'orphan',recipient,conversation,hop,act,body,sent_at FROM messages LIMIT 1",[json(&MessageId::new())?])?;Ok(())}).await.expect("orphan");
        let (mut cursor, initial) = room.mail_snapshot(None).await.expect("snapshot");
        let mut applied_edges = edges(&initial);
        let mut applied_rows = lists(&initial);
        let mut ids = Vec::new();
        let mut seed = 0x12345678_u64;
        for step in 1..=240 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            if step % 60 == 0 {
                room.retire(agent).await.expect("retire");
                ids.clear();
                agent = room
                    .register(AgentName::parse(&format!("a{step}")).expect("name"))
                    .await
                    .expect("register");
            } else if ids.is_empty() || step % 3 == 0 {
                let (_, env) = room.accept(msg(agent, step)).await.expect("accept");
                ids.push(env.id());
                let before = room.mail_snapshot(None).await.expect("before duplicate").0;
                room.accept(msg(agent, step)).await.expect("duplicate");
                assert_eq!(
                    room.mail_snapshot(None).await.expect("after duplicate").0,
                    before
                );
            } else {
                let id = ids[(seed as usize) % ids.len()];
                match room.delivery(id).await.expect("delivery").state {
                    DeliveryState::Queued => {
                        room.mark_offered(&[id], Incarnation(1))
                            .await
                            .expect("offer");
                    }
                    DeliveryState::Offered { incarnation } => match seed % 3 {
                        0 => room.mark_started(&[id], incarnation).await.expect("start"),
                        1 => room
                            .mark_consumed(&[id], incarnation)
                            .await
                            .expect("consume"),
                        _ => {
                            room.session_ended(agent, incarnation, seed.is_multiple_of(2))
                                .await
                                .expect("end");
                        }
                    },
                    DeliveryState::Held { .. } | DeliveryState::Uncertain { .. } => {
                        if seed.is_multiple_of(2) {
                            room.release(id).await.expect("release");
                        } else {
                            room.drop_held(id, "operator").await.expect("drop");
                        }
                    }
                    DeliveryState::Consumed | DeliveryState::Dead { .. } => {
                        assert!(room.mark_started(&[id], Incarnation(3)).await.is_err());
                    }
                }
            }
            loop {
                let (_, page) = room.mail_page(cursor).await.expect("page");
                if page.is_empty() {
                    break;
                }
                for mut frame in page {
                    cursor = frame["seq"].as_i64().expect("seq");
                    let (from, to) = edge_key(&frame);
                    if let Some(prev) = frame["prev_state"].as_str() {
                        *applied_edges
                            .entry((from.clone(), to.clone(), prev.into()))
                            .or_default() -= 1;
                    }
                    *applied_edges
                        .entry((from, to, frame["state"].as_str().expect("state").into()))
                        .or_default() += 1;
                    applied_edges.retain(|_, n| *n != 0);
                    let id = frame["id"].as_str().expect("id").to_owned();
                    let state = frame["state"].as_str().expect("state");
                    if ["queued", "offered", "held", "uncertain"].contains(&state) {
                        for key in ["event", "seq", "nonce", "prev_state", "at"] {
                            frame.as_object_mut().expect("frame").remove(key);
                        }
                        applied_rows.insert(id, frame);
                    } else {
                        applied_rows.remove(&id);
                    }
                }
            }
            let (_, fresh) = room.mail_snapshot(None).await.expect("snapshot");
            assert_eq!(
                applied_edges,
                edges(&fresh),
                "replayed edges diverged at step {step}"
            );
            assert_eq!(
                applied_rows,
                lists(&fresh),
                "replayed lists diverged at step {step}"
            );
            room.observe(|s|{
                let mismatches:i64=s.db.query_row("SELECT COUNT(*) FROM deliveries d JOIN messages m ON m.id=d.message_id WHERE d.msg_seq IS NOT m.seq",[],|r|r.get(0))?;
                assert_eq!(mismatches,0,"msg_seq invariant");
                let source:i64=s.db.query_row("SELECT COUNT(*) FROM (SELECT sender,recipient,state,COUNT(*) n FROM messages m JOIN deliveries d ON d.message_id=m.id GROUP BY sender,recipient,state EXCEPT SELECT sender,recipient,state,n FROM mail_edges WHERE n>0)",[],|r|r.get(0))?;
                assert_eq!(source,0,"source edges invariant");Ok(())
            }).await.expect("audit");
        }
        room.close().await.expect("close");
    }
    #[tokio::test]
    async fn metadata_authorizer_continuation_and_resume() {
        let dir = tempfile::tempdir().expect("dir");
        let room = Mailroom::open(dir.path().join("mail.db"), Config::default())
            .await
            .expect("open");
        let a = room
            .register(AgentName::parse("a").expect("name"))
            .await
            .expect("register");
        for n in 0..1050 {
            room.accept(msg(a, n)).await.expect("accept");
        }
        room.observe(|s| {
            s.db.authorizer(Some(|ctx: rusqlite::hooks::AuthContext<'_>| {
                match ctx.action {
                    rusqlite::hooks::AuthAction::Read {
                        table_name: "messages",
                        column_name: "body" | "sent_at",
                    } => rusqlite::hooks::Authorization::Deny,
                    _ => rusqlite::hooks::Authorization::Allow,
                }
            }));
            let (cursor, snap) = snapshot(&s.db, None)?;
            assert_eq!(snap["moving_truncated"], true);
            let a: Anchor = serde_json::from_value(snap["anchor"].clone())?;
            assert_eq!(snapshot(&s.db, Some(a.clone()))?, (cursor, Value::Null));
            assert!(
                !snapshot(
                    &s.db,
                    Some(Anchor {
                        seq: a.seq,
                        nonce: "wrong".into()
                    })
                )?
                .1
                .is_null()
            );
            assert_eq!(page(&s.db, 0)?.1.len(), 256);
            assert_eq!(
                query(
                    &s.db,
                    &MailQuery {
                        states: vec!["queued".into()],
                        limit: 1000,
                        ..Default::default()
                    }
                )?["rows"]
                    .as_array()
                    .expect("rows")
                    .len(),
                1000
            );
            assert!(
                query(
                    &s.db,
                    &MailQuery {
                        from: Some("missing".into()),
                        limit: 1,
                        ..Default::default()
                    }
                )?["next_before_msg_seq"]
                    .is_null()
            );
            agents(&s.db)?;
            Ok(())
        })
        .await
        .expect("authorized metadata");
        room.close().await.expect("close");
    }
    #[test]
    fn migration_versions() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("mail.db");
        let db = Connection::open(&path).expect("db");
        db.execute_batch(include_str!("schema.sql"))
            .expect("v1 schema");
        db.execute_batch("PRAGMA user_version=1").expect("version");
        drop(db);
        for _ in 0..2 {
            let s = Store::open(&path, Config::default()).expect("migrate");
            assert_eq!(
                s.db.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
                    .expect("version"),
                2
            );
        }
        let db = Connection::open(&path).expect("db");
        db.execute_batch("PRAGMA user_version=3").expect("v3");
        drop(db);
        assert!(Store::open(&path, Config::default()).is_err());
    }
}

#[cfg(test)]
mod gate {
    use super::*;
    use crate::supervisor::{AgentSpec, Broker};
    use crate::surfaces::{Bindings, ControlApi, events};
    use std::time::{Duration, Instant};
    #[derive(Default)]
    struct Discard {
        replayed: Option<Arc<std::sync::atomic::AtomicUsize>>,
    }
    #[async_trait::async_trait]
    impl events::Sink for Discard {
        async fn send(&mut self, value: Value) -> crate::surfaces::Result<()> {
            if value["event"] == "mail"
                && let Some(count) = &self.replayed
            {
                count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Ok(())
        }
    }
    fn percentile(mut times: Vec<f64>) -> f64 {
        times.sort_by(f64::total_cmp);
        times[(times.len() * 99 / 100).min(times.len() - 1)]
    }
    async fn sample(room: &Mailroom, to: AgentId, tag: &str) -> f64 {
        let mut times = Vec::new();
        for n in 0..500 {
            let started = Instant::now();
            room.accept(NewMessage {
                from: Sender::Human,
                to,
                body: Body::new("gate delivery").expect("body"),
                reply_to: None,
                client_key: ClientKey(format!("{tag}-{n}")),
                act: Act::Inform,
                task: None,
            })
            .await
            .expect("real accept");
            times.push(started.elapsed().as_secs_f64() * 1000.0);
        }
        percentile(times)
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "full-scale disk and latency measurement; run explicitly with --ignored --nocapture"]
    async fn m5_cost_gate() {
        let dir =
            tempfile::tempdir_in(std::env::current_dir().expect("cwd")).expect("gate directory");
        let room = Arc::new(
            Mailroom::open(dir.path().join("mail.db"), Config::default())
                .await
                .expect("open"),
        );
        let mut agents = Vec::new();
        for n in 0..40 {
            let name = AgentName::parse(&format!("gate-{n}")).expect("name");
            let id = room.register(name.clone()).await.expect("agent");
            room.save_spec(
                id,
                AgentSpec {
                    name,
                    kind: "claude".into(),
                    cwd: dir.path().into(),
                    workspace: None,
                    model: None,
                    permissions: Default::default(),
                    idle_after: None,
                },
            )
            .await
            .expect("spec");
            agents.push(id);
        }
        let ids = agents.clone();
        room.call(move|s| {
            let tx=s.db.transaction()?;
            let mut message=tx.prepare("INSERT INTO messages(id,sender,client_key,recipient,conversation,hop,act,body,sent_at) VALUES (?,?,?,?,?,0,'\"Inform\"',?,?)")?;
            let mut delivery=tx.prepare("INSERT INTO deliveries(message_id,state,reason,msg_seq) VALUES (?,?,?,?)")?;
            let small="x".repeat(512);let large="x".repeat(65536);
            for n in 1..=1_000_000_u64 {
                let id=format!("\"{n:026}\"");let to=json(&ids[n as usize%40])?;
                let from=json(&Sender::Agent(ids[(n as usize/40)%40]))?;
                message.execute(params![id,from,format!("seed-{n}"),to,format!("\"{n:026}\""),if n>980000 {&large}else{&small},"[2026,1,0,0,0,0,0,0,0]"])?;
                let msg_seq=tx.last_insert_rowid();
                let state=if n>980000 {match n%4 {0=>"held",1=>"uncertain",2=>"queued",_=>"offered"}}else{"consumed"};
                // Offered/uncertain require an incarnation, bound below in the same insert.
                if state=="offered"||state=="uncertain" {
                    tx.execute("INSERT INTO deliveries(message_id,state,incarnation,msg_seq) VALUES (?,?,1,?)",params![id,state,msg_seq])?;
                }else {delivery.execute(params![id,state,if state=="held" {Some("gate hold")}else{None},msg_seq])?;}
            }
            drop(message);drop(delivery);tx.commit()?;
            while s.db.query_row("SELECT COUNT(*) FROM mail_events",[],|r|r.get::<_,i64>(0))?>65536 {prune(&mut s.db)?;}
            Ok(())
        }).await.expect("seed 1M");
        println!(
            "GATE fixture messages=1000000 agents=40 newest_large=20000 body_bytes=65536 synchronous=FULL"
        );
        async fn snapshot_ms(room: &Mailroom) -> f64 {
            let mut values = Vec::new();
            for _ in 0..7 {
                values.push(
                    room.observe(|s| {
                        let start = Instant::now();
                        snapshot(&s.db, None)?;
                        Ok(start.elapsed().as_secs_f64() * 1000.0)
                    })
                    .await
                    .expect("snapshot measurement"),
                );
            }
            values.sort_by(f64::total_cmp);
            values[3]
        }
        let normal = snapshot_ms(&room).await;
        let to = agents[0];
        let recipient = json(&to).expect("recipient");
        room.call(move|s|{let tx=s.db.transaction()?;tx.execute("UPDATE deliveries SET state='queued' WHERE msg_seq<=200000",[])?;tx.execute("UPDATE messages SET recipient=? WHERE seq<=200000",[recipient])?;
            // The fixture changes immutable metadata only before measurement, then rebuilds its aggregate.
            tx.execute("DELETE FROM mail_edges",[])?;tx.execute("INSERT INTO mail_edges SELECT m.sender,m.recipient,d.state,COUNT(*) FROM messages m JOIN deliveries d ON d.message_id=m.id GROUP BY m.sender,m.recipient,d.state",[])?;tx.commit()?;while s.db.query_row("SELECT COUNT(*) FROM mail_events",[],|r|r.get::<_,i64>(0))?>65536 {prune(&mut s.db)?;}Ok(())}).await.expect("dormant backlog");
        let backlog = snapshot_ms(&room).await;
        let baseline = sample(&room, to, "baseline").await;
        room.call(|s| prune(&mut s.db))
            .await
            .expect("prune baseline before replay anchor");
        let broker =
            Arc::new(Broker::new(room.clone(), vec![], Default::default()).expect("broker"));
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let api = Arc::new(ControlApi {
            broker,
            mailroom: room.clone(),
            bindings: Arc::new(Bindings::new(
                dir.path().join("mcp.sock"),
                std::env::current_exe().expect("exe"),
                tx,
            )),
            control_path: dir.path().join("control.sock"),
            ui_url: None,
        });
        let anchor = room
            .observe(|s| {
                Ok(s.db.query_row(
                    "SELECT seq,nonce FROM mail_events ORDER BY seq LIMIT 1",
                    [],
                    |r| {
                        Ok(Anchor {
                            seq: r.get(0)?,
                            nonce: r.get(1)?,
                        })
                    },
                )?)
            })
            .await
            .expect("anchor");
        let mut tasks = tokio::task::JoinSet::new();
        let replayed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for n in 0..8 {
            let api = api.clone();
            let replayed = replayed.clone();
            let anchor = anchor.clone();
            tasks.spawn(async move {
                let _permit = events::subscribe().expect("subscriber");
                loop {
                    let mut sink = Discard {
                        replayed: if n == 0 { Some(replayed.clone()) } else { None },
                    };
                    if n == 0 {
                        events::feed(&api, &mut sink, Some(anchor.clone()))
                            .await
                            .expect("backlog feed");
                        return;
                    }

                    if let Ok(Err(e)) = tokio::time::timeout(
                        Duration::from_millis(if n == 0 { 2000 } else { 100 }),
                        events::feed(
                            &api,
                            &mut sink,
                            if n == 0 { Some(anchor.clone()) } else { None },
                        ),
                    )
                    .await
                    {
                        panic!("feed: {e}");
                    }
                }
            });
        }
        for n in 0..8 {
            let room = room.clone();
            tasks.spawn(async move {
                loop {
                    for states in [
                        vec!["held".into(), "uncertain".into()],
                        vec!["queued".into(), "offered".into()],
                    ] {
                        room.mail_query(MailQuery {
                            states,
                            limit: 1000,
                            ..Default::default()
                        })
                        .await
                        .expect("view");
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            });
            let _ = n;
        }
        for rare in [false, true] {
            let room = room.clone();
            tasks.spawn(async move {
                loop {
                    if rare {
                        room.mail_query(MailQuery {
                            from: Some("no such sender".into()),
                            limit: 1000,
                            ..Default::default()
                        })
                        .await
                        .expect("rare query");
                    } else {
                        room.observed_agents().await.expect("list_agents");
                    }
                }
            });
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            while replayed.load(std::sync::atomic::Ordering::Relaxed) < 256 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("replay starts before pruning");
        // Begin with pruning due while the observers contend with the real accept path.
        room.call(|s| {
            s.last_prune = 0;
            Ok(())
        })
        .await
        .expect("activate pruning");
        let loaded = sample(&room, to, "loaded").await;
        tokio::time::timeout(Duration::from_secs(60), async {
            while replayed.load(std::sync::atomic::Ordering::Relaxed) < 65535 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("full retained replay completes under observer load");
        let replayed = replayed.load(std::sync::atomic::Ordering::Relaxed);
        println!("GATE retained_backlog_frames_replayed={replayed}");
        tasks.shutdown().await;
        drop(api);
        println!(
            "GATE snapshot_median_ms={normal:.3} target_ms=100 backlog_200000_snapshot_median_ms={backlog:.3} target_ms=100"
        );
        println!(
            "GATE accept_baseline_p99_ms={baseline:.3} accept_observers_p99_ms={loaded:.3} delta_ms={:.3} target_delta_ms=20",
            loaded - baseline
        );
        Arc::try_unwrap(room)
            .unwrap_or_else(|_| panic!("gate room owners"))
            .close()
            .await
            .expect("close");
        assert!(normal < 100.0 && backlog < 100.0, "snapshot gate missed");
        assert!(loaded - baseline <= 20.0, "accept p99 gate missed");
    }
}

#[cfg(test)]
mod history_tests {
    use super::*;
    #[test]
    fn restored_histories_with_identical_timestamps_reject_nonce_and_pruned_anchors() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("mail.db");
        let backup = dir.path().join("backup.db");
        let mut s = Store::open(&path, Config::default()).expect("store");
        s.db.execute(
            "INSERT INTO agents(id,name) VALUES (?, 'a')",
            [json(&AgentId::new()).expect("agent")],
        )
        .expect("agent");
        let to: AgentId =
            s.db.query_row("SELECT id FROM agents", [], |r| json_col(r, 0))
                .expect("to");
        let mut saved = None;
        for n in 1..=150 {
            s.accept(NewMessage {
                from: Sender::Human,
                to,
                body: Body::new("a").expect("body"),
                reply_to: None,
                client_key: ClientKey(format!("key-{n}")),
                act: Act::Inform,
                task: None,
            })
            .expect("accept");
            s.db.execute("UPDATE mail_events SET at='frozen clock'", [])
                .expect("freeze original clock");
            if n == 100 {
                s.db.execute("VACUUM INTO ?", [backup.to_str().expect("path")])
                    .expect("backup");
            }
            if n == 150 {
                saved = Some(
                    serde_json::from_value::<Anchor>(
                        snapshot(&s.db, None).expect("snapshot").1["anchor"].clone(),
                    )
                    .expect("anchor"),
                );
            }
        }
        let anchor = saved.expect("anchor");
        let id: String =
            s.db.query_row(
                "SELECT message_id FROM mail_events WHERE seq=150",
                [],
                |r| r.get(0),
            )
            .expect("id");
        drop(s);
        std::fs::copy(&backup, &path).expect("restore");
        let mut s = Store::open(&path, Config::default()).expect("restored");
        for n in 101..=200 {
            let (_, env) = s
                .accept(NewMessage {
                    from: Sender::Human,
                    to,
                    body: Body::new("b").expect("body"),
                    reply_to: None,
                    client_key: ClientKey(format!("other-{n}")),
                    act: Act::Inform,
                    task: None,
                })
                .expect("accept");
            if n == 150 {
                s.db.execute("UPDATE mail_events SET message_id=? WHERE seq=150", [&id])
                    .expect("identical message id");
                let _ = env;
            }
        }
        // Both histories may use the same clock value. The proof depends only on seq and nonce.
        s.db.execute("UPDATE mail_events SET at='frozen clock'", [])
            .expect("clock");
        assert!(
            !snapshot(&s.db, Some(anchor.clone()))
                .expect("restored resume")
                .1
                .is_null()
        );
        assert!(
            !snapshot(
                &s.db,
                Some(Anchor {
                    seq: 999,
                    nonce: anchor.nonce
                })
            )
            .expect("ahead")
            .1
            .is_null()
        );
        let current: Anchor =
            serde_json::from_value(snapshot(&s.db, None).expect("snapshot").1["anchor"].clone())
                .expect("anchor");
        s.db.execute("DELETE FROM mail_events WHERE seq<=200", [])
            .expect("wipe");
        assert!(!snapshot(&s.db, Some(current)).expect("wiped").1.is_null());
        // Retention never resets sqlite_sequence, even when a test deletes every row.
        assert_eq!(head(&s.db).expect("head"), 200);
    }
    #[tokio::test]
    async fn rare_filter_continuation_terminates_and_counts_only_candidates() {
        let dir = tempfile::tempdir().expect("dir");
        let room = Mailroom::open(dir.path().join("mail.db"), Config::default())
            .await
            .expect("room");
        room.observe(|s|{
            let agent=json(&AgentId::new())?;s.db.execute("INSERT INTO agents(id,name) VALUES (?,'a')",[&agent])?;
            let tx=s.db.transaction()?;
            for n in 1..=20001 {let id=format!("\"{n:026}\"");tx.execute("INSERT INTO messages(id,sender,client_key,recipient,conversation,hop,act,body,sent_at) VALUES (?,'\"Human\"',?,?,'\"00000000000000000000000001\"',0,'\"Inform\"','body','unused')",params![id,format!("key-{n}"),agent])?;let seq=tx.last_insert_rowid();if n!=20000 {tx.execute("INSERT INTO deliveries(message_id,state,msg_seq) VALUES (?,'queued',?)",params![id,seq])?;}}
            tx.commit()?;Ok(())
        }).await.expect("seed");
        let mut before = None;
        let mut boundaries = Vec::new();
        loop {
            let v = room
                .mail_query(MailQuery {
                    from: Some("missing".into()),
                    before_msg_seq: before,
                    limit: 1000,
                    ..Default::default()
                })
                .await
                .expect("page");
            assert!(v["rows"].as_array().expect("rows").is_empty());
            let next = v["next_before_msg_seq"].as_i64();
            if next.is_none() {
                break;
            }
            if let Some(b) = before {
                assert!(next.expect("next") < b);
            }
            boundaries.push(next);
            before = next;
            assert!(boundaries.len() <= 2);
        }
        assert_eq!(boundaries, vec![Some(10002), Some(2)]);
        room.close().await.expect("close");
    }
}

#[cfg(test)]
mod durability_tests {
    use super::*;
    #[test]
    fn v1_mail_backfills_once_without_events() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("mail.db");
        let db = Connection::open(&path).expect("db");
        db.execute_batch(include_str!("schema.sql"))
            .expect("v1 schema");
        let agent = json(&AgentId::new()).expect("agent");
        let id = json(&MessageId::new()).expect("id");
        db.execute("INSERT INTO agents(id,name) VALUES (?,'a')", [&agent])
            .expect("agent");
        db.execute("INSERT INTO messages(seq,id,sender,client_key,recipient,conversation,hop,act,body,sent_at) VALUES (10,?,'\"Human\"','key',?,'\"00000000000000000000000001\"',0,'\"Inform\"','body','unused')",params![id,agent]).expect("message");
        db.execute(
            "INSERT INTO deliveries(message_id,state) VALUES (?,'queued')",
            [&id],
        )
        .expect("delivery");
        db.execute_batch("PRAGMA user_version=1").expect("version");
        drop(db);
        for _ in 0..2 {
            let s = Store::open(&path, Config::default()).expect("migrate");
            assert_eq!(head(&s.db).expect("head"), 0);
            let (seq, n): (i64, i64) =
                s.db.query_row(
                    "SELECT msg_seq,(SELECT n FROM mail_edges) FROM deliveries",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .expect("backfill");
            assert_eq!((seq, n), (10, 1));
            assert!(snapshot(&s.db, None).expect("snapshot").1["anchor"].is_null());
        }
    }
    #[test]
    fn retention_is_bounded_and_sequence_does_not_rewind() {
        let dir = tempfile::tempdir().expect("dir");
        let mut s = Store::open(&dir.path().join("mail.db"), Config::default()).expect("store");
        s.db.execute_batch("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<70000) INSERT INTO mail_events(nonce,message_id,state,turn_started,attempts,at) SELECT lower(hex(randomblob(16))),'id','queued',0,0,'at' FROM n").expect("seed events");
        prune(&mut s.db).expect("prune");
        assert_eq!(
            s.db.query_row("SELECT COUNT(*) FROM mail_events", [], |r| r
                .get::<_, i64>(0))
                .expect("count"),
            70000 - 4096
        );
        prune(&mut s.db).expect("second prune");
        assert_eq!(
            s.db.query_row("SELECT COUNT(*) FROM mail_events", [], |r| r
                .get::<_, i64>(0))
                .expect("count"),
            65536
        );
        assert_eq!(head(&s.db).expect("head"), 70000);
    }
    #[tokio::test]
    async fn rejected_multi_delivery_transaction_leaves_no_event_or_edge_change() {
        let dir = tempfile::tempdir().expect("dir");
        let room = Mailroom::open(dir.path().join("mail.db"), Config::default())
            .await
            .expect("room");
        let to = room
            .register(AgentName::parse("a").expect("name"))
            .await
            .expect("agent");
        let mut ids = Vec::new();
        for n in 0..2 {
            ids.push(
                room.accept(NewMessage {
                    from: Sender::Human,
                    to,
                    body: Body::new("body").expect("body"),
                    reply_to: None,
                    client_key: ClientKey(format!("key-{n}")),
                    act: Act::Inform,
                    task: None,
                })
                .await
                .expect("accept")
                .1
                .id(),
            );
        }
        room.mark_offered(&[ids[1]], Incarnation(1))
            .await
            .expect("offer");
        let before = room.mail_snapshot(None).await.expect("snapshot");
        assert!(room.mark_offered(&ids, Incarnation(2)).await.is_err());
        let after = room.mail_snapshot(None).await.expect("snapshot");
        assert_eq!(before, after, "rolled-back trigger work leaked");
        room.close().await.expect("close");
    }
}
