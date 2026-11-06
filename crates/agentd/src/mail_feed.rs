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

pub(super) fn prune(db: &mut Connection) -> Result<usize> {
    let tx = db.transaction()?;
    let deleted=tx.execute("DELETE FROM mail_events WHERE seq IN (SELECT seq FROM mail_events WHERE seq < (SELECT seq FROM mail_events ORDER BY seq DESC LIMIT 1 OFFSET 65535) ORDER BY seq LIMIT 4096)", [])?;
    tx.commit()?;
    Ok(deleted)
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
    let mut states = filter.states.clone();
    states.sort_unstable();
    states.dedup();
    if states.iter().any(|s| {
        !["queued", "offered", "consumed", "uncertain", "held", "dead"].contains(&s.as_str())
    }) {
        return Err(Error::Rejected("invalid mail state".into()));
    }
    #[cfg(test)]
    trace::label(if filter.from.as_deref() == Some("no such sender") {
        "mail_rare_filter"
    } else if states == ["held", "uncertain"] {
        "mail_states_held_uncertain"
    } else if states == ["offered", "queued"] {
        "mail_states_queued_offered"
    } else {
        "mail_filtered"
    });
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
    if from.is_none() && to.is_none() {
        let (rows, next) = unfiltered_page(db, &states, before, limit)?;
        return Ok(query_result(rows, next));
    }
    let sql = format!(
        "WITH candidates AS MATERIALIZED ({candidates}) SELECT {META},{DELIVERY} FROM candidates c JOIN messages m ON m.seq=c.seq JOIN deliveries d ON d.message_id=m.id WHERE (?2 IS NULL OR m.sender=?2) AND (?3 IS NULL OR m.recipient=?3) ORDER BY m.seq DESC LIMIT {limit}"
    );
    #[cfg(test)]
    let prepare = trace::query("prepare");
    let mut stmt = db.prepare(&sql)?;
    #[cfg(test)]
    drop(prepare);
    #[cfg(test)]
    let result = if trace::detailed() {
        trace::read_rows(db, &mut stmt, params![before, from, to], metadata, "rows")?
    } else {
        stmt.query_map(params![before, from, to], metadata)?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    #[cfg(not(test))]
    let result = stmt
        .query_map(params![before, from, to], metadata)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    {
        #[cfg(test)]
        let _cleanup = trace::query("cleanup");
        drop(stmt);
    }
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
    Ok(query_result(result, next))
}

fn query_result(result: Vec<Value>, next: Option<i64>) -> Value {
    #[cfg(test)]
    let _assembly = trace::query("mail_result_assembly");
    let mut response = json!({"next_before_msg_seq":next});
    response["rows"] = Value::Array(result);
    response
}

fn unfiltered_page(
    db: &Connection,
    states: &[String],
    before: i64,
    limit: usize,
) -> Result<(Vec<Value>, Option<i64>)> {
    // Merge the same bounded, ordered streams as the filtered query. This
    // avoids joining and sorting metadata for candidates the page cannot use.
    #[cfg(test)]
    let prepare = trace::query("prepare");
    let sql = if states.is_empty() {
        vec!["SELECT seq FROM messages WHERE seq<?1 ORDER BY seq DESC LIMIT 10000".to_owned()]
    } else {
        states.iter().map(|state| format!("SELECT msg_seq FROM deliveries WHERE state='{state}' AND msg_seq<?1 ORDER BY msg_seq DESC LIMIT 10000")).collect()
    };
    let mut sequences = sql
        .iter()
        .map(|sql| db.prepare(sql))
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut lookup = db.prepare(&format!("SELECT {META},{DELIVERY} FROM messages m JOIN deliveries d ON d.message_id=m.id WHERE m.seq=?"))?;
    #[cfg(test)]
    drop(prepare);
    #[cfg(test)]
    let mut profile = trace::SqlProbe::new("rows");
    #[cfg(test)]
    let stamp = profile.stamp();
    let mut cursors = sequences
        .iter_mut()
        .map(|s| s.query([before]))
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut heads = std::collections::BinaryHeap::new();
    for (index, cursor) in cursors.iter_mut().enumerate() {
        if let Some(row) = cursor.next()? {
            heads.push((row.get::<_, i64>(0)?, index));
        }
    }
    #[cfg(test)]
    profile.sql_done(stamp);
    let mut rows = Vec::new();
    let mut examined = 0;
    let mut last = None;
    while let Some((seq, index)) = heads.pop() {
        // UNION deduplicates multiple branches. A single branch keeps its
        // multiplicity, including repeated msg_seq in a manually edited DB.
        if states.len() <= 1 || last != Some(seq) {
            examined += 1;
            last = Some(seq);
            #[cfg(test)]
            let stamp = profile.stamp();
            let mut found = lookup.query([seq])?;
            let row = found.next()?;
            #[cfg(test)]
            profile.sql_done(stamp);
            if let Some(row) = row {
                #[cfg(test)]
                let stamp = profile.stamp();
                rows.push(metadata(row)?);
                #[cfg(test)]
                profile.row_done(stamp);
            }
            #[cfg(test)]
            let stamp = profile.stamp();
            drop(found);
            #[cfg(test)]
            profile.cleanup_done(stamp);
            if rows.len() == limit || examined == 10000 {
                break;
            }
        }
        #[cfg(test)]
        let stamp = profile.stamp();
        if let Some(row) = cursors[index].next()? {
            heads.push((row.get::<_, i64>(0)?, index));
        }
        #[cfg(test)]
        profile.sql_done(stamp);
    }
    #[cfg(test)]
    let stamp = profile.stamp();
    drop(cursors);
    #[cfg(test)]
    profile.cleanup_done(stamp);
    #[cfg(test)]
    profile.inspect(
        db,
        &sequences.iter().chain([&lookup]).collect::<Vec<_>>(),
        rows.len(),
        examined,
    )?;
    #[cfg(test)]
    let cleanup = trace::query("cleanup");
    drop(lookup);
    drop(sequences);
    #[cfg(test)]
    drop(cleanup);
    #[cfg(test)]
    profile.finish();
    let next = if rows.len() == limit {
        rows.last().and_then(|r| r["msg_seq"].as_i64())
    } else if examined == 10000 {
        last
    } else {
        None
    };
    Ok((rows, next))
}

fn window(db: &Connection, states: &[&str]) -> Result<(Vec<Value>, bool)> {
    let branches = states.iter().map(|s| format!("SELECT msg_seq FROM (SELECT msg_seq FROM deliveries WHERE state='{s}' ORDER BY msg_seq DESC LIMIT 1001)")).collect::<Vec<_>>().join(" UNION ALL ");
    let sql = format!(
        "WITH candidates AS MATERIALIZED (SELECT msg_seq FROM ({branches}) ORDER BY msg_seq DESC LIMIT 1001) SELECT {META},{DELIVERY} FROM candidates c JOIN messages m ON m.seq=c.msg_seq JOIN deliveries d ON d.message_id=m.id ORDER BY m.seq DESC"
    );
    let mut stmt = db.prepare(&sql)?;
    #[cfg(test)]
    let mut rows = if trace::detailed() {
        trace::read_rows(
            db,
            &mut stmt,
            [],
            metadata,
            if states == ["held", "uncertain"] {
                "stuck_rows"
            } else {
                "moving_rows"
            },
        )?
    } else {
        stmt.query_map([], metadata)?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    #[cfg(not(test))]
    let mut rows = stmt
        .query_map([], metadata)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let truncated = rows.len() > 1000;
    rows.truncate(1000);
    Ok((rows, truncated))
}

pub(crate) fn snapshot(db: &Connection, resume: Option<Anchor>) -> Result<(i64, Value)> {
    #[cfg(test)]
    trace::label(if resume.is_some() {
        "resume_validation"
    } else {
        "full_snapshot"
    });
    if let Some(a) = resume {
        #[cfg(test)]
        let _query = trace::query("resume_anchor");
        let valid: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM mail_events WHERE seq=? AND nonce=?)",
            params![a.seq, a.nonce],
            |r| r.get(0),
        )?;
        if valid {
            return Ok((a.seq, Value::Null));
        }
    }
    #[cfg(test)]
    trace::label("full_snapshot");
    #[cfg(test)]
    let timing = trace::query("head");
    let cursor = head(db)?;
    #[cfg(test)]
    drop(timing);
    #[cfg(test)]
    let timing = trace::query("anchor");
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
    #[cfg(test)]
    drop(timing);
    #[cfg(test)]
    let timing = trace::query("edges");
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
    #[cfg(test)]
    drop(timing);
    #[cfg(test)]
    let timing = trace::query("stuck");
    let (stuck, stuck_truncated) = window(db, &["held", "uncertain"])?;
    #[cfg(test)]
    drop(timing);
    #[cfg(test)]
    let timing = trace::query("moving");
    let (moving, moving_truncated) = window(db, &["queued", "offered"])?;
    #[cfg(test)]
    drop(timing);
    #[cfg(test)]
    let _assembly = trace::query("snapshot_result_assembly");
    // json! serializes Value expressions by reference, copying every row. Move
    // these arrays into the result so large metadata lists have one owner.
    let mut result = json!({"anchor":anchor,"edges_truncated":truncated,"stuck_truncated":stuck_truncated,"moving_truncated":moving_truncated});
    result["edges"] = Value::Array(edges);
    result["stuck"] = Value::Array(stuck);
    result["moving"] = Value::Array(moving);
    Ok((cursor, result))
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
        self.observe(move |s| {
            #[cfg(test)]
            trace::label(if filter.from.as_deref() == Some("no such sender") {
                "mail_rare_filter"
            } else {
                "mail_other"
            });
            query(&s.db, &filter)
        })
        .await
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
            let view = query(
                &s.db,
                &MailQuery {
                    states: vec!["queued".into()],
                    limit: 1000,
                    ..Default::default()
                },
            )?;
            assert_eq!(
                view["rows"], snap["moving"],
                "snapshot and mail must preserve identical complete rows"
            );
            assert_eq!(view["next_before_msg_seq"], snap["moving"][999]["msg_seq"]);
            assert_eq!(snap["stuck"], json!([]));
            assert_eq!(snap["stuck_truncated"], false);
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
    struct Load {
        reconnects: [std::sync::atomic::AtomicUsize; 7],
        timeout_attempts: [std::sync::atomic::AtomicUsize; 7],
        views: [std::sync::atomic::AtomicUsize; 8],
        rare: std::sync::atomic::AtomicUsize,
        lists: std::sync::atomic::AtomicUsize,
        replayed: std::sync::atomic::AtomicUsize,
    }
    impl Load {
        fn timed_out(&self, client: usize) {
            self.timeout_attempts[client].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        fn counts(&self) -> Vec<usize> {
            use std::sync::atomic::Ordering::Relaxed;
            self.reconnects
                .iter()
                .chain(&self.views)
                .chain([&self.rare, &self.lists, &self.replayed])
                .map(|n| n.load(Relaxed))
                .collect()
        }
        fn ready(&self) -> bool {
            use std::sync::atomic::Ordering::Relaxed;
            self.reconnects.iter().all(|n| n.load(Relaxed) >= 2)
                && self.views.iter().all(|n| n.load(Relaxed) >= 2)
                && self.rare.load(Relaxed) >= 20
                && self.lists.load(Relaxed) >= 20
                && self.replayed.load(Relaxed) >= 8192
        }
    }
    fn check_iterations(counts: &[usize]) -> std::result::Result<(), String> {
        let minimums = [vec![2; 7], vec![1; 8], vec![20, 20, 256]].concat();
        if counts.len() != minimums.len() {
            return Err("missing workload counts".into());
        }
        for (class, (actual, minimum)) in counts.iter().zip(minimums).enumerate() {
            if *actual < minimum {
                return Err(format!(
                    "workload class {class} absent from sampled accepts: {actual} < {minimum}"
                ));
            }
        }
        Ok(())
    }
    struct Serialized {
        load: Arc<Load>,
        replay: bool,
        client: Option<usize>,
        completed_full: bool,
    }
    #[async_trait::async_trait]
    impl events::Sink for Serialized {
        async fn send(&mut self, value: Value) -> crate::surfaces::Result<()> {
            if self.replay && value["event"] == "snapshot" && !value["mail"].is_null() {
                return Err("required retained replay lost its anchor".into());
            }
            let bytes = serde_json::to_vec(&value)?;
            std::hint::black_box(bytes);
            if self.replay && value["event"] == "mail" {
                self.load
                    .replayed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            tokio::task::yield_now().await;
            Ok(())
        }
        async fn send_snapshot(
            &mut self,
            json: String,
            full_mail: bool,
        ) -> crate::surfaces::Result<()> {
            if self.replay && full_mail {
                return Err("required retained replay lost its anchor".into());
            }
            std::hint::black_box(json);
            tokio::task::yield_now().await;
            if full_mail
                && !self.completed_full
                && let Some(client) = self.client
            {
                self.load.reconnects[client].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.completed_full = true;
            }
            Ok(())
        }
    }
    async fn drain_workloads(
        tasks: &mut tokio::task::JoinSet<crate::surfaces::Result<()>>,
    ) -> crate::surfaces::Result<usize> {
        let mut completed = 0;
        while let Some(result) = tasks.join_next().await {
            result??;
            completed += 1;
        }
        Ok(completed)
    }
    #[tokio::test]
    async fn gate_rejects_absent_load_and_failed_generators() {
        use std::sync::atomic::Ordering::Relaxed;
        let load = Load::default();
        assert!(!load.ready());
        for n in load.reconnects.iter().chain(&load.views) {
            n.store(2, Relaxed);
        }
        load.rare.store(20, Relaxed);
        load.lists.store(20, Relaxed);
        load.replayed.store(8192, Relaxed);
        assert!(load.ready());
        for counter in load.reconnects.iter().chain(&load.views).chain([
            &load.rare,
            &load.lists,
            &load.replayed,
        ]) {
            let previous = counter.swap(0, Relaxed);
            assert!(!load.ready(), "gate accepted absent workload class");
            counter.store(previous, Relaxed);
        }
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async { Err("induced workload error".into()) });
        assert!(
            drain_workloads(&mut tasks).await.is_err(),
            "gate ignored workload error"
        );
        tasks.spawn(async {
            panic!("induced generator panic");
        });
        assert!(
            drain_workloads(&mut tasks).await.is_err(),
            "gate ignored generator panic"
        );
    }
    #[tokio::test]
    async fn gate_rejects_reconnector_without_completed_full_snapshots() {
        use std::sync::atomic::Ordering::Relaxed;
        let load = Arc::new(Load::default());
        for value in &load.views {
            value.store(2, Relaxed);
        }
        load.rare.store(20, Relaxed);
        load.lists.store(20, Relaxed);
        load.replayed.store(8192, Relaxed);
        for client in 0..7 {
            for _ in 0..2 {
                let mut sink = Serialized {
                    load: load.clone(),
                    replay: false,
                    client: Some(client),
                    completed_full: false,
                };
                // Client 3 receives only volatile snapshots. Time still expires.
                events::Sink::send_snapshot(
                    &mut sink,
                    "{\"event\":\"snapshot\"}".into(),
                    client != 3,
                )
                .await
                .expect("snapshot sink");
                load.timed_out(client);
            }
        }
        assert!(
            check_iterations(&load.counts()).is_err(),
            "gate accepted reconnect timeouts without client 3 full snapshots"
        );
        assert_eq!(load.reconnects[3].load(Relaxed), 0);
        for _ in 0..2 {
            let mut sink = Serialized {
                load: load.clone(),
                replay: false,
                client: Some(3),
                completed_full: false,
            };
            events::Sink::send_snapshot(&mut sink, "{\"event\":\"snapshot\"}".into(), true)
                .await
                .expect("recovered snapshot");
        }
        check_iterations(&load.counts()).expect("all actual snapshots completed");
        assert!(load.ready());
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum GateMode {
        Production,
        RamDisk,
    }
    impl GateMode {
        fn from_marker(marker: Option<&std::ffi::OsStr>) -> std::result::Result<Self, String> {
            match marker {
                None => Ok(Self::Production),
                Some(value) if value == "1" => Ok(Self::RamDisk),
                Some(_) => Err("M5_GATE_RAM_DISK must be 1 or unset".into()),
            }
        }
        fn label(self) -> &'static str {
            match self {
                Self::Production => "production",
                Self::RamDisk => "ram-disk-diagnostic",
            }
        }
    }
    fn gate_failures(
        mode: GateMode,
        queue: f64,
        _delta: f64,
        services: &std::collections::BTreeMap<String, trace::Service>,
    ) -> Vec<String> {
        let mut failures = Vec::new();
        if !queue.is_finite() || queue > 20.0 {
            failures.push(format!(
                "loaded worker queue p99 {queue:.6} ms exceeds 20 ms"
            ));
        }
        for class in trace::OBSERVER_CLASSES {
            if !services.contains_key(class) {
                failures.push(format!("observer class {class} has no samples"));
            }
        }
        for (class, service) in services {
            if service.count < 100 {
                failures.push(format!(
                    "observer class {class} has {} samples; requires 100",
                    service.count
                ));
            }
            if mode == GateMode::Production
                && (!service.p99_ms.is_finite() || service.p99_ms > 20.0)
            {
                failures.push(format!(
                    "observer class {class} whole-job p99 {:.6} ms exceeds 20 ms",
                    service.p99_ms
                ));
            }
        }
        failures
    }
    #[test]
    fn v8_gate_requires_absolute_queue_and_whole_observer_jobs() {
        assert_eq!(
            GateMode::from_marker(None).expect("default"),
            GateMode::Production
        );
        assert_eq!(
            GateMode::from_marker(Some(std::ffi::OsStr::new("1"))).expect("explicit RAM"),
            GateMode::RamDisk
        );
        assert!(GateMode::from_marker(Some(std::ffi::OsStr::new("0"))).is_err());
        let services = trace::OBSERVER_CLASSES
            .into_iter()
            .map(|class| {
                (
                    class.to_owned(),
                    trace::Service {
                        count: 100,
                        p99_ms: 20.0,
                        max_ms: 999.0,
                        ..Default::default()
                    },
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        assert!(
            !gate_failures(GateMode::Production, 20.001, -100.0, &services).is_empty(),
            "inflated baseline hid queue p99 above 20 ms"
        );
        assert!(
            gate_failures(GateMode::Production, 20.0, 999.0, &services).is_empty(),
            "reported delta or maximum decided the pass rule"
        );
        for class in trace::OBSERVER_CLASSES {
            let mut short = services.clone();
            short.get_mut(class).expect("class").count = 99;
            assert!(
                !gate_failures(GateMode::Production, 1.0, 0.0, &short).is_empty(),
                "production accepted fewer than 100 {class} jobs"
            );
            assert!(
                !gate_failures(GateMode::RamDisk, 1.0, 0.0, &short).is_empty(),
                "RAM diagnostics lack samples for {class}"
            );
            let mut slow = services.clone();
            slow.get_mut(class).expect("class").p99_ms = 20.001;
            assert!(
                !gate_failures(GateMode::Production, 1.0, 0.0, &slow).is_empty(),
                "production accepted slow {class}"
            );
            assert!(
                gate_failures(GateMode::RamDisk, 1.0, 0.0, &slow).is_empty(),
                "RAM service tail was treated as blocker"
            );
        }
        assert!(!gate_failures(GateMode::RamDisk, 20.001, 0.0, &services).is_empty());
    }
    fn percentile(mut times: Vec<f64>) -> f64 {
        times.sort_by(f64::total_cmp);
        times[(times.len() * 99 / 100).min(times.len() - 1)]
    }
    #[derive(Clone, Copy)]
    struct AcceptTiming {
        id: usize,
        submitted: Instant,
        began: Instant,
        resumed: Instant,
        queue_ms: f64,
        job_ms: f64,
        resume_ms: f64,
    }
    impl AcceptTiming {
        fn total_ms(self) -> f64 {
            self.queue_ms + self.job_ms + self.resume_ms
        }
    }
    struct AcceptStats {
        samples: Vec<AcceptTiming>,
        p99: f64,
        queue_p99: f64,
        job_p99: f64,
        resume_p99: f64,
        at_p99: AcceptTiming,
    }
    impl AcceptStats {
        fn print(&self, lane: &str) {
            println!(
                "GATE accept_attribution lane={lane} samples={} total_p99_ms={:.6} queue_p99_ms={:.6} job_p99_ms={:.6} resume_p99_ms={:.6} at_total_p99_queue_ms={:.6} at_total_p99_job_ms={:.6} at_total_p99_resume_ms={:.6}",
                self.samples.len(),
                self.p99,
                self.queue_p99,
                self.job_p99,
                self.resume_p99,
                self.at_p99.queue_ms,
                self.at_p99.job_ms,
                self.at_p99.resume_ms
            );
        }
    }
    async fn sample(
        room: &Mailroom,
        to: AgentId,
        tag: &str,
        progress: Option<&trace::Progress>,
    ) -> AcceptStats {
        if let Some(progress) = progress {
            progress.begin_window();
        }
        let mut times = Vec::new();
        let mut previous_resume = None;
        let began_window = Instant::now();
        for n in 0.. {
            let message = NewMessage {
                from: Sender::Human,
                to,
                body: Body::new("gate delivery").expect("body"),
                reply_to: None,
                client_key: ClientKey(format!("{tag}-{n}")),
                act: Act::Inform,
                task: None,
            };
            let submitted = Instant::now();
            assert!(
                previous_resume.is_none_or(|last| last <= submitted),
                "accepts overlap"
            );
            let (_accepted, began, finished) = room
                .call(move |store| {
                    let began = Instant::now();
                    let accepted = store.accept(message)?;
                    let finished = Instant::now();
                    Ok((accepted, began, finished))
                })
                .await
                .expect("real accept");
            let resumed = Instant::now();
            previous_resume = Some(resumed);
            times.push(AcceptTiming {
                id: n,
                submitted,
                began,
                resumed,
                queue_ms: began.duration_since(submitted).as_secs_f64() * 1000.0,
                job_ms: finished.duration_since(began).as_secs_f64() * 1000.0,
                resume_ms: resumed.duration_since(finished).as_secs_f64() * 1000.0,
            });
            if n + 1 >= 2048 && progress.is_none_or(|p| p.enough()) {
                break;
            }
            if progress.is_some() && began_window.elapsed() >= Duration::from_secs(120) {
                break;
            }
        }
        println!(
            "GATE sample_window lane={tag} samples={} elapsed_ms={:.6} extended={} sufficient_observer_samples={}",
            times.len(),
            began_window.elapsed().as_secs_f64() * 1000.0,
            times.len() > 2048,
            progress.is_none_or(|p| p.enough())
        );
        times.sort_by(|a, b| a.total_ms().total_cmp(&b.total_ms()));
        let at_p99 = times[times.len() * 99 / 100];
        let stats = AcceptStats {
            p99: at_p99.total_ms(),
            queue_p99: percentile(times.iter().map(|s| s.queue_ms).collect()),
            job_p99: percentile(times.iter().map(|s| s.job_ms).collect()),
            resume_p99: percentile(times.iter().map(|s| s.resume_ms).collect()),
            at_p99,
            samples: times,
        };
        stats.print(tag);
        stats
    }
    fn trace_samples(stats: &AcceptStats) -> Vec<(usize, Instant, Instant, Instant, f64, f64)> {
        stats
            .samples
            .iter()
            .map(|s| {
                (
                    s.id,
                    s.submitted,
                    s.began,
                    s.resumed,
                    s.total_ms(),
                    s.queue_ms,
                )
            })
            .collect()
    }
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "full-scale disk and latency measurement; run explicitly with --ignored --nocapture"]
    async fn m5_cost_gate() {
        let mode = GateMode::from_marker(std::env::var_os("M5_GATE_RAM_DISK").as_deref())
            .expect("gate mode");
        println!(
            "GATE pass_rule=v8 mode={} queue_target_ms=20 service_target_ms=20 service_min_samples=100 minimum_accept_samples=2048 max_loaded_window_seconds=120",
            mode.label()
        );
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
        async fn snapshot_ms(api: &ControlApi) -> f64 {
            let mut values = Vec::new();
            for _ in 0..7 {
                let start = Instant::now();
                let frame = events::snapshot(api, None, None, &mut HashMap::new())
                    .await
                    .expect("assembled snapshot");
                std::hint::black_box(frame.json);
                values.push(start.elapsed().as_secs_f64() * 1000.0);
            }
            values.sort_by(f64::total_cmp);
            values[3]
        }
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
        let normal = snapshot_ms(&api).await;
        let to = agents[0];
        let recipient = json(&to).expect("recipient");
        room.call(move|s|{let tx=s.db.transaction()?;tx.execute("UPDATE deliveries SET state='queued' WHERE msg_seq<=200000",[])?;tx.execute("UPDATE messages SET recipient=? WHERE seq<=200000",[recipient])?;
            // The fixture changes immutable metadata only before measurement, then rebuilds its aggregate.
            tx.execute("DELETE FROM mail_edges",[])?;tx.execute("INSERT INTO mail_edges SELECT m.sender,m.recipient,d.state,COUNT(*) FROM messages m JOIN deliveries d ON d.message_id=m.id GROUP BY m.sender,m.recipient,d.state",[])?;tx.commit()?;while s.db.query_row("SELECT COUNT(*) FROM mail_events",[],|r|r.get::<_,i64>(0))?>65536 {prune(&mut s.db)?;}Ok(())}).await.expect("dormant backlog");
        let backlog = snapshot_ms(&api).await;
        room.call(|_| Ok(trace::start()))
            .await
            .expect("start baseline trace");
        let baseline = sample(&room, to, "baseline", None).await;
        let records = room
            .call(|_| Ok(trace::take()))
            .await
            .expect("baseline trace");
        trace::report(
            "baseline",
            &records,
            trace_samples(&baseline),
            baseline.p99,
            baseline.queue_p99,
        );

        room.call(|s| prune(&mut s.db))
            .await
            .expect("prune baseline before replay anchor");
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
        use std::sync::atomic::Ordering::Relaxed;
        let mut tasks = tokio::task::JoinSet::<crate::surfaces::Result<()>>::new();
        let cancel = tokio_util::sync::CancellationToken::new();
        let load = Arc::new(Load::default());
        for n in 0_usize..8 {
            let api = api.clone();
            let anchor = anchor.clone();
            let load = load.clone();
            let cancel = cancel.clone();
            tasks.spawn(async move {
                let _permit=events::subscribe()?;
                loop {
                    let mut sink=Serialized{load:load.clone(),replay:n==0,client:(n>0).then_some(n.saturating_sub(1)),completed_full:false};
                    if n==0 {return tokio::select!{_=cancel.cancelled()=>Ok(()),r=events::feed(&api,&mut sink,Some(anchor.clone()))=>{r?;Err("backlog feed ended unexpectedly".into())}};}
                    tokio::select! {
                        _=cancel.cancelled()=>return Ok(()),
                        r=tokio::time::timeout(Duration::from_millis(250),events::feed(&api,&mut sink,None))=>{
                            if let Ok(result)=r {result?;return Err("reconnecting feed ended unexpectedly".into());}
                            load.timed_out(n-1);
                        }
                    }
                }
            });
        }
        for n in 0..8 {
            let room = room.clone();
            let control_path = api.control_path.clone();
            let load = load.clone();
            let cancel = cancel.clone();
            tasks.spawn(async move {
                loop {
                    let query = async {
                        for states in [
                            vec!["held".into(), "uncertain".into()],
                            vec!["queued".into(), "offered".into()],
                        ] {
                            std::hint::black_box(
                                events::mail_response(
                                    &room,
                                    MailQuery {
                                        states,
                                        limit: 1000,
                                        ..Default::default()
                                    },
                                    control_path.clone(),
                                )
                                .await?,
                            );
                        }
                        load.views[n].fetch_add(1, Relaxed);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
                    };
                    tokio::select! {_=cancel.cancelled()=>return Ok(()),r=query=>r?}
                }
            });
        }
        for rare in [false, true] {
            let room = room.clone();
            let control_path = api.control_path.clone();
            let load = load.clone();
            let cancel = cancel.clone();
            tasks.spawn(async move {
                loop {
                    let query = async {
                        if rare {
                            std::hint::black_box(
                                events::mail_response(
                                    &room,
                                    MailQuery {
                                        from: Some("no such sender".into()),
                                        limit: 1000,
                                        ..Default::default()
                                    },
                                    control_path.clone(),
                                )
                                .await?,
                            );
                            load.rare.fetch_add(1, Relaxed);
                        } else {
                            room.observed_agents().await?;
                            load.lists.fetch_add(1, Relaxed);
                        }
                        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
                    };
                    tokio::select! {_=cancel.cancelled()=>return Ok(()),r=query=>r?}
                }
            });
        }
        tokio::time::timeout(Duration::from_secs(30), async {
            while !load.ready() {
                if let Some(result) = tasks.try_join_next() {
                    result.expect("workload panicked").expect("workload failed");
                    panic!("workload ended before measurement");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("all workload classes must be active");
        let first_head=room.call(|s| {
            s.db.execute("UPDATE deliveries SET attempts=attempts+1 WHERE msg_seq BETWEEN 200001 AND 206000",[])?;
            let head=head(&s.db)?;s.last_prune=head-1023;s.prunes.clear();Ok(head)
        }).await.expect("stage real delivery events for sampled pruning");
        let progress = room
            .call(|_| Ok(trace::start()))
            .await
            .expect("start loaded trace");
        let before_counts = load.counts();
        let loaded = sample(&room, to, "loaded", Some(&progress)).await;
        let during_counts = load
            .counts()
            .into_iter()
            .zip(before_counts)
            .map(|(after, before)| after - before)
            .collect::<Vec<_>>();
        let records = room
            .call(|_| Ok(trace::take()))
            .await
            .expect("worker trace");
        let services = trace::report(
            "loaded",
            &records,
            trace_samples(&loaded),
            loaded.p99,
            loaded.queue_p99,
        );
        println!("GATE iterations_inside_accept_window={during_counts:?}");
        println!(
            "GATE completed_full_snapshots_inside_window={:?}",
            &during_counts[..7]
        );
        let (last_head, prunes) = room
            .observe(|s| Ok((head(&s.db)?, s.prunes.clone())))
            .await
            .expect("sampled pruning evidence");
        let sampled_prunes = prunes
            .into_iter()
            .filter(|(seq, _)| *seq > first_head && *seq <= last_head)
            .collect::<Vec<_>>();
        println!(
            "GATE sampled_accept_seq_window={first_head}..{last_head} prunes={sampled_prunes:?}"
        );
        assert!(
            sampled_prunes.iter().any(|(_, deleted)| *deleted >= 4000),
            "multi-thousand-row prune did not land in accept samples"
        );
        tokio::time::timeout(Duration::from_secs(60), async {
            while load.replayed.load(Relaxed) < 65535 {
                if let Some(result) = tasks.try_join_next() {
                    result.expect("workload panicked").expect("workload failed");
                    panic!("workload ended during replay");
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("full retained replay completes under observer load");
        cancel.cancel();
        let completed = drain_workloads(&mut tasks)
            .await
            .expect("every workload task must succeed");
        assert_eq!(completed, 18, "every workload must finish cleanly");
        assert!(load.ready(), "minimum workload iterations missing");
        check_iterations(&during_counts)
            .expect("all workloads must complete inside sampled accepts");
        println!(
            "GATE current_thread=true snapshot_includes_serialization=true baseline_accept_samples={} loaded_accept_samples={} retained_backlog_frames_replayed={} reconnect_full_snapshots={:?} view_iterations={:?} rare_queries={} list_queries={} checked_workload_results={completed}",
            baseline.samples.len(),
            loaded.samples.len(),
            load.replayed.load(Relaxed),
            load.reconnects
                .iter()
                .map(|n| n.load(Relaxed))
                .collect::<Vec<_>>(),
            load.views
                .iter()
                .map(|n| n.load(Relaxed))
                .collect::<Vec<_>>(),
            load.rare.load(Relaxed),
            load.lists.load(Relaxed)
        );
        println!(
            "GATE reconnect_timeout_attempts={:?}",
            load.timeout_attempts
                .iter()
                .map(|n| n.load(Relaxed))
                .collect::<Vec<_>>()
        );
        drop(api);
        println!(
            "GATE snapshot_median_ms={normal:.3} target_ms=100 backlog_200000_snapshot_median_ms={backlog:.3} target_ms=100"
        );
        println!(
            "GATE accept_baseline_p99_ms={:.3} accept_observers_p99_ms={:.3} delta_ms={:.3} loaded_queue_p99_ms={:.6} queue_target_ms=20",
            baseline.p99,
            loaded.p99,
            loaded.p99 - baseline.p99,
            loaded.queue_p99
        );
        Arc::try_unwrap(room)
            .unwrap_or_else(|_| panic!("gate room owners"))
            .close()
            .await
            .expect("close");
        assert!(normal < 100.0 && backlog < 100.0, "snapshot gate missed");
        let failures = gate_failures(mode, loaded.queue_p99, loaded.p99 - baseline.p99, &services);
        assert!(
            failures.is_empty(),
            "v8 gate missed: {}",
            failures.join("; ")
        );
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

#[cfg(test)]
mod maintenance_tests {
    use super::*;
    #[tokio::test]
    async fn failed_prune_keeps_worker_alive_and_retries() {
        let dir = tempfile::tempdir().expect("dir");
        let room = Mailroom::open(dir.path().join("mail.db"), Config::default())
            .await
            .expect("room");
        let to = room
            .register(AgentName::parse("a").expect("name"))
            .await
            .expect("agent");
        room.call(|s|{s.db.execute_batch("CREATE TRIGGER fail_prune BEFORE DELETE ON mail_events BEGIN SELECT RAISE(FAIL,'induced prune failure'); END; WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<70000) INSERT INTO mail_events(nonce,message_id,state,turn_started,attempts,at) SELECT 'nonce','fixture','queued',0,0,'at' FROM n;")?;Ok(())}).await.expect("induce prune failure");
        room.call(|s| {
            assert_eq!(s.last_prune, 0, "failed prune advanced checkpoint");
            s.db.execute_batch("DROP TRIGGER fail_prune")?;
            Ok(())
        })
        .await
        .expect("worker serves after failed prune");
        room.accept(NewMessage {
            from: Sender::Human,
            to,
            body: Body::new("after prune error").expect("body"),
            reply_to: None,
            client_key: ClientKey("later".into()),
            act: Act::Inform,
            task: None,
        })
        .await
        .expect("later accept survives maintenance failure");
        room.observe(|s| {
            assert_eq!(s.last_prune, 70001);
            Ok(())
        })
        .await
        .expect("successful retry checkpoint");
        room.close().await.expect("close");
    }
}

#[cfg(test)]
mod state_filter_tests {
    use super::*;
    #[test]
    fn repeated_states_have_the_same_query_work_as_one_state() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let dir = tempfile::tempdir().expect("dir");
        let s = Store::open(&dir.path().join("mail.db"), Config::default()).expect("store");
        let steps = Arc::new(AtomicUsize::new(0));
        let count = steps.clone();
        s.db.progress_handler(
            1,
            Some(move || {
                count.fetch_add(1, Ordering::SeqCst);
                false
            }),
        );
        let one = query(
            &s.db,
            &MailQuery {
                states: vec!["queued".into()],
                limit: 1000,
                ..Default::default()
            },
        )
        .expect("single state");
        let single = steps.swap(0, Ordering::SeqCst);
        let many = query(
            &s.db,
            &MailQuery {
                states: vec!["queued".into(); 499],
                limit: 1000,
                ..Default::default()
            },
        )
        .expect("repeated states");
        let repeated = steps.load(Ordering::SeqCst);
        assert_eq!(one, many);
        assert!(
            repeated <= single * 2,
            "duplicate states multiplied query work: one={single}, repeated={repeated}"
        );
    }
}

#[cfg(test)]
mod head_failure_tests {
    use super::*;
    #[tokio::test]
    async fn failed_head_retries_and_wakes_without_another_write() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let dir = tempfile::tempdir().expect("dir");
        let room = Mailroom::open(dir.path().join("mail.db"), Config::default())
            .await
            .expect("room");
        let to = room
            .register(AgentName::parse("a").expect("name"))
            .await
            .expect("agent");
        let denied = Arc::new(AtomicBool::new(true));
        let attempts = Arc::new(AtomicUsize::new(0));
        let flag = denied.clone();
        let count = attempts.clone();
        room.call(move |s| {
            s.db.authorizer(Some(move |ctx: rusqlite::hooks::AuthContext<'_>| {
                if matches!(
                    ctx.action,
                    rusqlite::hooks::AuthAction::Read {
                        table_name: "sqlite_sequence",
                        ..
                    }
                ) {
                    count.fetch_add(1, Ordering::SeqCst);
                    if flag.load(Ordering::SeqCst) {
                        return rusqlite::hooks::Authorization::Deny;
                    }
                }
                rusqlite::hooks::Authorization::Allow
            }));
            Ok(())
        })
        .await
        .expect("install head failures");
        let mut changes = room.changes();
        changes.borrow_and_update();
        room.accept(NewMessage {
            from: Sender::Human,
            to,
            body: Body::new("committed").expect("body"),
            reply_to: None,
            client_key: ClientKey("wake".into()),
            act: Act::Inform,
            task: None,
        })
        .await
        .expect("accept");
        for _ in 0..20 {
            room.observed_agents().await.expect("read-only job");
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(
            (1..=6).contains(&attempts.load(Ordering::SeqCst)),
            "head retry spun on reads"
        );
        denied.store(false, Ordering::SeqCst);
        tokio::time::timeout(std::time::Duration::from_secs(2), changes.changed())
            .await
            .expect("committed mail never woke idle subscriber after head recovery")
            .expect("watch open");
        assert_eq!(*changes.borrow_and_update(), 1);
        room.close().await.expect("close");
    }
    #[tokio::test]
    async fn head_read_failure_does_not_close_worker() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let dir = tempfile::tempdir().expect("dir");
        let room = Mailroom::open(dir.path().join("mail.db"), Config::default())
            .await
            .expect("room");
        let to = room
            .register(AgentName::parse("a").expect("name"))
            .await
            .expect("agent");
        let fail = Arc::new(AtomicBool::new(true));
        let flag = fail.clone();
        room.call(move |s| {
            s.db.authorizer(Some(move |ctx: rusqlite::hooks::AuthContext<'_>| {
                if matches!(
                    ctx.action,
                    rusqlite::hooks::AuthAction::Read {
                        table_name: "sqlite_sequence",
                        ..
                    }
                ) && flag.swap(false, Ordering::SeqCst)
                {
                    rusqlite::hooks::Authorization::Deny
                } else {
                    rusqlite::hooks::Authorization::Allow
                }
            }));
            s.db.execute("UPDATE agents SET epoch=epoch+1", [])?;
            Ok(())
        })
        .await
        .expect("install one-shot head failure");
        room.accept(NewMessage {
            from: Sender::Human,
            to,
            body: Body::new("after head error").expect("body"),
            reply_to: None,
            client_key: ClientKey("head-recovery".into()),
            act: Act::Inform,
            task: None,
        })
        .await
        .expect("later accept survives head failure");
        assert!(!fail.load(Ordering::SeqCst));
        room.close().await.expect("close");
    }
}

#[cfg(test)]
mod query_work_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    fn legacy_page(
        db: &Connection,
        mut states: Vec<String>,
        before: Option<i64>,
        limit: usize,
    ) -> Value {
        states.sort_unstable();
        states.dedup();
        let limit = limit.clamp(1, 1000);
        let before = before.unwrap_or(i64::MAX);
        let candidates = if states.is_empty() {
            "SELECT seq FROM messages WHERE seq<?1 ORDER BY seq DESC LIMIT 10000".to_owned()
        } else {
            let branches=states.iter().map(|s|format!("SELECT msg_seq AS seq FROM (SELECT msg_seq FROM deliveries WHERE state='{s}' AND msg_seq<?1 ORDER BY msg_seq DESC LIMIT 10000)")).collect::<Vec<_>>().join(" UNION ");
            format!("SELECT seq FROM ({branches}) ORDER BY seq DESC LIMIT 10000")
        };
        let sql = format!(
            "WITH candidates AS MATERIALIZED ({candidates}) SELECT {META},{DELIVERY} FROM candidates c JOIN messages m ON m.seq=c.seq JOIN deliveries d ON d.message_id=m.id WHERE (?2 IS NULL OR m.sender=?2) AND (?3 IS NULL OR m.recipient=?3) ORDER BY m.seq DESC LIMIT {limit}"
        );
        let rows = db
            .prepare(&sql)
            .expect("legacy")
            .query_map(
                params![before, Option::<String>::None, Option::<String>::None],
                metadata,
            )
            .expect("rows")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect");
        let next = if rows.len() == limit {
            rows.last().and_then(|r| r["msg_seq"].as_i64())
        } else {
            let (n, last): (usize, Option<i64>) = db
                .query_row(
                    &format!("SELECT COUNT(*),MIN(seq) FROM ({candidates})"),
                    [before],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .expect("candidate count");
            if n == 10000 { last } else { None }
        };
        query_result(rows, next)
    }
    #[test]
    fn single_state_repeated_sequence_keeps_legacy_multiplicity() {
        let dir = tempfile::tempdir().expect("dir");
        let mut s = Store::open(&dir.path().join("mail.db"), Config::default()).expect("store");
        let to = AgentId::new();
        s.db.execute(
            "INSERT INTO agents(id,name) VALUES (?,'ties')",
            [json(&to).expect("id")],
        )
        .expect("agent");
        for n in 0..3 {
            s.accept(NewMessage {
                from: Sender::Human,
                to,
                body: Body::new("body").expect("body"),
                reply_to: None,
                client_key: ClientKey(format!("tie-{n}")),
                act: Act::Inform,
                task: None,
            })
            .expect("accept");
        }
        s.db.execute("UPDATE deliveries SET state='held',reason='fixture'", [])
            .expect("held");
        s.db.execute("UPDATE deliveries SET msg_seq=3 WHERE msg_seq=2", [])
            .expect("within-state tie");
        let filter = MailQuery {
            states: vec!["held".into()],
            limit: 10,
            ..Default::default()
        };
        let actual = query(&s.db, &filter).expect("page");
        let sequences = actual["rows"]
            .as_array()
            .expect("rows")
            .iter()
            .map(|r| r["msg_seq"].as_i64().expect("seq"))
            .collect::<Vec<_>>();
        assert_eq!(
            sequences,
            vec![3, 3, 1],
            "single-state duplicate was discarded"
        );
        assert_eq!(actual, legacy_page(&s.db, filter.states, None, 10));
    }
    #[test]
    fn unfiltered_pages_preserve_results_with_less_sql_work() {
        let dir = tempfile::tempdir().expect("query dir");
        let mut s =
            Store::open(&dir.path().join("mail.db"), Config::default()).expect("query store");
        let to = json(&AgentId::new()).expect("agent");
        s.db.execute("INSERT INTO agents(id,name) VALUES (?,'query')", [&to])
            .expect("agent");
        let tx = s.db.transaction().expect("tx");
        for n in 1..=40000 {
            let id = format!("\"{n:026}\"");
            tx.execute("INSERT INTO messages(id,sender,client_key,recipient,conversation,hop,act,body,sent_at) VALUES (?,'\"Human\"',?,?,'\"00000000000000000000000001\"',0,'\"Inform\"','small','unused')",params![id,format!("key-{n}"),to]).expect("message");
            let state = if n <= 20000 {
                "queued"
            } else {
                match n % 4 {
                    0 => "held",
                    1 => "uncertain",
                    2 => "queued",
                    _ => "offered",
                }
            };
            tx.execute("INSERT INTO deliveries(message_id,state,incarnation,reason,msg_seq) VALUES (?,?,?,?,?)",params![id,state,if ["offered","uncertain"].contains(&state){Some(1)}else{None},if state=="held"{Some("hold")}else{None},n]).expect("delivery");
        }
        tx.commit().expect("commit");
        let steps = Arc::new(AtomicUsize::new(0));
        let count = steps.clone();
        s.db.progress_handler(
            1,
            Some(move || {
                count.fetch_add(1, Ordering::Relaxed);
                false
            }),
        );
        for states in [
            vec!["held".to_owned(), "uncertain".to_owned()],
            vec!["offered".to_owned(), "queued".to_owned()],
        ] {
            let branches=states.iter().map(|s|format!("SELECT msg_seq AS seq FROM (SELECT msg_seq FROM deliveries WHERE state='{s}' AND msg_seq<?1 ORDER BY msg_seq DESC LIMIT 10000)")).collect::<Vec<_>>().join(" UNION ");
            let candidates = format!("SELECT seq FROM ({branches}) ORDER BY seq DESC LIMIT 10000");
            let sql = format!(
                "WITH candidates AS MATERIALIZED ({candidates}) SELECT {META},{DELIVERY} FROM candidates c JOIN messages m ON m.seq=c.seq JOIN deliveries d ON d.message_id=m.id WHERE (?2 IS NULL OR m.sender=?2) AND (?3 IS NULL OR m.recipient=?3) ORDER BY m.seq DESC LIMIT 1000"
            );
            steps.store(0, Ordering::Relaxed);
            let expected =
                s.db.prepare(&sql)
                    .expect("legacy")
                    .query_map(
                        params![i64::MAX, Option::<String>::None, Option::<String>::None],
                        metadata,
                    )
                    .expect("rows")
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .expect("collect");
            let legacy_steps = steps.swap(0, Ordering::Relaxed);
            let actual = query(
                &s.db,
                &MailQuery {
                    states: states.clone(),
                    limit: 1000,
                    ..Default::default()
                },
            )
            .expect("query");
            let actual_steps = steps.swap(0, Ordering::Relaxed);
            assert_eq!(actual["rows"], Value::Array(expected));
            assert_eq!(
                actual["next_before_msg_seq"],
                actual["rows"][999]["msg_seq"]
            );
            println!(
                "QUERY_WORK states={states:?} legacy_vm={legacy_steps} actual_vm={actual_steps}"
            );
            assert!(
                actual_steps < legacy_steps * 3 / 4,
                "unfiltered page fetched unused metadata: legacy_vm={legacy_steps} actual_vm={actual_steps}"
            );
        }
        s.db.progress_handler(0, None::<fn() -> bool>);
        s.db.execute("DELETE FROM deliveries WHERE msg_seq<=20000", [])
            .expect("orphan messages");
        s.db.execute(
            "UPDATE deliveries SET msg_seq=39000 WHERE msg_seq IN (39001,38996)",
            [],
        )
        .expect("duplicate candidate sequences");
        for states in [
            vec![],
            vec!["held"],
            vec!["queued", "queued"],
            vec!["held", "uncertain"],
            vec!["queued", "offered"],
            vec!["queued", "offered", "consumed", "held", "uncertain", "dead"],
        ] {
            let states = states.into_iter().map(str::to_owned).collect::<Vec<_>>();
            for before in [None, Some(39500), Some(20003), Some(20001), Some(0)] {
                for limit in [0, 1, 17, 1000, 2000] {
                    let actual = query(
                        &s.db,
                        &MailQuery {
                            states: states.clone(),
                            before_msg_seq: before,
                            limit,
                            ..Default::default()
                        },
                    )
                    .expect("page");
                    assert_eq!(
                        actual,
                        legacy_page(&s.db, states.clone(), before, limit),
                        "states={states:?} before={before:?} limit={limit}"
                    );
                }
            }
        }
    }
}
