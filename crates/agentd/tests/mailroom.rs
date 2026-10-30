use agentd::mailroom::*;
use agentd::model::*;
use agentd::swarm::*;
use tempfile::TempDir;

struct Tagged;
impl Interceptor for Tagged {
    fn name(&self) -> &'static str {
        "tagged"
    }
    fn inspect(&self, env: &Envelope, _view: &ConversationView) -> Verdict {
        match env.body().as_str() {
            "hold" => Verdict::Hold {
                reason: "test hold".into(),
            },
            "reject" => Verdict::Reject {
                reason: "test reject".into(),
            },
            _ => Verdict::Deliver,
        }
    }
}

async fn setup(config: Config) -> (TempDir, Mailroom, AgentId, AgentId) {
    let dir = tempfile::tempdir().expect("temp dir");
    let mail = Mailroom::open(dir.path().join("broker.db"), config)
        .await
        .expect("open");
    let a = mail
        .register(AgentName::parse("alice").expect("name"))
        .await
        .expect("register");
    let b = mail
        .register(AgentName::parse("bob").expect("name"))
        .await
        .expect("register");
    (dir, mail, a, b)
}

fn tagged_config() -> Config {
    let mut config = Config::default();
    config.interceptors.push(Box::new(Tagged));
    config
}

fn send(from: Sender, to: AgentId) -> NewMessage {
    NewMessage {
        from,
        to,
        body: Body::new("hello").expect("body"),
        reply_to: None,
        client_key: ClientKey(MessageId::new().to_string()),
        act: Act::Inform,
        task: None,
    }
}

async fn seed(mail: &Mailroom, to: AgentId, state: &str) -> Envelope {
    let mut msg = send(Sender::Human, to);
    if state == "held" {
        msg.body = Body::new("hold").expect("body");
    }
    let (_, env) = mail.accept(msg).await.expect("accept");
    if matches!(state, "offered" | "consumed" | "uncertain") {
        mail.mark_offered(&[env.id()], Incarnation(1))
            .await
            .expect("offer");
    }
    match state {
        "consumed" => mail
            .mark_consumed(&[env.id()], Incarnation(1))
            .await
            .expect("consume"),
        "uncertain" => {
            mail.session_ended(to, Incarnation(1), true)
                .await
                .expect("end");
        }
        "dead" => {
            mail.retire(to).await.expect("retire");
        }
        _ => {}
    }
    env
}

#[tokio::test]
async fn transitions_reject_every_illegal_source_state() {
    for state in ["queued", "offered", "consumed", "uncertain", "held", "dead"] {
        for operation in ["offer", "start", "consume", "release", "drop"] {
            let (_dir, mail, a, _) = setup(tagged_config()).await;
            let env = seed(&mail, a, state).await;
            let before = mail.delivery(env.id()).await.expect("delivery");
            let result = match operation {
                "offer" => mail
                    .mark_offered(&[env.id()], Incarnation(1))
                    .await
                    .map(|_| ()),
                "start" => mail.mark_started(&[env.id()], Incarnation(1)).await,
                "consume" => mail.mark_consumed(&[env.id()], Incarnation(1)).await,
                "release" => mail.release(env.id()).await.map(|_| ()),
                "drop" => mail.drop_held(env.id(), "operator").await,
                _ => unreachable!(),
            };
            // Repeating an offer under the same incarnation, or a receipt for consumed
            // mail, is a no-op that succeeds.
            let allowed = match operation {
                "offer" => matches!(state, "queued" | "offered"),
                "start" => state == "offered",
                "consume" => matches!(state, "offered" | "consumed"),
                "release" | "drop" => matches!(state, "held" | "uncertain"),
                _ => false,
            };
            assert_eq!(result.is_ok(), allowed, "{state} {operation}: {result:?}");
            if !allowed {
                assert!(matches!(result, Err(Error::Transition(_))), "{result:?}");
                assert_eq!(mail.delivery(env.id()).await.expect("delivery"), before);
            } else {
                let after = mail.delivery(env.id()).await.expect("delivery");
                match operation {
                    "offer" => assert_eq!(
                        after.state,
                        DeliveryState::Offered {
                            incarnation: Incarnation(1)
                        }
                    ),
                    "start" => assert!(after.turn_started),
                    "consume" => assert_eq!(after.state, DeliveryState::Consumed),
                    "release" => assert_eq!(after.state, DeliveryState::Queued),
                    "drop" => assert_eq!(
                        after.state,
                        DeliveryState::Dead {
                            reason: "operator".into()
                        }
                    ),
                    _ => unreachable!(),
                }
            }
            assert_eq!(mail.message(env.id()).await.expect("message"), env);
            mail.close().await.expect("close");
        }
    }
}

#[tokio::test]
async fn receipts_and_batches_are_incarnation_checked_and_atomic() {
    let (_dir, mail, a, _) = setup(Config::default()).await;
    let first = seed(&mail, a, "queued").await;
    let second = seed(&mail, a, "queued").await;
    mail.mark_offered(&[second.id()], Incarnation(2))
        .await
        .expect("offer");
    assert!(
        mail.mark_offered(&[first.id(), second.id()], Incarnation(3))
            .await
            .is_err()
    );
    assert_eq!(
        mail.delivery(first.id()).await.expect("state").state,
        DeliveryState::Queued
    );
    assert!(
        mail.mark_consumed(&[second.id()], Incarnation(1))
            .await
            .is_err()
    );
    assert!(
        mail.mark_started(&[second.id()], Incarnation(1))
            .await
            .is_err()
    );
    mail.mark_offered(&[first.id()], Incarnation(2))
        .await
        .expect("offer");
    assert!(
        mail.mark_consumed(&[first.id(), MessageId::new()], Incarnation(2))
            .await
            .is_err()
    );
    assert!(
        mail.exposures(a, Epoch(0))
            .await
            .expect("exposures")
            .is_empty()
    );
    assert!(
        mail.mark_offered(&[MessageId::new()], Incarnation(2))
            .await
            .is_err()
    );
    assert!(
        mail.mark_consumed(&[first.id(), first.id()], Incarnation(2))
            .await
            .is_err()
    );
    assert!(mail.release(MessageId::new()).await.is_err());
    assert!(mail.drop_held(MessageId::new(), "drop").await.is_err());
    mail.mark_consumed(&[first.id(), second.id()], Incarnation(2))
        .await
        .expect("consume batch");
    assert_eq!(
        mail.exposures(a, Epoch(0)).await.expect("exposures"),
        vec![
            (1, ExposureSource::Message(first.id())),
            (2, ExposureSource::Message(second.id()))
        ]
    );
    mail.close().await.expect("close");
}

#[tokio::test]
async fn acceptance_is_idempotent_even_on_retry_with_different_content() {
    let (dir, mail, a, b) = setup(Config::default()).await;
    let mut first = send(Sender::Human, a);
    first.client_key = ClientKey("retry".into());
    let mut retry = send(Sender::Human, b);
    retry.client_key = first.client_key.clone();
    let (x, y) = tokio::join!(mail.accept(first), mail.accept(retry));
    let (x_out, x) = x.expect("accept");
    let (y_out, y) = y.expect("retry");
    assert_eq!(x, y);
    assert!(matches!(
        (x_out, y_out),
        (SendOutcome::Queued { .. }, SendOutcome::Duplicate { .. })
    ));
    mail.retire(a).await.expect("retire");
    let mut retry = send(Sender::Human, a);
    retry.client_key = ClientKey("retry".into());
    assert_eq!(
        mail.accept(retry).await.expect("retired retry").1.id(),
        x.id()
    );
    let mut other_sender = send(Sender::Agent(b), b);
    other_sender.client_key = ClientKey("retry".into());
    assert_ne!(
        mail.accept(other_sender)
            .await
            .expect("sender scoped")
            .1
            .id(),
        x.id()
    );
    mail.close().await.expect("close");
    let db = rusqlite::Connection::open(dir.path().join("broker.db")).expect("inspect");
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM messages", [], |r| r.get::<_, u32>(0))
            .expect("count"),
        2
    );
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM deliveries", [], |r| r
            .get::<_, u32>(0))
            .expect("count"),
        2
    );
}

#[tokio::test]
async fn session_end_uses_durable_evidence_and_preserves_newer_offers() {
    let (dir, mail, a, _) = setup(Config::default()).await;
    let one = seed(&mail, a, "offered").await;
    let two = seed(&mail, a, "offered").await;
    let newer = seed(&mail, a, "queued").await;
    mail.mark_started(&[two.id()], Incarnation(1))
        .await
        .expect("start");
    mail.mark_offered(&[newer.id()], Incarnation(2))
        .await
        .expect("newer");
    mail.close().await.expect("close");
    let mail = Mailroom::open(dir.path().join("broker.db"), Config::default())
        .await
        .expect("reopen");
    let end = mail
        .session_ended(a, Incarnation(1), false)
        .await
        .expect("end");
    assert_eq!(
        end,
        SessionEndOutcome {
            requeued: 1,
            uncertain: vec![two.id()]
        }
    );
    assert_eq!(mail.pending_for(a).await.expect("pending"), vec![one]);
    assert_eq!(
        mail.delivery(newer.id()).await.expect("delivery").state,
        DeliveryState::Offered {
            incarnation: Incarnation(2)
        }
    );
    assert_eq!(mail.held(Some(a)).await.expect("held")[0].envelope, two);
    assert_eq!(
        mail.session_ended(a, Incarnation(1), false)
            .await
            .expect("repeat"),
        SessionEndOutcome::default()
    );
    let end = mail
        .session_ended(a, Incarnation(2), true)
        .await
        .expect("end with evidence");
    assert_eq!(end.uncertain, vec![newer.id()]);
    mail.close().await.expect("close");
}

#[tokio::test]
async fn default_attempt_cap_holds_at_settlement_and_release_grants_fresh_attempts() {
    let (dir, mail, a, _) = setup(Config::default()).await;
    let env = seed(&mail, a, "queued").await;
    for attempt in 1..=5 {
        assert_eq!(
            mail.mark_offered(&[env.id()], Incarnation(attempt))
                .await
                .expect("offer")
                .offered,
            vec![env.id()]
        );
        mail.session_ended(a, Incarnation(attempt), false)
            .await
            .expect("end");
    }
    mail.close().await.expect("close");
    let mail = Mailroom::open(dir.path().join("broker.db"), Config::default())
        .await
        .expect("reopen");
    assert!(matches!(
        mail.delivery(env.id()).await.expect("held").state,
        DeliveryState::Held { .. }
    ));
    assert_eq!(mail.held(Some(a)).await.expect("held mail").len(), 1);
    assert_eq!(mail.delivery(env.id()).await.expect("attempts").attempts, 5);
    assert!(mail.pending_for(a).await.expect("pending").is_empty());
    mail.release(env.id()).await.expect("release");
    assert_eq!(
        mail.mark_offered(&[env.id()], Incarnation(7))
            .await
            .expect("offer")
            .offered,
        vec![env.id()]
    );
    mail.close().await.expect("close");
}

#[tokio::test]
async fn hop_limit_holds_and_human_turn_resets_hops() {
    let config = Config {
        interceptors: vec![Box::new(HopLimit { max: 1 })],
        ..Config::default()
    };
    let (_dir, mail, a, b) = setup(config).await;
    let human = seed(&mail, a, "queued").await;
    assert_eq!(human.hop(), 0);
    assert_eq!(human.class(), DeliveryClass::UserTurn);
    let mut peer = send(Sender::Agent(a), b);
    peer.reply_to = Some(human.id());
    let (out, peer) = mail.accept(peer).await.expect("peer");
    assert!(matches!(out, SendOutcome::Queued { .. }));
    assert_eq!(peer.hop(), 1);
    assert_eq!(peer.class(), DeliveryClass::PeerEvent);
    let mut reply = send(Sender::Agent(b), a);
    reply.reply_to = Some(peer.id());
    let (out, reply) = mail.accept(reply).await.expect("reply");
    assert!(matches!(out, SendOutcome::Held { .. }));
    assert_eq!(reply.hop(), 2);
    assert_eq!(reply.conversation(), human.conversation());
    let mut reset = send(Sender::Human, a);
    reset.reply_to = Some(reply.id());
    let (_, reset) = mail.accept(reset).await.expect("reset");
    assert_eq!(reset.hop(), 0);
    let view = mail.view(human.conversation()).await.expect("view");
    assert_eq!(view.recent, vec![human, peer, reply, reset]);
    assert_eq!(view.pair_exchanges.get(&(a, b)), Some(&1));
    assert_eq!(view.pair_exchanges.get(&(b, a)), Some(&1));
    mail.close().await.expect("close");
}

#[tokio::test]
async fn rejection_creates_no_mail_and_unknown_replies_or_senders_fail() {
    let (dir, mail, a, b) = setup(tagged_config()).await;
    let mut msg = send(Sender::Human, a);
    msg.body = Body::new("reject").expect("body");
    assert!(matches!(mail.accept(msg).await, Err(Error::Rejected(_))));
    let mut msg = send(Sender::Human, a);
    msg.reply_to = Some(MessageId::new());
    assert!(mail.accept(msg).await.is_err());
    assert!(
        mail.accept(send(Sender::Agent(AgentId::new()), a))
            .await
            .is_err()
    );
    assert!(
        mail.accept(send(Sender::Human, AgentId::new()))
            .await
            .is_err()
    );
    let parent = seed(&mail, a, "queued").await;
    let mut unrelated = send(Sender::Agent(b), a);
    unrelated.reply_to = Some(parent.id());
    assert!(mail.accept(unrelated).await.is_err());
    assert_eq!(mail.pending_for(a).await.expect("pending"), vec![parent]);
    mail.close().await.expect("close");
    let db = rusqlite::Connection::open(dir.path().join("broker.db")).expect("inspect");
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM messages", [], |r| r.get::<_, u32>(0))
            .expect("count"),
        1
    );
}

#[tokio::test]
async fn retire_dead_letters_all_live_states_and_keeps_terminal_states() {
    for state in ["queued", "offered", "uncertain", "held", "consumed", "dead"] {
        let (dir, mail, a, _) = setup(tagged_config()).await;
        let env = seed(&mail, a, state).await;
        let before = mail.delivery(env.id()).await.expect("before");
        mail.retire(a).await.expect("retire");
        let after = mail.delivery(env.id()).await.expect("after");
        if state == "consumed" || state == "dead" {
            assert_eq!(before, after);
        } else {
            assert_eq!(
                after.state,
                DeliveryState::Dead {
                    reason: "recipient retired".into()
                }
            );
        }
        assert!(mail.accept(send(Sender::Human, a)).await.is_err());
        assert!(mail.accept(send(Sender::Agent(a), a)).await.is_err());
        assert!(mail.resolve(Recipient::Id(a)).await.is_err());
        assert!(
            mail.resolve(Recipient::parse("@alice").expect("name"))
                .await
                .is_err()
        );
        assert!(mail.held(None).await.expect("held").is_empty());
        let replacement = mail
            .register(AgentName::parse("alice").expect("name"))
            .await
            .expect("reuse alias");
        assert_ne!(a, replacement);
        mail.close().await.expect("close");
        let mail = Mailroom::open(dir.path().join("broker.db"), Config::default())
            .await
            .expect("reopen");
        assert!(mail.resolve(Recipient::Id(a)).await.is_err());
        assert_eq!(
            mail.resolve(Recipient::parse("alice").expect("name"))
                .await
                .expect("resolve"),
            replacement
        );
        mail.close().await.expect("close");
    }
}

fn budget(n: u64) -> Budget {
    Budget {
        tokens: Some(n),
        wall_secs: n,
        messages: n as u32,
        hops: n as u16,
    }
}

async fn request(
    mail: &Mailroom,
    from: Sender,
    to: AgentId,
    parent: Option<TaskId>,
    amount: Budget,
) -> Result<Envelope> {
    let mut msg = send(from, to);
    msg.task = parent;
    msg.act = Act::Request {
        deliverable: "a tested change".into(),
        budget: amount,
        stakes: Stakes::Normal,
    };
    Ok(mail.accept(msg).await?.1)
}

#[tokio::test]
async fn child_carving_and_spend_cannot_exceed_any_remaining_budget() {
    let (dir, mail, a, b) = setup(Config::default()).await;
    let root = request(&mail, Sender::Human, a, None, budget(10))
        .await
        .expect("root");
    let root_id = root.task().expect("task");
    mail.record_spend(root_id, budget(2)).await.expect("spend");
    let child = request(&mail, Sender::Agent(a), b, Some(root_id), budget(3))
        .await
        .expect("child");
    assert_eq!(mail.task(root_id).await.expect("root").spent, budget(5));
    assert_eq!(
        mail.task(child.task().expect("task"))
            .await
            .expect("child")
            .parent,
        Some(root_id)
    );
    for excess in [
        Budget {
            tokens: Some(6),
            ..budget(1)
        },
        Budget {
            wall_secs: 6,
            ..budget(1)
        },
        Budget {
            messages: 6,
            ..budget(1)
        },
        Budget {
            hops: 6,
            ..budget(1)
        },
        Budget {
            tokens: None,
            ..budget(1)
        },
    ] {
        assert!(matches!(
            request(&mail, Sender::Agent(a), b, Some(root_id), excess).await,
            Err(Error::BudgetExceeded)
        ));
        assert!(matches!(
            mail.record_spend(root_id, excess).await,
            Err(Error::BudgetExceeded)
        ));
        assert_eq!(
            mail.task(root_id).await.expect("unchanged").spent,
            budget(5)
        );
    }
    assert!(
        request(&mail, Sender::Agent(b), a, Some(root_id), budget(1))
            .await
            .is_err()
    );
    assert!(
        request(&mail, Sender::Agent(a), b, None, budget(1))
            .await
            .is_err()
    );
    request(&mail, Sender::Agent(a), b, Some(root_id), budget(5))
        .await
        .expect("exact remainder");
    assert_eq!(mail.task(root_id).await.expect("spent").spent, budget(10));
    mail.close().await.expect("close");
    let mail = Mailroom::open(dir.path().join("broker.db"), Config::default())
        .await
        .expect("reopen");
    assert_eq!(
        mail.task(root_id).await.expect("persisted").spent,
        budget(10)
    );
    mail.close().await.expect("close");
    let db = rusqlite::Connection::open(dir.path().join("broker.db")).expect("inspect");
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM tasks", [], |r| r.get::<_, u32>(0))
            .expect("tasks"),
        3
    );
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM messages", [], |r| r.get::<_, u32>(0))
            .expect("messages"),
        3
    );
}

#[tokio::test]
async fn rejected_request_rolls_back_carving_and_duplicate_request_creates_one_task() {
    let (_dir, mail, a, b) = setup(tagged_config()).await;
    let root = request(&mail, Sender::Human, a, None, budget(10))
        .await
        .expect("root");
    let id = root.task().expect("task");
    let mut msg = send(Sender::Agent(a), b);
    msg.task = Some(id);
    msg.body = Body::new("reject").expect("body");
    msg.act = Act::Request {
        deliverable: "work".into(),
        budget: budget(3),
        stakes: Stakes::Low,
    };
    assert!(mail.accept(msg).await.is_err());
    assert_eq!(mail.task(id).await.expect("parent").spent, budget(0));
    for _ in 0..2 {
        let mut msg = send(Sender::Agent(a), b);
        msg.task = Some(id);
        msg.client_key = ClientKey("child".into());
        msg.act = Act::Request {
            deliverable: "work".into(),
            budget: budget(3),
            stakes: Stakes::Low,
        };
        mail.accept(msg).await.expect("idempotent request");
    }
    assert_eq!(mail.task(id).await.expect("parent").spent, budget(3));
    assert_eq!(mail.pending_for(b).await.expect("pending").len(), 1);
    mail.close().await.expect("close");
}

#[tokio::test]
async fn reports_record_all_outcomes_and_enforce_ownership_and_evidence() {
    let (dir, mail, a, b) = setup(Config::default()).await;
    let evidence = Evidence {
        tried: "ran tests".into(),
        obstacle: "missing input".into(),
        reproduction: Some("test".into()),
    };
    let outcomes = vec![
        Outcome::Done {
            artifacts: vec![ArtifactRef {
                locator: "commit:abc".into(),
            }],
        },
        Outcome::Partial {
            artifacts: vec![],
            not_possible: "missing input".into(),
            evidence: evidence.clone(),
        },
        Outcome::Infeasible { evidence },
        Outcome::Failed {
            tried: "ran tests".into(),
        },
        Outcome::Blocked { on: "input".into() },
    ];
    let mut reports = Vec::new();
    for outcome in outcomes {
        let root = request(&mail, Sender::Human, a, None, budget(10))
            .await
            .expect("root");
        let id = root.task().expect("task");
        let mut invalid = send(Sender::Agent(b), a);
        invalid.act = Act::Report {
            task: id,
            outcome: outcome.clone(),
        };
        assert!(mail.accept(invalid).await.is_err());
        let mut report = send(Sender::Agent(a), b);
        report.act = Act::Report {
            task: id,
            outcome: outcome.clone(),
        };
        let (_, report) = mail.accept(report).await.expect("report");
        let state = mail.task(id).await.expect("task").state;
        match outcome {
            Outcome::Partial { .. } | Outcome::Infeasible { .. } => assert_eq!(
                state,
                TaskState::Verifying {
                    report: report.id()
                }
            ),
            _ => assert_eq!(
                state,
                TaskState::Closed {
                    success: matches!(outcome, Outcome::Done { .. }),
                    outcome: outcome.clone()
                }
            ),
        }
        assert!(mail.record_spend(id, budget(1)).await.is_err());
        let mut repeat = send(Sender::Agent(a), b);
        repeat.act = Act::Report { task: id, outcome };
        assert!(mail.accept(repeat).await.is_err());
        reports.push(report);
    }
    let root = request(&mail, Sender::Human, a, None, budget(10))
        .await
        .expect("root");
    let id = root.task().expect("task");
    let mut report = send(Sender::Agent(a), b);
    report.act = Act::Report {
        task: id,
        outcome: Outcome::Infeasible {
            evidence: Evidence {
                tried: "".into(),
                obstacle: "".into(),
                reproduction: None,
            },
        },
    };
    assert!(mail.accept(report).await.is_err());
    assert_eq!(mail.task(id).await.expect("task").state, TaskState::Open);
    mail.close().await.expect("close");
    let mail = Mailroom::open(dir.path().join("broker.db"), Config::default())
        .await
        .expect("reopen");
    for report in reports {
        assert_eq!(
            mail.message(report.id()).await.expect("persisted report"),
            report
        );
    }
    mail.close().await.expect("close");
}

#[tokio::test]
async fn consume_logs_exposure_and_sends_snapshot_lineage_across_reopen_and_epochs() {
    let (dir, mail, a, b) = setup(Config::default()).await;
    let human = seed(&mail, a, "offered").await;
    assert_eq!(human.exposure(), None);
    mail.mark_consumed(&[human.id()], Incarnation(1))
        .await
        .expect("consume");
    let (_, peer) = mail.accept(send(Sender::Agent(a), b)).await.expect("send");
    assert_eq!(
        peer.exposure(),
        Some(ExposureRef {
            sender: a,
            epoch: Epoch(0),
            seq: 1
        })
    );
    let epoch = mail.new_epoch(a).await.expect("new epoch");
    assert_eq!(epoch, Epoch(1));
    let (_, clean) = mail
        .accept(send(Sender::Agent(a), b))
        .await
        .expect("clean send");
    assert_eq!(
        clean.exposure(),
        Some(ExposureRef {
            sender: a,
            epoch,
            seq: 0
        })
    );
    mail.close().await.expect("close");
    let mail = Mailroom::open(dir.path().join("broker.db"), Config::default())
        .await
        .expect("reopen");
    assert_eq!(
        mail.pending_for(b).await.expect("ordered"),
        vec![peer.clone(), clean]
    );
    assert_eq!(mail.message(peer.id()).await.expect("lineage"), peer);
    assert_eq!(
        mail.exposures(a, Epoch(0)).await.expect("exposures"),
        vec![(1, ExposureSource::Message(human.id()))]
    );
    assert!(
        mail.exposures(a, Epoch(1))
            .await
            .expect("new epoch")
            .is_empty()
    );
    mail.close().await.expect("close");
    let db = rusqlite::Connection::open(dir.path().join("broker.db")).expect("inspect");
    let row: (String, u64, u64, String, String) = db
        .query_row(
            "SELECT agent,epoch,seq,source_kind,source_id FROM exposures",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .expect("exposure row");
    assert_eq!(
        row,
        (
            serde_json::to_string(&a).expect("id"),
            0,
            1,
            "message".into(),
            serde_json::to_string(&human.id()).expect("id")
        )
    );
    assert_eq!(
        db.query_row("PRAGMA journal_mode", [], |r| r.get::<_, String>(0))
            .expect("WAL"),
        "wal"
    );
}

#[tokio::test]
async fn exclusive_writer_and_alias_resolution() {
    let (dir, mail, a, _) = setup(Config::default()).await;
    assert!(
        Mailroom::open(dir.path().join("broker.db"), Config::default())
            .await
            .is_err()
    );
    assert_eq!(
        mail.resolve(Recipient::parse("@alice").expect("recipient"))
            .await
            .expect("resolve"),
        a
    );
    assert_eq!(
        mail.resolve(Recipient::parse(&format!("agent:{a}")).expect("recipient"))
            .await
            .expect("resolve"),
        a
    );
    assert_eq!(
        mail.register(AgentName::parse("alice").expect("name"))
            .await
            .expect("idempotent"),
        a
    );
    mail.close().await.expect("close");
    Mailroom::open(dir.path().join("broker.db"), Config::default())
        .await
        .expect("new writer")
        .close()
        .await
        .expect("close");
}

#[test]
fn validates_names_and_body_on_construction_and_deserialization() {
    for invalid in [
        "",
        "-agent",
        "Alice",
        "a_b",
        "@",
        "abcdefghijklmnopqrstuvwxyz1234567",
    ] {
        assert!(AgentName::parse(invalid).is_err(), "{invalid}");
        assert!(Recipient::parse(invalid).is_err());
    }
    assert!(Recipient::parse("agent:invalid").is_err());
    assert!(Body::new("").is_err());
    assert!(Body::new("a".repeat(MAX_BODY_BYTES + 1)).is_err());
    assert!(Body::new("é".repeat(MAX_BODY_BYTES / 2 + 1)).is_err());
    assert!(Body::new("a".repeat(MAX_BODY_BYTES)).is_ok());
    assert!(serde_json::from_str::<Body>("\"\"").is_err());
    assert!(serde_json::from_str::<AgentName>("\"Bad Name\"").is_err());
}

#[tokio::test]
async fn held_and_uncertain_survive_reopen_and_can_be_dropped_or_released() {
    let (dir, mail, a, _) = setup(tagged_config()).await;
    let held = seed(&mail, a, "held").await;
    let uncertain = seed(&mail, a, "uncertain").await;
    mail.close().await.expect("close");
    let mail = Mailroom::open(dir.path().join("broker.db"), Config::default())
        .await
        .expect("reopen");
    assert_eq!(
        mail.held(None)
            .await
            .expect("held")
            .iter()
            .map(|h| h.envelope.id())
            .collect::<Vec<_>>(),
        vec![held.id(), uncertain.id()]
    );
    mail.release(uncertain.id()).await.expect("release");
    mail.drop_held(held.id(), "operator dropped")
        .await
        .expect("drop");
    assert_eq!(mail.pending_for(a).await.expect("pending"), vec![uncertain]);
    mail.close().await.expect("close");
}

#[tokio::test]
async fn schema_version_and_attempt_configuration_are_checked() {
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("future.db");
    let db = rusqlite::Connection::open(&path).expect("db");
    db.execute_batch("PRAGMA user_version=2").expect("version");
    db.close().expect("close");
    assert!(Mailroom::open(&path, Config::default()).await.is_err());
    assert!(
        Mailroom::open(
            dir.path().join("zero.db"),
            Config {
                max_attempts: 0,
                ..Config::default()
            }
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn concurrent_children_cannot_double_allocate_the_parent_remainder() {
    let (_dir, mail, a, b) = setup(Config::default()).await;
    let root = request(&mail, Sender::Human, a, None, budget(10))
        .await
        .expect("root");
    let id = root.task().expect("task");
    let (left, right) = tokio::join!(
        request(&mail, Sender::Agent(a), b, Some(id), budget(6)),
        request(&mail, Sender::Agent(a), b, Some(id), budget(6))
    );
    assert!(left.is_ok());
    assert!(matches!(right, Err(Error::BudgetExceeded)));
    assert_eq!(mail.task(id).await.expect("parent").spent, budget(6));
    assert_eq!(mail.pending_for(b).await.expect("one child").len(), 1);
    mail.close().await.expect("close");
}

#[tokio::test]
async fn repeated_offers_and_receipts_are_no_ops() {
    let (_dir, mail, a, _) = setup(Config::default()).await;
    let first = seed(&mail, a, "consumed").await;
    let second = seed(&mail, a, "queued").await;

    mail.mark_offered(&[second.id()], Incarnation(1))
        .await
        .expect("offer");
    let again = mail
        .mark_offered(&[second.id()], Incarnation(1))
        .await
        .expect("repeated offer");
    assert_eq!(again.offered, vec![second.id()]);
    assert_eq!(
        mail.delivery(second.id()).await.expect("delivery").attempts,
        1
    );

    // A batch holding a repeated receipt still consumes the new message.
    mail.mark_consumed(&[first.id(), second.id()], Incarnation(1))
        .await
        .expect("consume");
    assert_eq!(
        mail.delivery(second.id()).await.expect("delivery").state,
        DeliveryState::Consumed
    );
    let sources: Vec<_> = mail
        .exposures(a, Epoch(0))
        .await
        .expect("exposures")
        .into_iter()
        .map(|(_, source)| source)
        .collect();
    assert_eq!(
        sources,
        vec![
            ExposureSource::Message(first.id()),
            ExposureSource::Message(second.id())
        ]
    );
    mail.close().await.expect("close");
}
