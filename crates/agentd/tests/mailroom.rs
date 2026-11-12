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
async fn acceptance_is_idempotent_and_a_key_reused_for_other_content_is_rejected() {
    let (dir, mail, a, b) = setup(Config::default()).await;
    let mut first = send(Sender::Human, a);
    first.client_key = ClientKey("retry".into());
    let mut same = send(Sender::Human, a);
    same.client_key = first.client_key.clone();
    let mut other = send(Sender::Human, b);
    other.client_key = first.client_key.clone();
    let (x, y, z) = tokio::join!(mail.accept(first), mail.accept(same), mail.accept(other));
    let (x_out, x) = x.expect("accept");
    let (y_out, y) = y.expect("retry");
    assert_eq!(x, y);
    assert!(matches!(
        (x_out, y_out),
        (SendOutcome::Queued { .. }, SendOutcome::Duplicate { .. })
    ));
    assert!(
        matches!(z, Err(Error::Rejected(_))),
        "a key reused for another recipient: {z:?}"
    );
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
    for _ in 0..2 {
        let mut note = send(Sender::Agent(a), b);
        note.task = Some(root_id);
        mail.accept(note).await.expect("owner inform");
    }
    let child = request(&mail, Sender::Agent(a), b, Some(root_id), budget(3))
        .await
        .expect("child");
    // Two informs, the request itself and the child's three messages.
    let spent = Budget {
        messages: 6,
        ..budget(3)
    };
    assert_eq!(mail.task(root_id).await.expect("root").spent, spent);
    assert_eq!(
        mail.task(child.task().expect("task"))
            .await
            .expect("child")
            .parent,
        Some(root_id)
    );
    for excess in [
        Budget {
            tokens: Some(8),
            ..budget(1)
        },
        Budget {
            wall_secs: 8,
            ..budget(1)
        },
        Budget {
            messages: 4,
            ..budget(1)
        },
        Budget {
            hops: 8,
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
        assert_eq!(mail.task(root_id).await.expect("unchanged").spent, spent);
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
    request(
        &mail,
        Sender::Agent(a),
        b,
        Some(root_id),
        Budget {
            messages: 3,
            ..budget(7)
        },
    )
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
        5
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
    assert_eq!(
        mail.task(id).await.expect("parent").spent,
        Budget {
            messages: 4,
            ..budget(3)
        }
    );
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
        let mut report = send(Sender::Agent(a), a);
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
        assert!(
            request(&mail, Sender::Agent(a), b, Some(id), budget(1))
                .await
                .is_err()
        );
        let mut repeat = send(Sender::Agent(a), a);
        repeat.act = Act::Report { task: id, outcome };
        assert!(mail.accept(repeat).await.is_err());
        reports.push(report);
    }
    let root = request(&mail, Sender::Human, a, None, budget(10))
        .await
        .expect("root");
    let id = root.task().expect("task");
    let mut report = send(Sender::Agent(a), a);
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
    db.execute_batch("PRAGMA user_version=5").expect("version");
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
async fn version_two_database_upgrades_with_agents_running() {
    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("v2.db");
    let db = rusqlite::Connection::open(&path).expect("db");
    db.execute_batch("PRAGMA journal_mode=WAL;").expect("wal");
    db.execute_batch(include_str!("../src/schema.sql"))
        .expect("v1 schema");
    db.execute_batch(include_str!("../src/schema-v2.sql"))
        .expect("v2 schema");
    let id = AgentId::new();
    let spec = serde_json::json!({"name":"old","kind":"claude","cwd":"/","model":null,
        "permissions":PermissionPolicy::default(),"idle_after":null});
    db.execute(
        "INSERT INTO agents(id,name) VALUES (?,?)",
        [serde_json::to_string(&id).expect("id"), "old".into()],
    )
    .expect("agent");
    db.execute(
        "INSERT INTO agent_specs(agent,spec) VALUES (?,?)",
        [serde_json::to_string(&id).expect("id"), spec.to_string()],
    )
    .expect("spec");
    db.close().expect("close");

    let mail = Mailroom::open(&path, Config::default())
        .await
        .expect("upgrade");
    assert!(!mail.stopped(id).await.expect("stop flag"));
    assert_eq!(mail.specs().await.expect("specs").len(), 1);
    mail.close().await.expect("close");
    let db = rusqlite::Connection::open(&path).expect("db");
    let version: u32 = db
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .expect("version");
    assert_eq!(version, 4);
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
    assert_eq!(
        mail.task(id).await.expect("parent").spent,
        Budget {
            messages: 7,
            ..budget(6)
        }
    );
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

#[tokio::test]
async fn per_pair_limit_rejects_without_writing_and_consumption_frees_a_slot() {
    let (dir, mail, a, b) = setup(Config::default()).await;
    let mut ids = Vec::new();
    for _ in 0..16 {
        ids.push(
            mail.accept(send(Sender::Agent(a), b))
                .await
                .expect("accept")
                .1
                .id(),
        );
    }
    let rejected = send(Sender::Agent(a), b);
    let key = rejected.client_key.clone();
    let error = mail.accept(rejected).await.expect_err("pair full");
    assert!(matches!(error, Error::MailLimit(_)));
    let text = error.to_string();
    assert!(
        text.contains("per_pair")
            && text.contains("16")
            && text.contains(&b.to_string())
            && text.contains("wait for delivery"),
        "{text}"
    );
    assert_eq!(mail.pending_for(b).await.expect("pending").len(), 16);
    mail.close().await.expect("close");
    let db = rusqlite::Connection::open(dir.path().join("broker.db")).expect("inspect");
    for table in ["messages", "deliveries"] {
        assert_eq!(
            db.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r
                .get::<_, u32>(0))
                .expect("count"),
            16
        );
    }
    db.close().expect("close inspector");
    let mail = Mailroom::open(dir.path().join("broker.db"), Config::default())
        .await
        .expect("reopen");
    mail.mark_offered(&[ids[0]], Incarnation(1))
        .await
        .expect("offer");
    assert!(
        matches!(
            mail.accept(send(Sender::Agent(a), b)).await,
            Err(Error::MailLimit(_))
        ),
        "offered mail still counts"
    );
    mail.mark_consumed(&[ids[0]], Incarnation(1))
        .await
        .expect("consume");
    let mut retry = send(Sender::Agent(a), b);
    retry.client_key = key;
    assert!(matches!(
        mail.accept(retry).await.expect("slot freed").0,
        SendOutcome::Queued { .. }
    ));
    mail.close().await.expect("close");
}

#[tokio::test]
async fn per_sender_limit_counts_across_recipients_and_conversations() {
    let (_dir, mail, a, b) = setup(Config::default()).await;
    let mut recipients = vec![b];
    for name in ["carol", "dave", "eve", "frank"] {
        recipients.push(
            mail.register(AgentName::parse(name).expect("name"))
                .await
                .expect("recipient"),
        );
    }
    let mut last = None;
    for &to in &recipients[..4] {
        for _ in 0..16 {
            let msg = send(Sender::Agent(a), to);
            let key = msg.client_key.clone();
            let (_, env) = mail.accept(msg).await.expect("accept");
            last = Some((key, env));
        }
    }
    let (key, env) = last.expect("bug: sent mail");
    let mut retry = send(Sender::Agent(a), env.to());
    retry.client_key = key;
    assert_eq!(
        mail.accept(retry)
            .await
            .expect("duplicate at both limits")
            .0,
        SendOutcome::Duplicate { id: env.id() }
    );
    let error = mail
        .accept(send(Sender::Agent(a), recipients[4]))
        .await
        .expect_err("sender full");
    assert!(matches!(error, Error::MailLimit(_)));
    assert!(error.to_string().contains("per_sender") && error.to_string().contains("64"));
    mail.accept(send(Sender::Agent(b), recipients[4]))
        .await
        .expect("another sender has its own limit");
    mail.mark_offered(&[env.id()], Incarnation(1))
        .await
        .expect("offer");
    mail.mark_consumed(&[env.id()], Incarnation(1))
        .await
        .expect("consume");
    mail.accept(send(Sender::Agent(a), recipients[4]))
        .await
        .expect("sender slot freed");
    mail.close().await.expect("close");
}

#[tokio::test]
async fn held_and_uncertain_count_against_both_limits_but_dead_does_not() {
    for sender_limit in [false, true] {
        for state in ["held", "uncertain"] {
            let mut config = tagged_config();
            config.limits.per_sender =
                sender_limit.then(|| std::num::NonZeroU32::new(1).expect("positive"));
            config.limits.per_pair =
                (!sender_limit).then(|| std::num::NonZeroU32::new(1).expect("positive"));
            let (_dir, mail, a, b) = setup(config).await;
            let mut msg = send(Sender::Agent(a), b);
            if state == "held" {
                msg.body = Body::new("hold").expect("body");
            }
            let (_, env) = mail.accept(msg).await.expect("accept");
            if state == "uncertain" {
                mail.mark_offered(&[env.id()], Incarnation(1))
                    .await
                    .expect("offer");
                mail.session_ended(b, Incarnation(1), true)
                    .await
                    .expect("uncertain");
            }
            let mut retry = send(Sender::Agent(a), b);
            retry.client_key = ClientKey("rejected-key".into());
            assert!(
                matches!(mail.accept(retry).await, Err(Error::MailLimit(_))),
                "{sender_limit} {state}"
            );
            assert_eq!(mail.held(Some(b)).await.expect("held").len(), 1);
            mail.drop_held(env.id(), "operator dropped")
                .await
                .expect("drop");
            mail.accept(send(Sender::Agent(a), b))
                .await
                .expect("dead frees slot");
            mail.close().await.expect("close");
        }
    }
}

#[tokio::test]
async fn duplicate_at_pair_limit_and_human_mail_are_exempt() {
    let (_dir, mail, a, b) = setup(Config::default()).await;
    let msg = send(Sender::Agent(a), b);
    let key = msg.client_key.clone();
    let (_, env) = mail.accept(msg).await.expect("first");
    for _ in 1..16 {
        mail.accept(send(Sender::Agent(a), b))
            .await
            .expect("accept");
    }
    let mut retry = send(Sender::Agent(a), b);
    retry.client_key = key;
    assert_eq!(
        mail.accept(retry).await.expect("retry").0,
        SendOutcome::Duplicate { id: env.id() }
    );
    for _ in 0..65 {
        mail.accept(send(Sender::Human, b))
            .await
            .expect("human exempt");
    }
    assert_eq!(mail.pending_for(b).await.expect("pending").len(), 81);
    mail.close().await.expect("close");
}

#[tokio::test]
async fn none_disables_each_mail_limit_independently() {
    for disabled in ["per_sender", "per_pair", "per_turn"] {
        let mut config = Config::default();
        match disabled {
            "per_sender" => config.limits.per_sender = None,
            "per_pair" => config.limits.per_pair = None,
            "per_turn" => config.limits.per_turn = None,
            _ => unreachable!(),
        }
        let (_dir, mail, a, b) = setup(config).await;
        if disabled == "per_sender" {
            for name in ["bob", "carol", "dave", "eve", "frank"] {
                let to = mail
                    .register(AgentName::parse(name).expect("name"))
                    .await
                    .expect("register");
                for _ in 0..16 {
                    mail.accept(send(Sender::Agent(a), to))
                        .await
                        .expect("sender unlimited");
                }
            }
        } else {
            let from = if disabled == "per_turn" {
                Sender::Human
            } else {
                Sender::Agent(a)
            };
            for _ in 0..20 {
                mail.accept(send(from.clone(), b)).await.expect("unlimited");
            }
            if disabled == "per_turn" {
                assert_eq!(mail.next_batch(b).await.expect("batch").len(), 20);
            }
        }
        mail.close().await.expect("close");
    }
}

#[tokio::test]
async fn next_batch_caps_and_orders_only_queued_mail() {
    for cap in [2, 16] {
        let config = Config {
            limits: MailLimits {
                per_turn: std::num::NonZeroU32::new(cap),
                ..Default::default()
            },
            ..tagged_config()
        };
        let (_dir, mail, a, b) = setup(config).await;
        let mut queued = Vec::new();
        seed(&mail, a, "held").await;
        seed(&mail, a, "consumed").await;
        seed(&mail, a, "uncertain").await;
        for _ in 0..20 {
            queued.push(seed(&mail, a, "queued").await);
            seed(&mail, b, "queued").await;
        }
        let first = mail.next_batch(a).await.expect("first batch");
        assert_eq!(first, queued[..cap as usize]);
        let ids = first.iter().map(Envelope::id).collect::<Vec<_>>();
        mail.mark_offered(&ids, Incarnation(2))
            .await
            .expect("offer");
        let end = (2 * cap as usize).min(queued.len());
        assert_eq!(
            mail.next_batch(a).await.expect("next batch"),
            queued[cap as usize..end]
        );
        mail.close().await.expect("close");
    }
}

#[tokio::test]
async fn concurrent_sends_cannot_overrun_the_last_slot() {
    let (_dir, mail, a, b) = setup(Config {
        limits: MailLimits {
            per_pair: std::num::NonZeroU32::new(1),
            ..Default::default()
        },
        ..Default::default()
    })
    .await;
    let (one, two) = tokio::join!(
        mail.accept(send(Sender::Agent(a), b)),
        mail.accept(send(Sender::Agent(a), b))
    );
    assert!(
        matches!(
            (&one, &two),
            (Ok(_), Err(Error::MailLimit(_))) | (Err(Error::MailLimit(_)), Ok(_))
        ),
        "{one:?} {two:?}"
    );
    assert_eq!(mail.pending_for(b).await.expect("pending").len(), 1);
    mail.close().await.expect("close");
}

fn message_count(dir: &TempDir) -> u32 {
    let db = rusqlite::Connection::open(dir.path().join("broker.db")).expect("inspect");
    db.query_row("SELECT COUNT(*) FROM messages", [], |r| r.get(0))
        .expect("messages")
}

#[tokio::test]
async fn reports_go_to_the_requester_or_to_the_assignee_for_operator_tasks() {
    let (dir, mail, a, b) = setup(Config::default()).await;
    let c = mail
        .register(AgentName::parse("carol").expect("name"))
        .await
        .expect("register");
    let root = request(&mail, Sender::Human, a, None, budget(10))
        .await
        .expect("root")
        .task()
        .expect("task");
    let child = request(&mail, Sender::Agent(a), b, Some(root), budget(2))
        .await
        .expect("child")
        .task()
        .expect("task");
    let done = Outcome::Done { artifacts: vec![] };
    let report = |task: TaskId, from: AgentId, to: AgentId| {
        let mut msg = send(Sender::Agent(from), to);
        msg.act = Act::Report {
            task,
            outcome: done.clone(),
        };
        msg
    };
    mail.close().await.expect("close");
    let before = message_count(&dir);
    let mail = Mailroom::open(dir.path().join("broker.db"), Config::default())
        .await
        .expect("reopen");
    for (task, from, to) in [(child, b, c), (child, b, b), (root, a, b), (root, a, c)] {
        assert!(
            matches!(
                mail.accept(report(task, from, to)).await,
                Err(Error::Rejected(_))
            ),
            "report of {task} from {from} to {to}"
        );
        assert_eq!(mail.task(task).await.expect("task").state, TaskState::Open);
    }
    mail.close().await.expect("close");
    assert_eq!(
        message_count(&dir),
        before,
        "rejected reports store no mail"
    );
    let mail = Mailroom::open(dir.path().join("broker.db"), Config::default())
        .await
        .expect("reopen");
    mail.accept(report(child, b, a))
        .await
        .expect("to requester");
    mail.accept(report(root, a, a))
        .await
        .expect("operator task to self");
    mail.close().await.expect("close");
}

#[tokio::test]
async fn act_content_and_budgets_are_bounded() {
    let (dir, mail, a, b) = setup(Config::default()).await;
    let root = request(&mail, Sender::Human, a, None, budget(10))
        .await
        .expect("root")
        .task()
        .expect("task");
    let child = |deliverable: String, amount: Budget| {
        let mut msg = send(Sender::Agent(a), b);
        msg.task = Some(root);
        msg.act = Act::Request {
            deliverable,
            budget: amount,
            stakes: Stakes::Normal,
        };
        msg
    };
    let half = "x".repeat(MAX_BODY_BYTES / 2);
    let mut oversized = child(half.clone(), budget(1));
    oversized.body = Body::new(half.clone()).expect("body");
    let mut rejected = vec![
        child("x".repeat(1 << 20), budget(1)),
        oversized,
        child(
            "x".into(),
            Budget {
                wall_secs: 0,
                ..budget(1)
            },
        ),
        child(
            "x".into(),
            Budget {
                messages: 0,
                ..budget(1)
            },
        ),
        child(
            "x".into(),
            Budget {
                tokens: Some(0),
                ..budget(1)
            },
        ),
    ];
    let mut report = send(Sender::Agent(a), a);
    report.act = Act::Report {
        task: root,
        outcome: Outcome::Failed {
            tried: "x".repeat(MAX_BODY_BYTES),
        },
    };
    rejected.push(report);
    for msg in rejected {
        assert!(
            matches!(mail.accept(msg).await, Err(Error::Rejected(_))),
            "act must be rejected"
        );
    }
    let parent = mail.task(root).await.expect("root");
    assert_eq!(parent.spent, budget(0));
    assert_eq!(parent.state, TaskState::Open);
    let mut fits = child(
        half,
        Budget {
            hops: 0,
            ..budget(1)
        },
    );
    fits.body = Body::new("x".repeat(MAX_BODY_BYTES / 4)).expect("body");
    mail.accept(fits).await.expect("bounded act with zero hops");
    let mut full = send(Sender::Agent(a), b);
    full.body = Body::new("x".repeat(MAX_BODY_BYTES)).expect("body");
    mail.accept(full)
        .await
        .expect("a full-size inform is unchanged");
    mail.close().await.expect("close");
    assert_eq!(message_count(&dir), 3);
}

#[tokio::test]
async fn a_client_key_reused_for_different_content_is_an_error() {
    let (dir, mail, a, b) = setup(Config::default()).await;
    let root = request(&mail, Sender::Human, a, None, budget(10))
        .await
        .expect("root")
        .task()
        .expect("task");
    let keyed = |act: Act| {
        let mut msg = send(Sender::Agent(a), a);
        msg.client_key = ClientKey("reused".into());
        msg.act = act;
        msg
    };
    let report = Act::Report {
        task: root,
        outcome: Outcome::Done { artifacts: vec![] },
    };
    let (first, inform) = mail.accept(keyed(Act::Inform)).await.expect("inform");
    assert!(matches!(first, SendOutcome::Queued { .. }));
    let (again, same) = mail.accept(keyed(Act::Inform)).await.expect("retry");
    assert_eq!(again, SendOutcome::Duplicate { id: inform.id() });
    assert_eq!(same, inform);
    assert!(matches!(
        mail.accept(keyed(report.clone())).await,
        Err(Error::Rejected(_))
    ));
    let mut other_body = keyed(Act::Inform);
    other_body.body = Body::new("different").expect("body");
    assert!(matches!(
        mail.accept(other_body).await,
        Err(Error::Rejected(_))
    ));
    let mut other_to = keyed(Act::Inform);
    other_to.to = b;
    assert!(matches!(
        mail.accept(other_to).await,
        Err(Error::Rejected(_))
    ));
    assert_eq!(mail.task(root).await.expect("root").state, TaskState::Open);
    mail.close().await.expect("close");
    assert_eq!(message_count(&dir), 2);
}

#[tokio::test]
async fn a_client_key_retry_must_name_the_same_parent_task() {
    let (dir, mail, a, b) = setup(Config::default()).await;
    let root_msg = request(&mail, Sender::Human, a, None, budget(10))
        .await
        .expect("root");
    let root = root_msg.task().expect("task");
    let keyed = |key: &str, act: Act, task: Option<TaskId>, reply_to: Option<MessageId>| {
        let mut msg = send(Sender::Agent(a), b);
        msg.client_key = ClientKey(key.into());
        msg.act = act;
        msg.task = task;
        msg.reply_to = reply_to;
        msg
    };
    let child_act = Act::Request {
        deliverable: "work".into(),
        budget: budget(2),
        stakes: Stakes::Low,
    };
    let (_, child) = mail
        .accept(keyed("child", child_act.clone(), Some(root), None))
        .await
        .expect("child");
    let (_, inform) = mail
        .accept(keyed("inform", Act::Inform, Some(root), None))
        .await
        .expect("inform");
    let mut threaded = keyed("threaded", Act::Inform, None, Some(root_msg.id()));
    threaded.to = a;
    let (_, threaded_env) = mail.accept(threaded).await.expect("threaded");
    assert_eq!(threaded_env.task(), Some(root));
    for (key, act) in [("child", child_act.clone()), ("inform", Act::Inform)] {
        let result = mail.accept(keyed(key, act, None, None)).await;
        assert!(
            matches!(result, Err(Error::Rejected(_))),
            "{key} retry without parent_task: {result:?}"
        );
    }
    let honest_retries = || {
        let mut threaded = keyed("threaded", Act::Inform, None, Some(root_msg.id()));
        threaded.to = a;
        [
            (
                keyed("child", child_act.clone(), Some(root), None),
                child.id(),
            ),
            (keyed("inform", Act::Inform, Some(root), None), inform.id()),
            (threaded, threaded_env.id()),
        ]
    };
    for (msg, id) in honest_retries() {
        assert_eq!(
            mail.accept(msg).await.expect("honest retry").0,
            SendOutcome::Duplicate { id }
        );
    }
    let mut close = send(Sender::Agent(a), a);
    close.act = Act::Report {
        task: root,
        outcome: Outcome::Done { artifacts: vec![] },
    };
    mail.accept(close).await.expect("close root");
    for (msg, id) in honest_retries() {
        assert_eq!(
            mail.accept(msg).await.expect("honest retry").0,
            SendOutcome::Duplicate { id }
        );
    }
    mail.retire(b).await.expect("retire");
    for (msg, id) in honest_retries() {
        assert_eq!(
            mail.accept(msg).await.expect("honest retry").0,
            SendOutcome::Duplicate { id }
        );
    }
    mail.close().await.expect("close");
    assert_eq!(message_count(&dir), 5);
}

#[tokio::test]
async fn the_assignee_reports_to_itself_when_the_requester_is_retired() {
    let (dir, mail, a, b) = setup(Config::default()).await;
    let c = mail
        .register(AgentName::parse("carol").expect("name"))
        .await
        .expect("register");
    let root = request(&mail, Sender::Human, a, None, budget(10))
        .await
        .expect("root")
        .task()
        .expect("task");
    let mut children = Vec::new();
    for _ in 0..2 {
        children.push(
            request(&mail, Sender::Agent(a), b, Some(root), budget(2))
                .await
                .expect("child")
                .task()
                .expect("task"),
        );
    }
    mail.retire(a).await.expect("retire requester");
    let report = |task: TaskId, to: AgentId, outcome: Outcome| {
        let mut msg = send(Sender::Agent(b), to);
        msg.act = Act::Report { task, outcome };
        msg
    };
    let done = Outcome::Done { artifacts: vec![] };
    mail.close().await.expect("close");
    let before = message_count(&dir);
    let mail = Mailroom::open(dir.path().join("broker.db"), Config::default())
        .await
        .expect("reopen");
    match mail.accept(report(children[0], a, done.clone())).await {
        Err(Error::Rejected(reason)) => assert!(
            reason.contains("is retired") && reason.contains("address the report to yourself"),
            "{reason}"
        ),
        other => panic!("report to a retired requester: {other:?}"),
    }
    assert!(matches!(
        mail.accept(report(children[0], c, done.clone())).await,
        Err(Error::Rejected(_))
    ));
    for child in &children {
        assert_eq!(
            mail.task(*child).await.expect("task").state,
            TaskState::Open
        );
    }
    mail.accept(report(children[0], b, done.clone()))
        .await
        .expect("done to self");
    assert_eq!(
        mail.task(children[0]).await.expect("task").state,
        TaskState::Closed {
            outcome: done,
            success: true
        }
    );
    let (_, partial) = mail
        .accept(report(
            children[1],
            b,
            Outcome::Partial {
                artifacts: vec![],
                not_possible: "missing input".into(),
                evidence: Evidence {
                    tried: "ran tests".into(),
                    obstacle: "missing input".into(),
                    reproduction: None,
                },
            },
        ))
        .await
        .expect("partial to self");
    assert_eq!(
        mail.task(children[1]).await.expect("task").state,
        TaskState::Verifying {
            report: partial.id()
        }
    );
    mail.close().await.expect("close");
    assert_eq!(message_count(&dir), before + 2);
}

fn in_task(from: AgentId, to: AgentId, task: TaskId) -> NewMessage {
    let mut msg = send(Sender::Agent(from), to);
    msg.task = Some(task);
    msg
}

fn report(from: AgentId, to: AgentId, task: TaskId) -> NewMessage {
    let mut msg = send(Sender::Agent(from), to);
    msg.act = Act::Report {
        task,
        outcome: Outcome::Done { artifacts: vec![] },
    };
    msg
}

fn seconds(n: i64) -> time::Duration {
    time::Duration::seconds(n)
}

#[tokio::test]
async fn the_message_cap_rejects_the_next_send_and_still_accepts_the_report() {
    let (dir, mail, a, b) = setup(Config::default()).await;
    let root = request(
        &mail,
        Sender::Human,
        a,
        None,
        Budget {
            messages: 2,
            ..budget(10)
        },
    )
    .await
    .expect("root")
    .task()
    .expect("task");
    for _ in 0..2 {
        mail.accept(in_task(a, b, root)).await.expect("within cap");
    }
    let error = mail
        .accept(in_task(a, b, root))
        .await
        .expect_err("third message exceeds the cap");
    let text = error.to_string();
    assert!(
        text.contains(&root.to_string())
            && text.contains("all 2 messages")
            && text.contains("a Report is still accepted"),
        "{text}"
    );
    assert!(
        request(&mail, Sender::Agent(a), b, Some(root), budget(1))
            .await
            .is_err(),
        "a request is a message too"
    );
    assert_eq!(mail.task(root).await.expect("root").spent.messages, 2);
    mail.accept(report(a, a, root))
        .await
        .expect("a report is never rejected for budget");
    assert!(matches!(
        mail.task(root).await.expect("root").state,
        TaskState::Closed { success: true, .. }
    ));
    mail.close().await.expect("close");
    assert_eq!(message_count(&dir), 4, "rejected sends store nothing");
}

#[tokio::test]
async fn a_replayed_send_is_charged_once() {
    let (_dir, mail, a, b) = setup(Config::default()).await;
    let root = request(&mail, Sender::Human, a, None, budget(10))
        .await
        .expect("root")
        .task()
        .expect("task");
    let keyed = || {
        let mut msg = in_task(a, b, root);
        msg.client_key = ClientKey("once".into());
        msg
    };
    let (first, _) = mail.accept(keyed()).await.expect("first");
    assert!(matches!(first, SendOutcome::Queued { .. }));
    let (again, _) = mail.accept(keyed()).await.expect("replay");
    assert!(matches!(again, SendOutcome::Duplicate { .. }));
    assert_eq!(mail.task(root).await.expect("root").spent.messages, 1);
    mail.close().await.expect("close");
}

#[tokio::test]
async fn expiry_at_the_deadline_queues_each_notice_once() {
    let (_dir, mail, a, b) = setup(Config::default()).await;
    let root = request(&mail, Sender::Human, a, None, budget(100))
        .await
        .expect("root")
        .task()
        .expect("task");
    let child = request(
        &mail,
        Sender::Agent(a),
        b,
        Some(root),
        Budget {
            wall_secs: 60,
            ..budget(5)
        },
    )
    .await
    .expect("child")
    .task()
    .expect("task");
    let opened = mail.task(child).await.expect("child").opened;
    let deadline = opened + seconds(60);
    let early = mail
        .expire_tasks(deadline - seconds(1))
        .await
        .expect("early sweep");
    assert!(early.expired.is_empty());
    assert_eq!(early.next, Some(deadline));
    let pending_before = (
        mail.pending_for(a).await.expect("a").len(),
        mail.pending_for(b).await.expect("b").len(),
    );
    let sweep = mail.expire_tasks(deadline).await.expect("sweep");
    assert_eq!(sweep.expired, vec![child]);
    assert_eq!(sweep.notified, vec![b, a]);
    let expired = mail.task(child).await.expect("child");
    assert_eq!(expired.state, TaskState::Expired);
    assert_eq!(expired.spent.wall_secs, 60);
    assert_eq!(mail.task(root).await.expect("root").state, TaskState::Open);
    let again = mail
        .expire_tasks(deadline + seconds(1))
        .await
        .expect("again");
    assert!(again.expired.is_empty() && again.notified.is_empty());
    let notices = |agent: AgentId, before: usize| {
        let mail = &mail;
        async move {
            let pending = mail.pending_for(agent).await.expect("pending");
            assert_eq!(pending.len(), before + 1, "exactly one notice");
            pending.last().expect("notice").clone()
        }
    };
    let to_owner = notices(b, pending_before.1).await;
    assert_eq!(to_owner.from(), &Sender::Human);
    assert_eq!(to_owner.task(), Some(child));
    assert_eq!(*to_owner.act(), Act::Inform);
    let body = to_owner.body().as_str();
    assert!(
        body.contains(&child.to_string())
            && body.contains("wall clock budget of 60s")
            && body.contains("can no longer be reported"),
        "{body}"
    );
    let to_requester = notices(a, pending_before.0).await;
    let body = to_requester.body().as_str();
    assert!(
        body.contains(&child.to_string()) && body.contains("@bob"),
        "{body}"
    );
    mail.close().await.expect("close");
}

#[tokio::test]
async fn sends_on_an_expired_task_are_rejected_with_the_expiry() {
    let (dir, mail, a, b) = setup(Config::default()).await;
    let root = request(
        &mail,
        Sender::Human,
        a,
        None,
        Budget {
            wall_secs: 60,
            ..budget(10)
        },
    )
    .await
    .expect("root")
    .task()
    .expect("task");
    let opened = mail.task(root).await.expect("root").opened;
    mail.expire_tasks(opened + seconds(60))
        .await
        .expect("sweep");
    let error = mail
        .accept(report(a, a, root))
        .await
        .expect_err("expired tasks take no report");
    assert!(
        matches!(&error, Error::TaskExpired { task, wall_secs: 60, notified } if *task == root && notified.is_empty()),
        "{error:?}"
    );
    assert!(error.to_string().contains("expired"), "{error}");
    for msg in [in_task(a, b, root), {
        let mut child = in_task(a, b, root);
        child.act = Act::Request {
            deliverable: "more".into(),
            budget: budget(1),
            stakes: Stakes::Low,
        };
        child
    }] {
        assert!(matches!(
            mail.accept(msg).await,
            Err(Error::TaskExpired { .. })
        ));
    }
    mail.close().await.expect("close");
    assert_eq!(message_count(&dir), 2, "the request and one notice");
}

#[tokio::test]
async fn an_overdue_task_expires_after_the_database_is_reopened() {
    let (dir, mail, a, _) = setup(Config::default()).await;
    let root = request(
        &mail,
        Sender::Human,
        a,
        None,
        Budget {
            wall_secs: 60,
            ..budget(10)
        },
    )
    .await
    .expect("root")
    .task()
    .expect("task");
    let opened = mail.task(root).await.expect("root").opened;
    mail.close().await.expect("close");
    let mail = Mailroom::open(dir.path().join("broker.db"), Config::default())
        .await
        .expect("reopen");
    let sweep = mail
        .expire_tasks(opened + seconds(3600))
        .await
        .expect("sweep");
    assert_eq!(sweep.expired, vec![root]);
    assert_eq!(sweep.notified, vec![a]);
    assert_eq!(sweep.next, None);
    mail.close().await.expect("close");
}

/// Version 4 adds no columns, so a version 3 file differs only in
/// `user_version`. The rows below are the ones the version 3 code wrote.
#[tokio::test]
async fn a_version_three_database_opens_and_its_open_tasks_expire() {
    let (dir, mail, a, _) = setup(Config::default()).await;
    let root = request(
        &mail,
        Sender::Human,
        a,
        None,
        Budget {
            wall_secs: 60,
            ..budget(10)
        },
    )
    .await
    .expect("root")
    .task()
    .expect("task");
    mail.close().await.expect("close");
    let path = dir.path().join("broker.db");
    let db = rusqlite::Connection::open(&path).expect("db");
    db.execute_batch("PRAGMA user_version=3")
        .expect("version 3");
    let state: String = db
        .query_row("SELECT state FROM tasks", [], |r| r.get(0))
        .expect("state");
    assert_eq!(state, "\"Open\"");
    db.close().expect("close");
    let mail = Mailroom::open(&path, Config::default())
        .await
        .expect("upgrade");
    let task = mail.task(root).await.expect("old task");
    assert_eq!(task.state, TaskState::Open);
    let sweep = mail
        .expire_tasks(task.opened + seconds(60))
        .await
        .expect("sweep");
    assert_eq!(sweep.expired, vec![root]);
    mail.close().await.expect("close");
    let db = rusqlite::Connection::open(&path).expect("db");
    let version: u32 = db
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .expect("version");
    assert_eq!(version, 4);
}
