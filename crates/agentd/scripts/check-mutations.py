#!/usr/bin/env python3
"""Disable one guarantee at a time, require a behavioral failure, then restore green."""
import pathlib
import subprocess
import sys

root = pathlib.Path(__file__).resolve().parents[3]
acp = "crates/agentd/src/adapters/acp.rs"
actor = "crates/agentd/src/supervisor.rs"
store = "crates/agentd/src/mailroom.rs"
cases = [
    ("cancelled-claim", store, 'store.actors.remove(&agent);', 'let _ = store.actors.get(&agent);', "mailroom::actor_tests::cancelled_claim_releases_its_lease_after_transaction"),
    ("restrictive-mode", acp, '"session/set_mode",', '"session/set_no_mode",', "open_mode_prompt_receipt_and_response_only"),
    ("receipt", acp, '.push_back(SessionEvent::Committed { ids: flight.ids });', '.push_back(SessionEvent::Activity(format!("{:?}", flight.ids)));', "open_mode_prompt_receipt_and_response_only"),
    ("busy-gate", actor, 'Phase::Idle | Phase::Dormant)', 'Phase::Idle | Phase::Dormant | Phase::Busy)', "busy_mail_waits_then_batches_peer_framing_and_human_input"),
    ("resume-load", acp, 'self.request("session/load", params)', 'self.request("session/new", params)', "idle_suspend_resumes_and_load_failure_needs_recovery"),
    ("turn-start-evidence", actor, '.mark_started(&ids, self.incarnation)', '.mark_started(&[], self.incarnation)', "death_before_start_requeues_and_after_start_is_uncertain"),
    ("failed-threshold", actor, 'self.crashes.len() >= self.config.crash_limit', 'self.crashes.len() >= self.config.crash_limit + 10', "crash_backoff_and_five_crashes_fail"),
    ("permission-incarnation", actor, 'request.incarnation != incarnation || incarnation != self.incarnation', 'request.incarnation != self.incarnation', "permission_escalation_answer_and_stale_incarnation"),
    ("permission-timeout", actor, '.find(|o| o.effect == PermissionEffect::RejectOnce)', '.find(|o| o.effect == PermissionEffect::AllowOnce)', "permission_timeout_and_per_agent_rules"),
    ("cancel-wire", acp, '"method":"session/cancel"', '"method":"session/no-cancel"', "cancel_is_a_notification_and_turn_completes"),
    ("single-owner", store, 'if claim.is_some() && s.actors.contains_key(&agent)', 'if claim.is_some() && false && s.actors.contains_key(&agent)', "startup_settles_old_incarnation_and_duplicate_spawn_is_rejected"),
    ("startup-recovery", actor, '.session_ended(self.id, Incarnation(self.incarnation.0 - 1), false)', '.session_ended(self.id, Incarnation(0), false)', "database_reopen_recovers_prior_offers_and_resume_token"),
    ("nonresumable-idle", actor, 'self.session.as_ref().is_some_and(|s| s.resumable())', 'self.session.as_ref().is_some()', "unsupported_resume_is_recovery_and_nonresumable_agents_do_not_suspend"),
    ("withdraw-stale-permission", actor, 'let (request, _) = self.pending.get(&id).ok_or(Error::StalePermission)?;\n        if request.incarnation != incarnation || incarnation != self.incarnation {\n            return Err(Error::StalePermission);\n        }', 'let (request, _) = self.pending.values().next().ok_or(Error::StalePermission)?;', "permission_from_dead_incarnation_cannot_answer_replacement"),
]

if len(sys.argv) > 1:
    cases = [case for case in cases if case[0] in sys.argv[1:]]

for label, filename, before, after, test in cases:
    path = root / filename
    original = path.read_text()
    if original.count(before) != 1:
        sys.exit(f"anchor count for {label}: {original.count(before)}")
    try:
        path.write_text(original.replace(before, after))
        run = subprocess.run(["cargo", "test", "-p", "agentd", *( ["--lib"] if test.startswith("mailroom::") else ["--test", "supervisor"] ), test, "--", "--exact"],
                             cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=45)
    finally:
        path.write_text(original)
    if run.returncode == 0 or "test result: FAILED." not in run.stdout:
        print(run.stdout)
        sys.exit(f"mutation did not fail behaviorally: {label}")
    failure = next(line for line in run.stdout.splitlines() if line.startswith("test result:"))
    print(f"RED {label} / {test}: {failure}", flush=True)
    green = subprocess.run(["cargo", "test", "-p", "agentd", *( ["--lib"] if test.startswith("mailroom::") else ["--test", "supervisor"] ), test, "--", "--exact"],
                           cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=45)
    if green.returncode:
        print(green.stdout)
        sys.exit(f"restore failed: {label}")
    summary = next(line for line in green.stdout.splitlines() if line.startswith("test result:"))
    print(f"GREEN {label}: {summary}", flush=True)
print(f"{len(cases)} red/restored-green mutation pairs passed")
