use agentd::{
    model::*,
    surfaces::events::{cut, mail, permission},
};
use serde_json::{Value, json};
use std::path::Path;

pub fn maximum_snapshot() -> Value {
    let hostile = "\u{0}\n\"\\'".repeat(8192);
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
    let snapshot = json!({"event":"snapshot","mail_cursor":i64::MAX,"agents":vec![json!({"id":format!("agent:{agent}"),"name":cut(&"'".repeat(1024),1024),"kind":cut(&"'".repeat(1024),1024),"isolation":"bwrap+proxy","phase":"failed","error":cut(&"\0".repeat(1024),1024)});1024],"agents_truncated":true,"approvals":vec![approval;64],"approvals_truncated":true,"mail":{"anchor":{"seq":i64::MAX,"nonce":"a".repeat(32)},"edges":vec![edge;65536],"edges_truncated":true,"stuck":vec![row.clone();1000],"moving":vec![row;1000],"stuck_truncated":true,"moving_truncated":true}});

    snapshot
}
