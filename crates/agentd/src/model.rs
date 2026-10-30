use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

pub use crate::swarm::TaskId;
use crate::swarm::{Act, ExposureRef};

macro_rules! id {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub struct $name(pub ulid::Ulid);

        impl $name {
            pub fn new() -> Self {
                Self(ulid::Ulid::new())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}
pub(crate) use id;
id!(AgentId);
id!(MessageId);
id!(ConversationId);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Incarnation(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct AgentName(String);

impl AgentName {
    pub fn parse(s: &str) -> Result<Self, InvalidName> {
        let s = s.strip_prefix('@').unwrap_or(s);
        if s.is_empty()
            || s.len() > 32
            || !s
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
            || s.starts_with('-')
        {
            return Err(InvalidName);
        }
        Ok(Self(s.into()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for AgentName {
    type Error = InvalidName;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::parse(&s)
    }
}

impl From<AgentName> for String {
    fn from(name: AgentName) -> Self {
        name.0
    }
}

#[derive(Debug, thiserror::Error)]
#[error("expected an agent ULID or [a-z0-9][a-z0-9-]{{0,31}}")]
pub struct InvalidName;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Recipient {
    Id(AgentId),
    Name(AgentName),
}

impl Recipient {
    pub fn parse(s: &str) -> Result<Self, InvalidName> {
        match s.strip_prefix("agent:") {
            Some(id) => Ok(Self::Id(AgentId(id.parse().map_err(|_| InvalidName)?))),
            None => Ok(Self::Name(AgentName::parse(s)?)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClientKey(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Sender {
    Human,
    Agent(AgentId),
}

pub const MAX_BODY_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Body(String);

impl Body {
    pub fn new(s: impl Into<String>) -> Result<Self, InvalidBody> {
        let s = s.into();
        if s.is_empty() || s.len() > MAX_BODY_BYTES {
            return Err(InvalidBody);
        }
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Body {
    type Error = InvalidBody;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl From<Body> for String {
    fn from(body: Body) -> Self {
        body.0
    }
}

#[derive(Debug, thiserror::Error)]
#[error("body must contain 1 to 65536 UTF-8 bytes")]
pub struct InvalidBody;

/// Committed messages are immutable. Only the mailroom constructs them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Envelope {
    pub(crate) id: MessageId,
    pub(crate) from: Sender,
    pub(crate) to: AgentId,
    pub(crate) conversation: ConversationId,
    pub(crate) in_reply_to: Option<MessageId>,
    pub(crate) hop: u16,
    pub(crate) act: Act,
    pub(crate) task: Option<TaskId>,
    pub(crate) exposure: Option<ExposureRef>,
    pub(crate) body: Body,
    pub(crate) sent_at: OffsetDateTime,
}

macro_rules! getter {
    ($name:ident, $ty:ty) => {
        pub fn $name(&self) -> $ty {
            self.$name
        }
    };
}

impl Envelope {
    getter!(id, MessageId);
    getter!(to, AgentId);
    getter!(conversation, ConversationId);
    getter!(in_reply_to, Option<MessageId>);
    getter!(hop, u16);
    getter!(task, Option<TaskId>);
    getter!(exposure, Option<ExposureRef>);
    getter!(sent_at, OffsetDateTime);

    pub fn from(&self) -> &Sender {
        &self.from
    }
    pub fn act(&self) -> &Act {
        &self.act
    }
    pub fn body(&self) -> &Body {
        &self.body
    }

    pub fn class(&self) -> DeliveryClass {
        match self.from {
            Sender::Human => DeliveryClass::UserTurn,
            Sender::Agent(_) => DeliveryClass::PeerEvent,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryClass {
    UserTurn,
    PeerEvent,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeliveryState {
    Queued,
    Offered { incarnation: Incarnation },
    Consumed,
    Uncertain { incarnation: Incarnation },
    Held { reason: String },
    Dead { reason: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct HeldMail {
    pub envelope: Envelope,
    pub state: DeliveryState,
}
