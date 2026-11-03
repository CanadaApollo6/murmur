use crate::{BoxError, engines::Engine};
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};
use url::{Host, Url};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub server: Vec<ServerConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub name: String,
    pub engine: Engine,
    pub base_url: Url,
    pub model: Option<String>,
    pub api_key_env: Option<String>,
    pub context_window: usize,
    pub slots: Option<usize>,
    #[serde(default)]
    pub supports_image_input: bool,
    #[serde(default = "yes")]
    pub include_usage: bool,
    #[serde(default = "five")]
    pub connect_timeout_secs: u64,
    #[serde(default = "six_hundred")]
    pub first_token_timeout_secs: u64,
    #[serde(default = "six_hundred")]
    pub progress_timeout_secs: u64,
    #[serde(default = "ten")]
    pub discovery_timeout_secs: u64,
    #[serde(default)]
    pub profile: Vec<Profile>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub id: String,
    pub display_name: String,
    pub temperature: Option<f64>,
    pub top_p: Option<f64>,
    pub top_k: Option<u64>,
    pub thinking: Option<bool>,
    pub max_output_tokens: Option<u64>,
    #[serde(default)]
    pub extra: HashMap<String, Value>,
}
fn yes() -> bool {
    true
}
fn five() -> u64 {
    5
}
fn ten() -> u64 {
    10
}
fn six_hundred() -> u64 {
    600
}
fn identifier(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, BoxError> {
        let path = expand_path(path)?;
        Self::parse(&std::fs::read_to_string(path)?)
    }
    pub fn parse(text: &str) -> Result<Self, BoxError> {
        let config: Self = toml::from_str(text)?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<(), BoxError> {
        let mut names = HashSet::new();
        let mut profiles = 0;
        for server in &self.server {
            if !identifier(&server.name) || !names.insert(&server.name) {
                return Err(format!("invalid or duplicate server name {:?}", server.name).into());
            }
            let url = &server.base_url;
            let loopback = match url.host() {
                Some(Host::Domain("localhost")) => true,
                Some(Host::Ipv4(ip)) => ip.is_loopback(),
                Some(Host::Ipv6(ip)) => ip.is_loopback(),
                _ => false,
            };
            if url.scheme() != "http"
                || !loopback
                || !matches!(url.path(), "" | "/")
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(
                    format!("{}: base_url must be an http loopback origin", server.name).into(),
                );
            }
            if server.context_window == 0 || server.slots == Some(0) {
                return Err(
                    format!("{}: context_window and slots must be positive", server.name).into(),
                );
            }
            if [
                server.connect_timeout_secs,
                server.first_token_timeout_secs,
                server.progress_timeout_secs,
                server.discovery_timeout_secs,
            ]
            .contains(&0)
            {
                return Err(format!("{}: timeouts must be positive", server.name).into());
            }
            let mut ids = HashSet::new();
            for profile in &server.profile {
                profiles += 1;
                if !identifier(&profile.id) || !ids.insert(&profile.id) {
                    return Err(format!(
                        "{}: invalid or duplicate profile id {:?}",
                        server.name, profile.id
                    )
                    .into());
                }
                for key in [
                    "model",
                    "messages",
                    "stream",
                    "stream_options",
                    "n",
                    "tools",
                    "tool_choice",
                    "parallel_tool_calls",
                    "max_tokens",
                    "max_completion_tokens",
                ] {
                    if profile.extra.contains_key(key) {
                        return Err(format!("extra.{key} is owned by the provider").into());
                    }
                }
                if server.engine == Engine::TensorFold {
                    if let Some(seed) = profile.extra.get("seed")
                        && seed.as_i64().is_none()
                        && seed.as_u64().is_none()
                    {
                        return Err("TensorFold extra.seed must be an integer".into());
                    }
                    if profile.max_output_tokens.is_none() {
                        tracing::warn!(
                            server = server.name,
                            profile = profile.id,
                            "TensorFold replies stop at the default 4096-token cap; thinking profiles need more"
                        );
                    }
                }
            }
        }
        if profiles == 0 {
            return Err("at least one profile is required".into());
        }
        Ok(())
    }
}

pub fn expand_path(path: &Path) -> Result<PathBuf, BoxError> {
    if let Ok(rest) = path.strip_prefix("~/") {
        Ok(PathBuf::from(std::env::var_os("HOME").ok_or("HOME is not set")?).join(rest))
    } else {
        Ok(path.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const BASE: &str = "[[server]]\nname='local'\nengine='tensorfold'\nbase_url='http://127.0.0.1:1'\ncontext_window=128\n[[server.profile]]\nid='think'\ndisplay_name='Think'\n";
    #[test]
    fn strict_validation() {
        Config::parse(BASE).expect("valid config");
        for text in [
            BASE.replace("context_window=128", "context_window=0"),
            BASE.replace("context_window=128", "context_window=128\nslots=0"),
            BASE.replace("127.0.0.1", "example.com"),
            BASE.replace("http:", "https:"),
            BASE.replace("127.0.0.1:1", "127.0.0.1:1/v1"),
            BASE.replace("name='local'", "name='Bad'"),
            format!("{BASE}unexpected=true\n"),
            format!("{BASE}extra={{messages=[]}}\n"),
            format!("{BASE}extra={{seed=1.2}}\n"),
            format!("{BASE}{BASE}"),
            BASE.split("[[server.profile]]")
                .next()
                .expect("server part")
                .to_owned(),
            format!("{BASE}[[server.profile]]\nid='think'\ndisplay_name='duplicate'\n"),
        ] {
            assert!(Config::parse(&text).is_err(), "must reject {text}");
        }
        Config::parse(&BASE.replace("127.0.0.1", "[::1]")).expect("IPv6 loopback");
        Config::parse(&format!("{BASE}extra={{seed=-1}}\n")).expect("integer seed");
    }
    #[tokio::test]
    async fn catalog_never_connects() {
        use infinity_provider_protocol::ModelProvider;
        let provider =
            crate::LocalProvider::new(Config::parse(BASE).expect("config")).expect("provider");
        let models = provider.list_models().await.expect("config catalog");
        assert_eq!(models[0].model_id, "local/think");
        assert_eq!(models[0].context_window, 128);
        assert!(models[0].keeps_unsigned_reasoning);
        assert_eq!(models[0].max_output_tokens, None);
    }
}
