//! Per-agent HTTPS CONNECT policy. DNS resolution and connect share the checked address.
use crate::model::AgentId;
use crate::supervisor::Event;
use crate::surfaces::{Result, Socket};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixStream};

#[derive(Clone)]
pub struct Policy {
    pub hosts: Vec<String>,
    pub ports: Vec<u16>,
}
fn public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, _, _] = ip.octets();
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_unspecified()
                && !ip.is_multicast()
                && !ip.is_broadcast()
                && !(a == 100 && (64..=127).contains(&b))
                && a != 0
                && a < 240
                && !(a == 192 && b == 0)
                && !(a == 198 && (b == 18 || b == 19))
        }
        IpAddr::V6(ip) => {
            let b = ip.octets();
            if let Some(v4) = ip.to_ipv4_mapped() {
                return public(v4.into());
            }
            !ip.is_loopback()
                && !ip.is_unspecified()
                && !ip.is_multicast()
                && b[0] & 0xfe != 0xfc
                && !(b[0] == 0xfe && b[1] & 0xc0 == 0x80)
                // Require global unicast and exclude 6to4 and Teredo transitions.
                && b[0] & 0xe0 == 0x20
                && !(b[0] == 0x20 && b[1] == 0x02)
                && !(b[..4] == [0x20, 0x01, 0, 0])
        }
    }
}
impl Policy {
    fn allows(&self, host: &str, port: u16) -> bool {
        crate::sandbox::host_pattern(host)
            && !host.starts_with("*.")
            && self.ports.contains(&port)
            && self.hosts.iter().any(|pattern| {
                if let Some(suffix) = pattern.strip_prefix("*.") {
                    host.len() > suffix.len() + 1 && host.ends_with(&format!(".{suffix}"))
                } else {
                    host == pattern
                }
            })
    }
    fn permitted(&self, host: &str, port: u16, addrs: &[SocketAddr]) -> Option<SocketAddr> {
        if !self.allows(host, port)
            || addrs.is_empty()
            || addrs.iter().any(|a| !public(a.ip()) || a.port() != port)
        {
            None
        } else {
            Some(addrs[0])
        }
    }
}
pub async fn serve(
    socket: Socket,
    policy: Policy,
    agent: AgentId,
    name: String,
    events: tokio::sync::broadcast::Sender<Event>,
) -> Result<()> {
    let slots = Arc::new(tokio::sync::Semaphore::new(64));
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = socket.listener.accept() => {
                let (stream,_) = accepted?;
                if let Err(error) = Socket::check_peer(&stream) {tracing::warn!(%error,"egress peer rejected");continue;}
                if let Ok(permit) = slots.clone().try_acquire_owned() {
                    let policy = policy.clone(); let name = name.clone(); let events = events.clone();
                    connections.spawn(async move {let _permit = permit; if let Err(error) = connection(stream,&policy,agent,&name,&events).await {tracing::debug!(%error,"egress connection closed");}});
                }
            },
            done = connections.join_next(), if !connections.is_empty() => { if let Some(Err(error)) = done {tracing::warn!(%error,"egress task failed");} }
        }
    }
}
async fn connection(
    stream: UnixStream,
    policy: &Policy,
    agent: AgentId,
    name: &str,
    events: &tokio::sync::broadcast::Sender<Event>,
) -> Result<()> {
    let mut read = BufReader::new(stream);
    let header = tokio::time::timeout(Duration::from_secs(15), async {
        let mut header = Vec::new();
        loop {
            if header.len() >= 16384 {
                return Err("CONNECT header too large".into());
            }
            let mut line = Vec::new();
            use tokio::io::AsyncReadExt;
            let n = (&mut read)
                .take((16384 - header.len()) as u64)
                .read_until(b'\n', &mut line)
                .await?;
            if n == 0 {
                return Err("truncated CONNECT".into());
            }
            let done = line == b"\r\n";
            header.extend(line);
            if done {
                return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(header);
            }
        }
    })
    .await??;
    let text = std::str::from_utf8(&header)?;
    let parts: Vec<_> = text
        .lines()
        .next()
        .unwrap_or("")
        .split_whitespace()
        .collect();
    let authority = if parts.len() == 3 && parts[0] == "CONNECT" && parts[2] == "HTTP/1.1" {
        parts[1]
    } else {
        ""
    };
    let (host, port) = authority
        .rsplit_once(':')
        .and_then(|(h, p)| p.parse::<u16>().ok().map(|p| (h.to_ascii_lowercase(), p)))
        .unwrap_or_default();
    let addrs = if policy.allows(&host, port) {
        match tokio::time::timeout(
            Duration::from_secs(10),
            tokio::net::lookup_host((host.as_str(), port)),
        )
        .await
        {
            Ok(Ok(addrs)) => addrs.collect::<Vec<_>>(),
            _ => vec![],
        }
    } else {
        vec![]
    };
    let Some(addr) = policy.permitted(&host, port, &addrs) else {
        let body = format!("blocked by agentd egress policy for @{name}\n");
        read.get_mut().write_all(format!("HTTP/1.1 403 Forbidden\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await?;
        // Events have no delivery guarantee, just like activity and phase events.
        if events
            .send(Event::EgressDenied { agent, host, port })
            .is_err()
        {
            tracing::debug!("no egress event subscribers");
        }
        return Ok(());
    };
    let mut upstream =
        tokio::time::timeout(Duration::from_secs(15), TcpStream::connect(addr)).await??;
    read.get_mut()
        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        .await?;
    tokio::io::copy_bidirectional(&mut read, &mut upstream).await?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn policy_checks_every_address_and_hostname_boundary() {
        let policy = Policy {
            hosts: vec!["api.example.com".into(), "*.example.org".into()],
            ports: vec![443],
        };
        let good: SocketAddr = "1.1.1.1:443".parse().expect("address");
        assert_eq!(
            policy.permitted("api.example.com", 443, &[good]),
            Some(good)
        );
        assert!(policy.permitted("x.example.org", 443, &[good]).is_some());
        for host in [
            "example.org",
            "badexample.org",
            "api.example.com.attacker.org",
            "127.0.0.1",
        ] {
            assert!(policy.permitted(host, 443, &[good]).is_none());
        }
        for ip in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.1.1",
            "100.64.0.1",
            "0.0.0.0",
            "::1",
            "fc00::1",
            "fe80::1",
            "::ffff:127.0.0.1",
        ] {
            let bad = SocketAddr::new(ip.parse().expect("IP"), 443);
            assert!(
                policy
                    .permitted("api.example.com", 443, &[good, bad])
                    .is_none(),
                "{ip}"
            );
        }
        assert!(policy.permitted("api.example.com", 80, &[good]).is_none());
        assert!(policy.permitted("api.example.com", 443, &[]).is_none());
    }
}
