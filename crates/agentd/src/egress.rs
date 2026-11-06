//! Per-agent HTTPS CONNECT policy. DNS resolution and connect share the checked address.
use crate::model::AgentId;
use crate::supervisor::Event;
use crate::surfaces::{Result, Socket};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixStream};

#[derive(Clone)]
pub struct Policy {
    pub hosts: Vec<String>,
    pub ports: Vec<u16>,
}
fn public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
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
                // IANA IPv4/IPv6 special-purpose registries, including documentation.
                && !(a == 198 && b == 51 && c == 100)
                && !(a == 203 && b == 0 && c == 113)
                && !(a == 192 && matches!((b, c), (31, 196) | (52, 193) | (88, 99) | (175, 48)))
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
                && !(b[0] == 0x20 && b[1] == 0x01 && b[2] < 2)
                && !(b[..4] == [0x20, 0x01, 0x0d, 0xb8])
                && !(b[..4] == [0x20, 0x01, 0x00, 0x03])
                && !(b[..4] == [0x26, 0x20, 0x00, 0x4f])
                && !(b[0] == 0x3f && b[1] == 0xff && b[2] < 16)
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
    fn permitted<'a>(
        &self,
        host: &str,
        port: u16,
        addrs: &'a [SocketAddr],
    ) -> Option<&'a [SocketAddr]> {
        if !self.allows(host, port)
            || addrs.is_empty()
            || addrs.iter().any(|a| !public(a.ip()) || a.port() != port)
        {
            None
        } else {
            Some(addrs)
        }
    }
}
pub async fn serve(
    socket: Socket,
    policy: Policy,
    agent: AgentId,
    name: String,
    events: crate::event_hub::EventHub,
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
    events: &crate::event_hub::EventHub,
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
    let Some(addrs) = policy.permitted(&host, port, &addrs) else {
        let body = format!("blocked by agentd egress policy for @{name}\n");
        read.get_mut().write_all(format!("HTTP/1.1 403 Forbidden\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await?;
        // Events have no delivery guarantee, just like activity and phase events.
        events.emit(Event::EgressDenied { agent, host, port });
        return Ok(());
    };
    let mut upstream = connect(addrs).await?;
    read.get_mut()
        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        .await?;
    let hello =
        match tokio::time::timeout(Duration::from_secs(10), client_hello(&mut read, &host)).await {
            Ok(Ok(hello)) => hello,
            result => {
                tracing::warn!(%agent, %host, ?result, "TLS ClientHello rejected");
                events.emit(Event::EgressDenied { agent, host, port });
                return Ok(());
            }
        };
    upstream.write_all(&hello).await?;
    tokio::io::copy_bidirectional(&mut read, &mut upstream).await?;
    Ok(())
}
async fn connect(addrs: &[SocketAddr]) -> Result<TcpStream> {
    tokio::time::timeout(Duration::from_secs(15), async {
        let budget = Duration::from_secs(15) / addrs.len().max(1) as u32;
        for addr in addrs {
            if let Ok(Ok(stream)) = tokio::time::timeout(budget, TcpStream::connect(addr)).await {
                return Ok(stream);
            }
        }
        Err("all validated egress addresses failed".into())
    })
    .await?
}
fn take<'a>(bytes: &mut &'a [u8], len: usize) -> Result<&'a [u8]> {
    if len > bytes.len() {
        return Err("truncated ClientHello".into());
    }
    let (head, tail) = bytes.split_at(len);
    *bytes = tail;
    Ok(head)
}
fn u16be(bytes: &mut &[u8]) -> Result<usize> {
    let n = take(bytes, 2)?;
    Ok(u16::from_be_bytes([n[0], n[1]]) as usize)
}
fn vector<'a>(bytes: &mut &'a [u8]) -> Result<&'a [u8]> {
    let len = u16be(bytes)?;
    take(bytes, len)
}
fn check_hello(mut bytes: &[u8], host: &str) -> Result<()> {
    let handshake = take(&mut bytes, 4)?;
    let len =
        ((handshake[1] as usize) << 16) | ((handshake[2] as usize) << 8) | handshake[3] as usize;
    if handshake[0] != 1 || len != bytes.len() {
        return Err("expected one complete ClientHello".into());
    }
    let version = take(&mut bytes, 2)?;
    if version[0] != 3 || version[1] > 3 {
        return Err("invalid TLS version".into());
    }
    take(&mut bytes, 32)?;
    let session_len = take(&mut bytes, 1)?[0] as usize;
    if session_len > 32 {
        return Err("invalid TLS session ID".into());
    }
    take(&mut bytes, session_len)?;
    let ciphers = vector(&mut bytes)?;
    if ciphers.is_empty() || ciphers.len() % 2 != 0 {
        return Err("invalid cipher suites".into());
    }
    let compression_len = take(&mut bytes, 1)?[0] as usize;
    if take(&mut bytes, compression_len)?.is_empty() {
        return Err("missing compression methods".into());
    }
    let mut extensions = vector(&mut bytes)?;
    if !bytes.is_empty() {
        return Err("trailing ClientHello bytes".into());
    }
    let mut sni = None;
    let mut seen = std::collections::HashSet::new();
    while !extensions.is_empty() {
        let kind = u16be(&mut extensions)?;
        let mut value = vector(&mut extensions)?;
        if !seen.insert(kind) || kind == 0xfe0d {
            return Err("duplicate extension or ECH forbidden".into());
        }
        if kind == 0 {
            let mut names = vector(&mut value)?;
            if !value.is_empty() {
                return Err("invalid SNI list".into());
            }
            while !names.is_empty() {
                let kind = take(&mut names, 1)?[0];
                let name = vector(&mut names)?;
                if kind != 0 || sni.is_some() {
                    return Err("invalid SNI name".into());
                }
                sni = Some(std::str::from_utf8(name)?);
            }
        }
    }
    if !sni.is_some_and(|sni| sni.eq_ignore_ascii_case(host)) {
        return Err("TLS SNI must match CONNECT host".into());
    }
    Ok(())
}
async fn client_hello(read: &mut (impl AsyncRead + Unpin), host: &str) -> Result<Vec<u8>> {
    const LIMIT: usize = 64 * 1024;
    let mut original = Vec::new();
    let mut handshake = Vec::new();
    loop {
        let mut header = [0u8; 5];
        read.read_exact(&mut header).await?;
        let len = u16::from_be_bytes([header[3], header[4]]) as usize;
        if header[0] != 22 || header[1] != 3 || header[2] > 3 || !(1..=16384).contains(&len) {
            return Err("invalid or oversized TLS handshake record".into());
        }
        if original.len() + 5 + len > LIMIT {
            return Err("ClientHello records exceed 64 KiB".into());
        }
        original.extend(header);
        let offset = original.len();
        original.resize(offset + len, 0);
        read.read_exact(&mut original[offset..]).await?;
        handshake.extend_from_slice(&original[offset..]);
        if handshake.len() >= 4 {
            let len = ((handshake[1] as usize) << 16)
                | ((handshake[2] as usize) << 8)
                | handshake[3] as usize;
            if handshake[0] != 1 || len + 4 > LIMIT {
                return Err("expected ClientHello under 64 KiB".into());
            }
            if handshake.len() >= len + 4 {
                check_hello(&handshake, host)?;
                return Ok(original);
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn hello(host: Option<&str>, ech: bool) -> Vec<u8> {
        let mut extensions = vec![];
        if let Some(host) = host {
            let mut names = vec![0];
            names.extend((host.len() as u16).to_be_bytes());
            names.extend(host.as_bytes());
            extensions.extend([0, 0]);
            extensions.extend(((names.len() + 2) as u16).to_be_bytes());
            extensions.extend((names.len() as u16).to_be_bytes());
            extensions.extend(names);
        }
        if ech {
            extensions.extend([0xfe, 0x0d, 0, 0]);
        }
        let mut body = vec![3, 3];
        body.extend([0; 32]);
        body.extend([0, 0, 2, 0x13, 1, 1, 0]);
        body.extend((extensions.len() as u16).to_be_bytes());
        body.extend(extensions);
        let mut record = vec![22, 3, 1];
        record.extend(((body.len() + 4) as u16).to_be_bytes());
        record.extend([
            1,
            0,
            ((body.len() >> 8) & 255) as u8,
            (body.len() & 255) as u8,
        ]);
        record.extend(body);
        record
    }
    #[tokio::test]
    async fn tls_sni_requires_connect_host_and_rejects_ech_and_non_tls() {
        for (bytes, allowed) in [
            (hello(Some("API.example.com"), false), true),
            (hello(Some("mcp.example.com"), false), false),
            (hello(Some("api.example.com"), true), false),
            (hello(None, false), false),
            (b"GET / HTTP/1.1\r\n".to_vec(), false),
            (vec![22, 3, 1, 255, 255], false),
            (vec![22, 3, 1, 0, 4, 1, 255, 255, 255], false),
        ] {
            assert_eq!(
                client_hello(&mut bytes.as_slice(), "api.example.com")
                    .await
                    .is_ok(),
                allowed
            );
        }
    }
    #[tokio::test]
    async fn tls_record_split_across_reads_is_forwarded_intact() {
        let bytes = hello(Some("api.example.com"), false);
        let (mut writer, mut reader) = tokio::io::duplex(7);
        let original = bytes.clone();
        let task = tokio::spawn(async move {
            for part in bytes.chunks(3) {
                writer.write_all(part).await.expect("write fragment");
                tokio::task::yield_now().await;
            }
        });
        assert_eq!(
            client_hello(&mut reader, "api.example.com")
                .await
                .expect("hello"),
            original
        );
        task.await.expect("writer");
    }
    fn record(payload: &[u8], kind: u8) -> Vec<u8> {
        let mut bytes = vec![kind, 3, 3];
        bytes.extend((payload.len() as u16).to_be_bytes());
        bytes.extend(payload);
        bytes
    }
    #[tokio::test]
    async fn hello_spans_records_but_rejects_oversize_and_interleaving() {
        let hello = hello(Some("api.example.com"), false);
        // Split even the handshake length field across records.
        for split in [2, 35, hello.len() - 6] {
            let mut bytes = record(&hello[5..5 + split], 22);
            bytes.extend(record(&hello[5 + split..], 22));
            assert_eq!(
                client_hello(&mut bytes.as_slice(), "api.example.com")
                    .await
                    .expect("fragmented hello"),
                bytes
            );
        }
        let mut bytes = record(&hello[5..35], 22);
        bytes.extend(record(&hello[35..], 23));
        assert!(
            client_hello(&mut bytes.as_slice(), "api.example.com")
                .await
                .is_err()
        );
        let bytes = record(&[1, 1, 0, 0], 22);
        assert!(
            client_hello(&mut bytes.as_slice(), "api.example.com")
                .await
                .is_err()
        );
        let mut bytes = record(&[1, 0, 255, 252], 22);
        for _ in 0..4 {
            bytes.extend(record(&vec![0; 16384], 22));
        }
        assert!(
            client_hello(&mut bytes.as_slice(), "api.example.com")
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn connects_to_second_address_when_first_fails() {
        let closed = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let first = closed.local_addr().expect("address");
        drop(closed);
        let live = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let second = live.local_addr().expect("address");
        assert_eq!(
            connect(&[first, second])
                .await
                .expect("fallback")
                .peer_addr()
                .expect("peer"),
            second
        );
    }
    #[test]
    fn policy_checks_every_address_and_hostname_boundary() {
        let policy = Policy {
            hosts: vec!["api.example.com".into(), "*.example.org".into()],
            ports: vec![443],
        };
        let good: SocketAddr = "1.1.1.1:443".parse().expect("address");
        assert_eq!(
            policy.permitted("api.example.com", 443, &[good]),
            Some(&[good][..])
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
            "198.51.100.1",
            "203.0.113.1",
            "192.31.196.1",
            "192.52.193.1",
            "192.88.99.1",
            "192.175.48.1",
            "2001:db8::1",
            "2001:2::1",
            "2001:10::1",
            "3fff::1",
            "3fff:fff::1",
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
