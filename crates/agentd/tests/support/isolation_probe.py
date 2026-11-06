import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import ssl
import stat

root = Path(sys.argv[1])
cfg = json.loads((root / 'probe-input.json').read_text())
result = {}
if cfg.get('action'):
    action = cfg['action']
    first = not (root / 'action-result').exists()
    home = Path.home()
    if action == 'refresh':
        target = home / '.codex/auth.json'
        temp = target.with_name('refresh.tmp')
        temp.write_text(json.dumps({'token': cfg['token']}))
        temp.replace(target)
    elif action == 'symlink':
        target = home / '.codex/auth.json'
        target.unlink()
        target.symlink_to(cfg['secret'])
    elif action == 'restart' and first:
        target = home / '.claude/settings.json'
        target.unlink()
        target.symlink_to(cfg['victim'])
        sock = Path('/run/agentd/io/mcp.sock')
        sock.unlink()
        sock.write_text('agent planted regular file')
    (root / 'action-result').write_text(action if first else 'replayed')
    if action == 'restart':
        if first:
            import signal
            os.kill(os.getppid(), signal.SIGKILL)
        else:
            subprocess.Popen(['/usr/bin/python3', '-c', 'import time; time.sleep(600)', cfg['marker']], start_new_session=True, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    sys.exit(0)


def unix(path, request):
    with socket.socket(socket.AF_UNIX) as s:
        s.settimeout(3)
        s.connect(path)
        s.sendall(json.dumps(request).encode() + b'\n')
        return json.loads(s.makefile('rb').readline())

for name, path in cfg['paths'].items():
    try:
        if name == 'control':
            response = unix(path, {'command': 'send', 'to': '@a', 'body': 'forged human', 'key': 'forged-human'})
            result[name] = 'result' in response
        else:
            Path(path).stat()
            if name.startswith('sibling_'):
                Path(path).read_bytes()
            result[name] = True
    except (OSError, ValueError):
        result[name] = False
result['host_dirfd_hidden'] = True
for fd in Path('/proc/self/fd').iterdir():
    if int(fd.name) >= 3:
        try:
            if stat.S_ISDIR(fd.stat().st_mode):
                result['host_dirfd_hidden'] = False
        except FileNotFoundError:
            pass
result['own_token'] = 'AGENTD_MCP_TOKEN' in os.environ
result['forged_agent'] = False
try:
    env = Path(cfg['paths']['sibling_environ']).read_bytes().split(b'\0')
    token = next(v.split(b'=', 1)[1].decode() for v in env if v.startswith(b'AGENTD_MCP_TOKEN='))
    response = unix(cfg['shared'], {'token': token, 'session': 'a' * 64, 'request': {'jsonrpc': '2.0', 'id': 'stolen', 'method': 'tools/call', 'params': {'name': 'send_message', 'arguments': {'to': '@a', 'body': 'forged sibling'}}}})
    result['forged_agent'] = response['result']['isError'] is False
except (OSError, StopIteration, KeyError):
    pass
if not cfg['baseline']:
    request = {'session': 'b' * 64, 'token': '', 'request': {'jsonrpc': '2.0', 'id': 1, 'method': 'ping'}}
    result['token_rejected'] = unix('/run/agentd/io/mcp.sock', request)['error']['code'] == -32600
    for relative in ['.claude/settings.json', '.claude/CLAUDE.md', '.claude/agents/planted.md', '.claude/new-dir/planted', '.codex/AGENTS.md', '.codex/config.toml', '.codex/new-dir/planted']:
        path = Path.home() / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('planted private config')
    late = Path(cfg['late'])
    result['late_file_hidden'] = not (late / 'secret').exists()
    try:
        with socket.socket(socket.AF_UNIX) as s:
            s.connect(str(late / 'sock'))
        result['late_socket_hidden'] = False
    except OSError:
        result['late_socket_hidden'] = True
    credential = Path(cfg['credential'])
    temporary = credential.with_name('refresh.tmp')
    temporary.write_text('{"token":1}')
    temporary.replace(credential)
    result['rename'] = credential.read_text() == '{"token":1}'
    def proxy(host):
        with socket.create_connection(('127.0.0.1', 3128), timeout=10) as s:
            s.sendall(f'CONNECT {host}:443 HTTP/1.1\r\nHost: {host}:443\r\n\r\n'.encode())
            return s.recv(4096).decode()
    denied = proxy('attacker.invalid')
    result['proxy_deny'] = '403' in denied and 'blocked by agentd egress policy for @a' in denied
    def tls(sni):
        with socket.create_connection(('127.0.0.1', 3128), timeout=15) as s:
            s.sendall(b'CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n')
            header = b''
            while not header.endswith(b'\r\n\r\n'):
                part = s.recv(1)
                if not part:
                    raise AssertionError('CONNECT closed before TLS')
                header += part
            assert b'200 Connection Established' in header, header
            try:
                with ssl.create_default_context().wrap_socket(s, server_hostname=sni) as secure:
                    return bool(secure.version())
            except (ssl.SSLError, ConnectionResetError):
                return False
    result['proxy_allow'] = tls('example.com')
    import time
    time.sleep(1.1)
    result['proxy_sni_deny'] = not tls('www.cloudflare.com')
    result['proxy_private_deny'] = '403' in proxy('localhost')
    try:
        socket.create_connection(('1.1.1.1', 443), timeout=0.2)
        result['direct_network'] = True
    except OSError:
        result['direct_network'] = False
    # This detached descendant must die with the PID namespace, even after SIGKILL.
    subprocess.Popen(['/usr/bin/python3', '-c', 'import time; time.sleep(600)', cfg['marker']], start_new_session=True, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
(root / 'probe-result.json').write_text(json.dumps(result))
