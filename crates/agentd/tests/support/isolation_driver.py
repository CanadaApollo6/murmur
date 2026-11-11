"""Real broker and fake ACP gates. Also runnable against an exported main binary."""
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time

binary, fake = [str(Path(p).resolve()) for p in sys.argv[1:3]]
baseline = '--baseline' in sys.argv
hard_exit = '--sigkill' in sys.argv
root = Path(tempfile.mkdtemp(prefix='agentd-isolation-'))
state = root / 'state'
state.mkdir(mode=0o700)
(state / "ui.token").write_text("a" * 64)
(state / "ui.token").chmod(0o600)
human = root / 'home'
for directory in ['.claude', '.codex']:
    (human / directory).mkdir(parents=True)
credential = human / '.claude/.credentials.json'
codex_credential = human / '.codex/auth.json'
credential.write_text('{"token":0}')
codex_credential.write_text('{"token":0}')
for name in ['.claude/settings.json', '.claude/CLAUDE.md', '.codex/AGENTS.md', '.codex/config.toml']:
    (human / name).write_text('human CLI config')
original_cli = {str(p.relative_to(human)): p.read_bytes() for p in human.rglob('*') if p.is_file()}

for name in ['a', 'b', 'c']:
    (root / name).mkdir()
    shutil.copy(Path(__file__).with_name('isolation_probe.py'), root / name / 'probe.py')
mode = 'none' if baseline else 'bwrap'
config = f'sandbox = "{mode}"\n[bwrap]\nro_binds = [{json.dumps(str(Path(fake).parent))}]\n'
for kind, name, model in [('claude', 'a', 'default'), ('codex', 'b', 'read-only')]:
    config += f'[kind.{kind}]\ncommand = [{json.dumps(fake)}]\nmode = "{model}"\nidle_after_secs = 0\n'
    if not baseline:
        config += f'egress = ["localhost", "example.com"]\n'
    config += f'[kind.{kind}.env]\nFAKE_ROOT = {json.dumps(str(root / name))}\nFAKE_MODE = {json.dumps(model)}\nFAKE_ROOT_FROM_CWD = "1"\n'
if not baseline:
    with socket.socket() as ui_reserve, socket.socket() as rap_reserve:
        ui_reserve.bind(('127.0.0.1', 0))
        rap_reserve.bind(('127.0.0.1', 0))
        ui_port = ui_reserve.getsockname()[1]
        rap_port = rap_reserve.getsockname()[1]
    config += f'[ui]\nlisten = \"127.0.0.1:{ui_port}\"\n'
    config += f'[rap]\nlisten = \"127.0.0.1:{rap_port}\"\n'
    config += '[kind.infinity]\nwire = "infinity"\nsocket = "/nonexistent/infinity.sock"\n'
(root / 'config.toml').write_text(config)
# Old main does not understand isolation configuration.
if baseline:
    (root / 'config.toml').write_text('\n'.join(line for line in config.splitlines() if not line.startswith(('sandbox =', '[bwrap]', 'ro_binds ='))))
log = open(root / 'daemon.log', 'wb')
daemon = subprocess.Popen([binary, '--config', str(root / 'config.toml'), '--state-dir', str(state)], stdout=subprocess.DEVNULL, stderr=log, env=dict(os.environ, HOME=str(human)))

def wait(fn, label, seconds=20):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        if daemon.poll() is not None:
            raise AssertionError(f'daemon exited {daemon.returncode}: {(root / "daemon.log").read_text()}')
        value = fn()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError(f'timeout {label}; {(root / "daemon.log").read_text()}')

def ctl(request):
    with socket.socket(socket.AF_UNIX) as s:
        s.settimeout(10)
        s.connect(str(state / 'agentd.sock'))
        s.sendall(json.dumps(request).encode() + b'\n')
        response = json.loads(s.makefile('rb').readline())
        assert 'error' not in response, response
        return response['result']

def spawn(name, kind):
    return ctl({'command': 'spawn', 'spec': {'name': name, 'kind': kind, 'cwd': str(root / name), 'permissions': {'sandbox': 'ReadOnly', 'rules': [], 'timeout': None}, 'model': None, 'idle_after': None}, 'first_message': None, 'key': 'spawn-' + name})

def proc_scan(needle, field):
    found = []
    for proc in Path('/proc').iterdir():
        if proc.name.isdigit():
            try:
                if needle in (proc / field).read_bytes():
                    found.append(int(proc.name))
            except OSError:
                pass
    return found

def process_tree(parent):
    table = {}
    for proc in Path('/proc').iterdir():
        if proc.name.isdigit():
            try:
                fields = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
                table[int(proc.name)] = (int(fields[1]), fields[0], fields[19])
            except (OSError, ValueError):
                pass
    descendants = {parent}
    while True:
        expanded = descendants | {pid for pid, row in table.items() if row[0] in descendants}
        if expanded == descendants:
            break
        descendants = expanded
    return {pid: table[pid][2] for pid in descendants if pid != parent and pid in table}

def surviving(recorded):
    alive = []
    for pid, start in recorded.items():
        try:
            fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
            if fields[19] == start and fields[0] != 'Z':
                alive.append(pid)
        except OSError:
            pass
    return alive

def wire_ready(name):
    p = root / name / 'wire.jsonl'
    return p.exists() and 'session/set_mode' in p.read_text()

success = False
try:
    wait(lambda: (state / 'agentd.sock').exists(), 'listen')
    b = spawn('b', 'codex')
    wait(lambda: wire_ready('b'), 'b idle')
    # The shim carries the sibling token on main. In the sandbox it carries none.
    sibling = wait(lambda: proc_scan(str(root / 'b').encode(), 'environ' if baseline else 'cmdline'), 'sibling host PID')[0]
    c = spawn('c', 'codex')
    wait(lambda: any(row['id'] == c['id'] and row['phase'] == 'idle' for row in ctl({'command': 'ls'})), 'sibling idle')
    a = spawn('a', 'claude')
    wait(lambda: wire_ready('a'), 'a idle')
    late = human / '.claude/late'
    late.mkdir()
    (late / 'secret').write_text('late host secret')
    late_socket = socket.socket(socket.AF_UNIX)
    late_socket.bind(str(late / 'sock'))
    late_socket.listen(1)
    marker = str(root / 'detached-marker')
    config = {'baseline': baseline, 'shared': str(state / 'mcp.sock'), 'credential': str(credential), 'marker': marker, 'late': str(late),
              'paths': {'control': str(state / 'agentd.sock'), 'state': str(state), 'sqlite': str(state / 'mail.db'), 'ui_token': str(state / 'ui.token'),
                        'sibling_environ': f'/proc/{sibling}/environ', 'sibling_cmdline': f'/proc/{sibling}/cmdline', 'sibling_mcp': str(state / 'agents' / b['id'].split(':', 1)[1] / 'io/mcp.sock')}}
    if not baseline:
        assert 'mixed isolation: kind infinity is unsandboxed' in (root / 'daemon.log').read_text()
    # Find the sibling shim itself for the token theft baseline.
    if baseline:
        sibling = next(pid for pid in proc_scan(b'AGENTD_MCP_TOKEN=', 'environ') if str(state / 'mcp.sock').encode() in Path(f'/proc/{pid}/cmdline').read_bytes())
        config['paths']['sibling_environ'] = f'/proc/{sibling}/environ'
        config['paths']['sibling_cmdline'] = f'/proc/{sibling}/cmdline'
    (root / 'a/probe-input.json').write_text(json.dumps(config))
    if not baseline:
        watch = socket.socket(socket.AF_UNIX)
        watch.settimeout(5)
        watch.connect(str(state / 'agentd.sock'))
        watch.sendall(b'{"command":"watch"}\n')
        watched = watch.makefile('rb')
        assert json.loads(watched.readline())['event'] == 'snapshot'
        web = socket.create_connection(('127.0.0.1', ui_port), timeout=5)
        web.sendall((f'GET /events HTTP/1.1\r\nHost: 127.0.0.1:{ui_port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: MDEyMzQ1Njc4OWFiY2RlZg==\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: murmur.events.v1, murmur.token.' + 'a' * 64 + '\r\n\r\n').encode())
        web_read = web.makefile('rb')
        assert b'101' in web_read.readline()
        while web_read.readline() != b'\r\n':
            pass
        def web_event():
            import struct
            while True:
                header = web_read.read(2)
                assert len(header) == 2, 'WebSocket closed'
                size = header[1] & 127
                if size == 126:
                    size = struct.unpack('!H', web_read.read(2))[0]
                elif size == 127:
                    size = struct.unpack('!Q', web_read.read(8))[0]
                data = web_read.read(size)
                if header[0] & 15 == 1:
                    return json.loads(data)
        assert web_event()['event'] == 'snapshot'

    ctl({'command': 'send', 'to': '@a', 'body': 'isolation-probe mcp-send hold', 'key': 'probe'})
    wait(lambda: (root / 'a/probe-result.json').exists(), 'probe')
    wait(lambda: (root / 'a/mcp-result.json').exists(), 'own shim')
    result = json.loads((root / 'a/probe-result.json').read_text())
    print(json.dumps({'root': str(root), 'baseline': baseline, 'probe': result}), flush=True)
    if baseline:
        assert all(result[k] for k in ['control', 'state', 'sqlite', 'ui_token', 'sibling_environ', 'sibling_cmdline', 'forged_agent']), result
    else:
        assert not any(result[k] for k in ['control', 'state', 'sqlite', 'ui_token', 'sibling_environ', 'sibling_cmdline', 'forged_agent', 'own_token', 'direct_network', 'sibling_mcp']), result
        assert all(result[k] for k in ['host_dirfd_hidden', 'token_rejected', 'rename', 'proxy_deny', 'proxy_private_deny', 'proxy_allow', 'proxy_sni_deny']), result
        while True:
            event = json.loads(watched.readline())
            if event.get('event') == 'egress_denied' and event.get('host') == 'attacker.invalid':
                break
        while True:
            event = web_event()
            if event.get('event') == 'egress_denied' and event.get('host') == 'attacker.invalid':
                break
        missing = []
        for transport, receive in [('Unix', lambda: json.loads(watched.readline())), ('WebSocket', web_event)]:
            try:
                while True:
                    event = receive()
                    if event.get('event') == 'egress_denied' and event.get('host') == 'example.com':
                        break
            except TimeoutError:
                missing.append(transport)
        assert not missing, f'missing example.com ClientHello denial on {missing}'
        watched.close()
        watch.close()
        web_read.close()
        web.close()
        print('PASS real proxy and mismatched ClientHello denials reach Unix and WebSocket feeds', flush=True)
        wait(lambda: credential.read_text() == '{"token":1}', 'credential copy-back')
        for relative, before in original_cli.items():
            if relative not in ['.claude/.credentials.json', '.codex/auth.json']:
                assert (human / relative).read_bytes() == before, relative
        for relative in ['.claude/agents', '.claude/new-dir', '.codex/new-dir']:
            assert not (human / relative).exists(), relative
        assert result['late_file_hidden'] and result['late_socket_hidden'], result
        print('PASS private CLI writes and late host file/socket isolation', flush=True)
        def action(name, data, crash=False):
            work = root / name
            (work / 'action-result').unlink(missing_ok=True)
            (work / 'probe-input.json').write_text(json.dumps(data))
            ctl({'command': 'send', 'to': '@' + name, 'body': 'isolation-probe' + (' die-before' if crash else ''), 'key': str(time.monotonic())})
            wait(lambda: (work / 'action-result').exists(), 'action ' + data['action'])
        bhome = state / 'agents' / b['id'].split(':', 1)[1] / 'home'
        chome = state / 'agents' / c['id'].split(':', 1)[1] / 'home'
        action('b', {'action': 'refresh', 'token': 1})
        wait(lambda: json.loads(codex_credential.read_text())['token'] == 1, 'host refresh')
        wait(lambda: json.loads((chome / '.codex/auth.json').read_text())['token'] == 1, 'sibling refresh')
        action('b', {'action': 'refresh', 'token': 0})
        time.sleep(2.2)
        assert json.loads(codex_credential.read_text())['token'] == 1, 'stale cached token clobbered host'
        codex_credential.write_text('{"token":99}')
        action('b', {'action': 'refresh', 'token': 2})
        time.sleep(2.2)
        assert json.loads(codex_credential.read_text())['token'] == 99, 'human refresh clobbered'
        print('PASS rename refresh reaches host and sibling; stale and external updates preserved', flush=True)
        secret = root / 'host-secret'
        secret.write_text('{"host_secret":true}')
        action('b', {'action': 'symlink', 'secret': str(secret)}, crash=True)
        wait(lambda: any(row['id'] == b['id'] and row['phase'] == 'failed' for row in ctl({'command': 'ls'})), 'symlink relaunch fails closed', seconds=30)
        assert json.loads(codex_credential.read_text())['token'] == 99
        assert secret.read_text() == '{"host_secret":true}'
        assert (bhome / '.codex/auth.json').is_symlink()
        assert next(row for row in ctl({'command': 'ls'}) if row['id'] == b['id'])['isolation'] == 'none'
        print('PASS private credential symlink blocks sync and relaunch without reading host secret', flush=True)
        ctl({'command': 'cancel', 'to': '@a'})
        wait(lambda: any(row['id'] == a['id'] and row['phase'] == 'idle' for row in ctl({'command': 'ls'})), 'a cancelled')
        victim = root / 'outside-seed'
        action('a', {'action': 'restart', 'victim': str(victim), 'marker': marker})
        wait(lambda: (root / 'a/action-result').read_text() == 'replayed', 'crash replay')
        wait(lambda: any(row['id'] == a['id'] and row['phase'] == 'idle' for row in ctl({'command': 'ls'})), 'socket restart')
        assert not victim.exists(), 'seed followed dangling symlink'
        import stat
        assert stat.S_ISSOCK((state / 'agents' / a['id'].split(':', 1)[1] / 'io/mcp.sock').stat().st_mode)
        print('PASS dangling seed symlink creates no host file and planted mcp.sock self-heals', flush=True)
        assert not proc_scan(b'AGENTD_MCP_TOKEN=', 'environ'), 'host-wide token env leak'
        assert not proc_scan(b'AGENTD_MCP_TOKEN', 'cmdline'), 'host-wide token argv leak'
        print('PASS host-wide readable /proc argv and environ scan: no MCP token; kernel-denied files are not inspected', flush=True)
        assert all(row['isolation'] == 'bwrap+proxy' for row in ctl({'command': 'ls'}) if row['phase'] == 'idle')
    if not baseline:
        wait(lambda: proc_scan(marker.encode(), 'cmdline'), 'detached descendant')
    recorded = process_tree(daemon.pid)
    assert recorded, 'no child processes captured for teardown check'
    daemon.send_signal(signal.SIGKILL if hard_exit else signal.SIGTERM)
    daemon.wait(timeout=10)
    if not hard_exit:
        assert daemon.returncode == 0
    # The mailroom records the socket identity, regardless of request contents.
    with sqlite3.connect(state / 'mail.db') as db:
        senders = [json.loads(row[0]) for row in db.execute("SELECT sender FROM messages WHERE body='peer payload'")]
        assert senders == [{'Agent': a['id'].split(':', 1)[1]}], senders
    print('PASS own shim mail attributed to @a', flush=True)
    if not baseline:
        until = time.monotonic() + 5
        while (surviving(recorded) or proc_scan(marker.encode(), 'cmdline')) and time.monotonic() < until:
            time.sleep(.02)
        assert not proc_scan(marker.encode(), 'cmdline'), 'sandbox descendant survived agentd'
        assert not surviving(recorded), f'captured sandbox processes survived: {surviving(recorded)}'
        print(f'PASS no sandboxed process survived agentd exit; checked {len(recorded)} host PIDs and start times', flush=True)
    success = True
finally:
    if daemon.poll() is None:
        daemon.terminate()
        daemon.wait(timeout=10)
    log.close()
    if success and '--keep' not in sys.argv:
        shutil.rmtree(root)
