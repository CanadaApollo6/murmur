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
credential_dir = root / 'credentials'
credential_dir.mkdir()
credential = credential_dir / 'dummy.json'
credential.write_text('old dummy credential')
for name in ['a', 'b']:
    (root / name).mkdir()
    shutil.copy(Path(__file__).with_name('isolation_probe.py'), root / name / 'probe.py')
mode = 'none' if baseline else 'bwrap'
config = f'sandbox = "{mode}"\n[bwrap]\nro_binds = [{json.dumps(fake)}]\n'
for kind, name, model in [('claude', 'a', 'default'), ('codex', 'b', 'read-only')]:
    config += f'[kind.{kind}]\ncommand = [{json.dumps(fake)}]\nmode = "{model}"\nidle_after_secs = 0\n'
    if not baseline:
        config += f'credentials = [{json.dumps(str(credential))}]\negress = ["localhost", "example.com"]\n'
    config += f'[kind.{kind}.env]\nFAKE_ROOT = {json.dumps(str(root / name))}\nFAKE_MODE = {json.dumps(model)}\n'
if not baseline:
    config += '[kind.infinity]\nwire = "infinity"\nsocket = "/nonexistent/infinity.sock"\n'
(root / 'config.toml').write_text(config)
# Old main does not understand isolation configuration.
if baseline:
    (root / 'config.toml').write_text('\n'.join(line for line in config.splitlines() if not line.startswith(('sandbox =', '[bwrap]', 'ro_binds ='))))
log = open(root / 'daemon.log', 'wb')
daemon = subprocess.Popen([binary, '--config', str(root / 'config.toml'), '--state-dir', str(state)], stdout=subprocess.DEVNULL, stderr=log)

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

def wire_ready(name):
    p = root / name / 'wire.jsonl'
    return p.exists() and 'session/set_mode' in p.read_text()

try:
    wait(lambda: (state / 'agentd.sock').exists(), 'listen')
    b = spawn('b', 'codex')
    wait(lambda: wire_ready('b'), 'b idle')
    # The shim carries the sibling token on main. In the sandbox it carries none.
    sibling = wait(lambda: proc_scan(str(root / 'b').encode(), 'environ' if baseline else 'cmdline'), 'sibling host PID')[0]
    a = spawn('a', 'claude')
    wait(lambda: wire_ready('a'), 'a idle')
    marker = str(root / 'detached-marker')
    config = {'baseline': baseline, 'shared': str(state / 'mcp.sock'), 'credential': str(credential), 'marker': marker,
              'paths': {'control': str(state / 'agentd.sock'), 'state': str(state), 'sqlite': str(state / 'mail.db'),
                        'sibling_environ': f'/proc/{sibling}/environ', 'sibling_cmdline': f'/proc/{sibling}/cmdline', 'sibling_mcp': str(state / 'agents' / b['id'].split(':', 1)[1] / 'io/mcp.sock')}}
    if not baseline:
        assert 'mixed isolation: kind infinity is unsandboxed' in (root / 'daemon.log').read_text()
    # Find the sibling shim itself for the token theft baseline.
    if baseline:
        sibling = next(pid for pid in proc_scan(b'AGENTD_MCP_TOKEN=', 'environ') if str(state / 'mcp.sock').encode() in Path(f'/proc/{pid}/cmdline').read_bytes())
        config['paths']['sibling_environ'] = f'/proc/{sibling}/environ'
        config['paths']['sibling_cmdline'] = f'/proc/{sibling}/cmdline'
    (root / 'a/probe-input.json').write_text(json.dumps(config))
    ctl({'command': 'send', 'to': '@a', 'body': 'isolation-probe mcp-send hold', 'key': 'probe'})
    wait(lambda: (root / 'a/probe-result.json').exists(), 'probe')
    wait(lambda: (root / 'a/mcp-result.json').exists(), 'own shim')
    result = json.loads((root / 'a/probe-result.json').read_text())
    print(json.dumps({'root': str(root), 'baseline': baseline, 'probe': result}), flush=True)
    if baseline:
        assert all(result[k] for k in ['control', 'state', 'sqlite', 'sibling_environ', 'sibling_cmdline', 'forged_agent']), result
    else:
        assert not any(result[k] for k in ['control', 'state', 'sqlite', 'sibling_environ', 'sibling_cmdline', 'forged_agent', 'own_token', 'direct_network', 'sibling_mcp']), result
        assert all(result[k] for k in ['token_rejected', 'rename', 'proxy_deny', 'proxy_private_deny', 'proxy_allow']), result
        assert credential.read_text() == 'refreshed dummy credential'
        assert not proc_scan(b'AGENTD_MCP_TOKEN=', 'environ'), 'host-wide token env leak'
        assert not proc_scan(b'AGENTD_MCP_TOKEN', 'cmdline'), 'host-wide token argv leak'
        print('PASS host-wide readable /proc argv and environ scan: no MCP token; kernel-denied files are not inspected', flush=True)
        assert all(row['isolation'] == 'bwrap+proxy' for row in ctl({'command': 'ls'}))
    if not baseline:
        wait(lambda: proc_scan(marker.encode(), 'cmdline'), 'detached descendant')
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
        while proc_scan(marker.encode(), 'cmdline') and time.monotonic() < until:
            time.sleep(.02)
        assert not proc_scan(marker.encode(), 'cmdline'), 'sandbox descendant survived agentd'
        assert not proc_scan(str(root / 'a').encode(), 'environ'), 'ACP survived agentd'
        print('PASS no sandboxed process survived agentd exit', flush=True)
finally:
    if daemon.poll() is None:
        daemon.terminate()
        daemon.wait(timeout=10)
    log.close()
