"""Opt-in live gates. Uses existing logins without changing expiry or auth state."""
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time

binary = str(Path(sys.argv[1]).resolve())
tools = Path(sys.argv[2]).resolve()
root = Path(tempfile.mkdtemp(prefix='agentd-live-isolation-'))
state = root / 'state'
state.mkdir(mode=0o700)
home = Path.home()
config = '[bwrap]\nro_binds = ' + json.dumps([str(p) for p in [home / '.local/share/mise/installs', home / '.cargo/bin', tools] if p.exists()]) + '\n'
for name in ['claude', 'codex']:
    (root / name).mkdir()
    config += f'[kind.{name}]\ncommand = [{json.dumps(str(tools / "node_modules/.bin" / (name + "-agent-acp" if name == "claude" else "codex-acp")))}]\nidle_after_secs = 0\n'
(root / 'config.toml').write_text(config)
credentials = [home / '.claude/.credentials.json', home / '.codex/auth.json']
def hashes():
    return [hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None for p in credentials]
before = hashes()
os.environ['RUST_LOG'] = 'warn'
log = open(root / 'daemon.log', 'wb')
daemon = subprocess.Popen([binary, '--config', str(root / 'config.toml'), '--state-dir', str(state)], stderr=log, stdout=subprocess.DEVNULL)

def ctl(request):
    with socket.socket(socket.AF_UNIX) as s:
        s.settimeout(10)
        s.connect(str(state / 'agentd.sock'))
        s.sendall(json.dumps(request).encode() + b'\n')
        return json.loads(s.makefile('rb').readline())

try:
    deadline = time.monotonic() + 30
    while not (state / 'agentd.sock').exists():
        assert daemon.poll() is None, (root / 'daemon.log').read_text()
        assert time.monotonic() < deadline, 'listen timeout'
        time.sleep(.05)
    watch = socket.socket(socket.AF_UNIX)
    watch.settimeout(180)
    watch.connect(str(state / 'agentd.sock'))
    watch.sendall(b'{"command":"watch"}\n')
    events = watch.makefile('rb')
    events.readline()
    for name in (sys.argv[3:] or ['claude', 'codex']):
        response = ctl({'command': 'spawn', 'spec': {'name': name, 'kind': name, 'cwd': str(root / name), 'model': None, 'permissions': {'sandbox': 'ReadOnly', 'rules': [], 'timeout': None}, 'idle_after': None}, 'first_message': 'Reply with exactly pong. Do not use tools.', 'key': name})
        assert 'result' in response, response
        agent = response['result']['id'].split(':', 1)[1]
        output = ''
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            line = events.readline()
            assert line, 'event stream closed'
            event = json.loads(line)
            print(json.dumps(event), flush=True)
            if event.get('agent') != agent:
                continue
            if event.get('event') == 'activity':
                output += event.get('text', '')
            if event.get('event') == 'phase_changed' and event.get('phase') in ['failed', 'crashed', 'needs-recovery']:
                raise AssertionError(f'{name} failed; inspect daemon log at {root}')
            if event.get('event') == 'turn_ended':
                assert event['outcome'] == 'Completed' and 'pong' in output.lower(), (name, event, output)
                print(f'PASS live {name} proxy turn: pong', flush=True)
                break
        else:
            raise AssertionError(f'{name} live gate deadline')
    watch.close()
finally:
    daemon.send_signal(signal.SIGTERM)
    daemon.wait(timeout=15)
    log.close()
    print(json.dumps({'root': str(root), 'credential_hash_unchanged': [a == b for a, b in zip(before, hashes())]}), flush=True)
