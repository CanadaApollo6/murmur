"""Agent-planted filesystem state must fail only that agent's next launch."""
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time


def driver(binary, fake):
    with tempfile.TemporaryDirectory(prefix='agentd-relaunch-') as temporary:
        root = Path(temporary)
        home = root / 'human'
        home.mkdir()
        work = root / 'work'
        work.mkdir()
        state = root / 'state'
        (work / 'probe.py').write_text('''
from pathlib import Path
import sys
socket = Path('/run/agentd/io/mcp.sock')
socket.unlink()
nested = socket
for _ in range(40):
    nested /= 'd'
nested.mkdir(parents=True)
(Path(sys.argv[1]) / 'planted').touch()
''')
        config = '[bwrap]\nro_binds = [' + json.dumps(str(Path(fake).parent)) + ']\n'
        for kind in ['claude', 'codex']:
            config += f'[kind.{kind}]\ncommand = [{json.dumps(fake)}]\ncredentials = []\n'
            config += f'[kind.{kind}.env]\nFAKE_ROOT_FROM_CWD = "1"\nFAKE_MODE = "default"\n'
        (root / 'config.toml').write_text(config)
        with (root / 'daemon.log').open('wb') as log:
            daemon = subprocess.Popen([binary, '--config', str(root / 'config.toml'), '--state-dir', str(state)],
                                      env=dict(os.environ, HOME=str(home)), stdout=log, stderr=log)

            def ctl(request):
                with socket.socket(socket.AF_UNIX) as sock:
                    sock.settimeout(5)
                    sock.connect(str(state / 'agentd.sock'))
                    sock.sendall(json.dumps(request).encode() + b'\n')
                    reply = json.loads(sock.makefile('rb').readline())
                    assert 'error' not in reply, reply
                    return reply['result']

            def wait(predicate):
                until = time.monotonic() + 30
                while time.monotonic() < until:
                    assert daemon.poll() is None, (root / 'daemon.log').read_text()
                    value = predicate()
                    if value:
                        return value
                    time.sleep(.02)
                raise AssertionError('timeout: ' + (root / 'daemon.log').read_text())

            def phase(name, expected):
                return next((r for r in ctl({'command': 'ls'})
                             if r['name'] == '@' + name and r['phase'] == expected), None)

            def spawn(name, cwd):
                return ctl({'command': 'spawn', 'spec': {'name': name, 'kind': 'claude', 'cwd': str(cwd),
                            'permissions': {'sandbox': 'ReadOnly', 'rules': [], 'timeout': None}}, 'key': name})

            try:
                wait(lambda: (state / 'agentd.sock').exists())
                spawn('attacker', work)
                wait(lambda: phase('attacker', 'idle'))
                ctl({'command': 'send', 'to': '@attacker', 'body': 'isolation-probe', 'key': 'plant'})
                wait(lambda: (work / 'planted').exists())
                ctl({'command': 'send', 'to': '@attacker', 'body': 'die-before', 'key': 'crash'})
                failed = wait(lambda: phase('attacker', 'failed'))
                assert 'socket cleanup exceeds' in failed['error'], failed
                healthy = root / 'healthy'
                healthy.mkdir()
                spawn('healthy', healthy)
                wait(lambda: phase('healthy', 'idle'))
                print('PASS oversized planted socket tree rejects relaunch; broker and another sandboxed agent remain available', flush=True)
            finally:
                daemon.terminate()
                try:
                    daemon.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    daemon.kill()
                    daemon.wait(timeout=5)


if __name__ == '__main__':
    driver(*sys.argv[1:])
