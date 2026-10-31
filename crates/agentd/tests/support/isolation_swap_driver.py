"""Real-daemon nested workspace and configured-bind identity regressions."""
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time

agentd, fake = [str(Path(p).resolve()) for p in sys.argv[1:3]]
PROBE_A = '''
import os, sys
from pathlib import Path
ws = Path(os.environ['WS']); target = os.environ['TARGET']
os.rename(ws / 'sub', ws / 'sub.moved'); os.symlink(target, ws / 'sub')
(Path(sys.argv[1]) / 'swapped').write_text(os.readlink(ws / 'sub'))
'''
PROBE_B = '''
import json, os, socket, sys
from pathlib import Path
r = {'cwd': os.getcwd(), 'listing': sorted(os.listdir('.'))}
try:
    with socket.socket(socket.AF_UNIX) as s:
        s.connect('agentd.sock'); s.sendall(json.dumps({'command': 'ls'}).encode() + b'\\n')
        r['control_ls'] = json.loads(s.makefile('rb').readline())
    with socket.socket(socket.AF_UNIX) as s:
        s.connect('agentd.sock'); s.sendall(json.dumps({'command': 'send', 'to': '@a', 'body': 'forged by b', 'key': 'forge'}).encode() + b'\\n')
        r['control_send'] = json.loads(s.makefile('rb').readline())
except OSError as e:
    r['control_error'] = repr(e)
out = Path(sys.argv[1]) / ('result-%d.json' % len(list(Path(sys.argv[1]).glob('result-*.json'))))
out.write_text(json.dumps(r))
'''

def run(scenario):
    root = Path(tempfile.mkdtemp(prefix='agentd-swap-'))
    human = root / 'human'
    human.mkdir()
    state = root / 'state'
    state.mkdir(mode=0o700)
    ws = root / 'ws'
    (ws / 'sub').mkdir(parents=True)
    secret = root / 'secret'
    secret.mkdir()
    obs = {n: root / f'obs-{n}' for n in 'ab'}
    for n, code in [('a', PROBE_A), ('b', PROBE_B)]:
        obs[n].mkdir()
        (obs[n] / 'probe.py').write_text(code)
    binds = [str(obs['a']), str(obs['b'])]
    if scenario == 'bind-restart':
        binds.append(str(ws / 'sub'))
    config = f'[bwrap]\nro_binds = [{json.dumps(str(Path(fake).parent))}]\nrw_binds = {json.dumps(binds)}\n'
    config += f'[kind.codex]\ncommand = [{json.dumps(fake)}]\nmode = "read-only"\ncredentials = []\nidle_after_secs = 0\n[kind.codex.env]\nFAKE_ROOT = {json.dumps(str(obs["a"]))}\nFAKE_MODE = "read-only"\nWS = {json.dumps(str(ws))}\nTARGET = {json.dumps(str(secret if scenario == "spawn-recovery" else state))}\n'
    config += f'[kind.claude]\ncommand = [{json.dumps(fake)}]\nmode = "default"\ncredentials = []\nidle_after_secs = 0\n[kind.claude.env]\nFAKE_ROOT = {json.dumps(str(obs["b"]))}\nFAKE_MODE = "default"\n'
    cfg = root / 'config.toml'
    cfg.write_text(config)
    log = open(root / 'daemon.log', 'ab')
    def launch():
        return subprocess.Popen([agentd, '--config', str(cfg), '--state-dir', str(state)], env=dict(os.environ, HOME=str(human)), stdout=log, stderr=log)
    d = launch()
    def wait(fn, label):
        until = time.monotonic() + 35
        while time.monotonic() < until:
            assert d.poll() is None, (root / 'daemon.log').read_text()
            value = fn()
            if value:
                return value
            time.sleep(.05)
        raise AssertionError(f'{scenario}: timeout {label}: {(root / "daemon.log").read_text()}')
    def ctl(req, rejected=False):
        with socket.socket(socket.AF_UNIX) as s:
            s.settimeout(10)
            s.connect(str(state / 'agentd.sock'))
            s.sendall(json.dumps(req).encode() + b'\n')
            response = json.loads(s.makefile('rb').readline())
            if rejected:
                assert 'error' in response, response
                return response['error']
            assert 'error' not in response, response
            return response['result']
    def spawn(name, kind, cwd, rejected=False):
        return ctl({'command': 'spawn', 'spec': {'name': name, 'kind': kind, 'cwd': str(cwd), 'permissions': {'sandbox': 'ReadOnly', 'rules': [], 'timeout': None}}, 'key': name}, rejected)
    def phase(name, value):
        return next((r for r in ctl({'command': 'ls'}) if r['name'] == '@' + name and r['phase'] == value), None)
    try:
        wait(lambda: (state / 'agentd.sock').exists(), 'listen')
        spawn('a', 'codex', ws)
        spawn('b', 'claude', ws / 'sub' if scenario != 'bind-restart' else obs['b'])
        wait(lambda: phase('a', 'idle') and phase('b', 'idle'), 'initial launches')
        ctl({'command': 'send', 'to': '@a', 'body': 'isolation-probe', 'key': 'swap'})
        wait(lambda: (obs['a'] / 'swapped').exists(), 'sandboxed swap')
        if scenario in ['workspace-reopen', 'spawn-recovery']:
            ctl({'command': 'send', 'to': '@b', 'body': 'die-before', 'key': 'crash'})
        else:
            d.terminate()
            d.wait(timeout=10)
            if scenario == 'bind-restart':
                cfg.write_text(config.replace('[kind.codex]\n', '[kind.codex]\nsandbox = "none"\n'))
            d = launch()
            wait(lambda: (state / 'agentd.sock').exists(), 'restart listen')
        failed = wait(lambda: phase('b', 'failed'), 'failed-closed victim')
        assert 'identity changed' in failed['error'], failed
        assert not list(obs['b'].glob('result-*.json')), 'victim ran after swap'
        assert not (state / 'wire.jsonl').exists(), 'state became workspace'
        if scenario == 'spawn-recovery':
            for name in ['b', 'fresh']:
                error = spawn(name, 'claude', ws / 'sub', rejected=True)
                assert str(ws / 'sub') in str(error) and str(secret) in str(error), error
                assert 'agent-writable symlink' in str(error), error
            for name in ['b', 'cli-fresh']:
                reply = subprocess.run([str(Path(agentd).with_name('agentctl')), '--socket', str(state / 'agentd.sock'),
                    'spawn', name, '--kind', 'claude', '--cwd', str(ws / 'sub')],
                    env=dict(os.environ, HOME=str(human)), text=True, capture_output=True, timeout=10)
                assert reply.returncode != 0 and 'agent-writable symlink' in reply.stderr, reply
                assert str(ws / 'sub') in reply.stderr and str(secret) in reply.stderr, reply
            error = spawn('b', 'claude', secret, rejected=True)
            assert str(ws / 'sub') in str(error) and str(secret) in str(error), error
            assert 'spawn under a new name' in str(error), error
            assert phase('b', 'failed'), 'rejection must preserve the failed actor'
            assert not (secret / 'wire.jsonl').exists(), 'victim executed in swapped target'
            alias = root / 'operator-alias'
            alias.symlink_to(secret, target_is_directory=True)
            reply = spawn('explicit', 'claude', alias)
            assert reply['cwd'] == str(secret), reply
            wait(lambda: phase('explicit', 'idle'), 'explicit canonical grant')
            print('PASS failed re-spawn and fresh spawn reject planted link; canonical mismatch refused; outside symlink allowed; spawn reply includes canonical cwd', flush=True)
        if scenario == 'bind-restart':
            wait(lambda: phase('a', 'idle'), 'unaffected unsandboxed kind')
        print(f'PASS {scenario}: {failed["error"]}; no control access or forged mail', flush=True)
    finally:
        if d.poll() is None:
            d.terminate()
            d.wait(timeout=10)
        log.close()
    assert "identity changed" in (root / "daemon.log").read_text()
    with sqlite3.connect(state / 'mail.db') as db:
        assert db.execute("SELECT COUNT(*) FROM messages WHERE body = 'forged by b'").fetchone()[0] == 0
    shutil.rmtree(root)

for scenario in ['workspace-reopen', 'workspace-restart', 'bind-restart', 'spawn-recovery']:
    run(scenario)
