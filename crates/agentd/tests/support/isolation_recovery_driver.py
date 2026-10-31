"""Availability and explicit reauthorization through the real daemon's control socket."""
from contextlib import closing
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time

agentd, fake = [str(Path(p).resolve()) for p in sys.argv[1:3]]

class Daemon:
    def __init__(self, binds=None, sandbox='bwrap'):
        self.tmp = tempfile.TemporaryDirectory(prefix='agentd-recovery-')
        self.root = Path(self.tmp.name)
        self.home = self.root / 'home'
        self.home.mkdir()
        (self.home / '.claude').mkdir()
        (self.home / '.codex').mkdir()
        self.state = self.root / 'state'
        self.state.mkdir(mode=0o700)
        self.work = self.root / 'work'
        self.work.mkdir()
        self.config = self.root / 'config.toml'
        self.config.write_text(f'sandbox = {json.dumps(sandbox)}\n[bwrap]\nro_binds = {json.dumps([str(Path(fake).parent)] + (binds or []))}\n' + ''.join(
            f'[kind.{kind}]\ncommand = [{json.dumps(fake)}]\nmode = {json.dumps(mode)}\nidle_after_secs = 0\n[kind.{kind}.env]\nFAKE_ROOT_FROM_CWD = "1"\nFAKE_MODE = {json.dumps(mode)}\n'
            for kind, mode in [('claude', 'default'), ('codex', 'read-only')]))
        self.proc = None
        self.log = open(self.root / 'daemon.log', 'ab')
    def __enter__(self):
        return self
    def __exit__(self, *args):
        self.stop()
        self.log.close()
        self.tmp.cleanup()
    def start(self, *args):
        self.proc = subprocess.Popen([agentd, '--config', str(self.config), '--state-dir', str(self.state), *args], env=dict(os.environ, HOME=str(self.home)), stdout=self.log, stderr=self.log)
        self.wait(lambda: self.call({'command': 'ls'}), 'control ready', allow_empty=True)
    def stop(self):
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(timeout=15)
    def wait(self, fn, label, allow_empty=False):
        until = time.monotonic() + 30
        while time.monotonic() < until:
            assert self.proc.poll() is None, (self.root / 'daemon.log').read_text()
            try:
                value = fn()
                if value or (allow_empty and value is not None):
                    return value
            except (FileNotFoundError, ConnectionRefusedError):
                pass
            time.sleep(.05)
        raise AssertionError(f'{label}: {(self.root / "daemon.log").read_text()}')
    def call(self, req):
        with socket.socket(socket.AF_UNIX) as s:
            s.settimeout(10)
            s.connect(str(self.state / 'agentd.sock'))
            s.sendall(json.dumps(req).encode() + b'\n')
            return json.loads(s.makefile('rb').readline())
    def spawn(self, name='a', kind='claude', cwd=None):
        return self.call({'command': 'spawn', 'spec': {'name': name, 'kind': kind, 'cwd': str(cwd or self.work), 'permissions': {'sandbox': 'ReadOnly', 'rules': [], 'timeout': None}}, 'key': str(time.monotonic_ns())})
    def row(self, name='a', phase=None):
        return next((r for r in self.call({'command': 'ls'})['result'] if r['name'] == '@' + name and (phase is None or r['phase'] == phase)), None)
    def settled(self, name='a', phase='idle'):
        return self.wait(lambda: self.row(name, phase), f'{name} {phase}')
    def edit_spec(self, fn):
        with closing(sqlite3.connect(self.state / 'mail.db')) as db:
            agent, raw = db.execute('SELECT agent, spec FROM agent_specs').fetchone()
            spec = json.loads(raw)
            fn(spec['workspace'])
            db.execute('UPDATE agent_specs SET spec=? WHERE agent=?', (json.dumps(spec), agent))
            db.commit()

def stable_pin():
    with Daemon() as d:
        d.start()
        assert 'error' not in d.spawn()
        d.settled()
        d.stop()
        d.edit_spec(lambda p: p.update(dev=os.stat(d.work).st_dev + 999))
        binds = d.state / 'sandbox-binds.json'
        pins = json.loads(binds.read_text())
        for pin in pins.values():
            pin['dev'] = os.stat(pin['path']).st_dev + 999
        binds.write_text(json.dumps(pins))
        d.start()
        d.settled()
        d.stop()
        d.edit_spec(lambda p: (p.pop('identity', None), p.update(dev=1)))
        for pin in pins.values():
            pin.pop('identity', None)
        binds.write_text(json.dumps(pins))
        d.start()
        d.settled()
        d.stop()
        with closing(sqlite3.connect(d.state / 'mail.db')) as db:
            assert 'identity' in json.loads(db.execute('SELECT spec FROM agent_specs').fetchone()[0])['workspace']

def legacy_swap():
    for case in ['workspace', 'workspace-same-inode', 'bind']:
        with tempfile.TemporaryDirectory(prefix='agentd-legacy-bind-') as tmp:
            source = Path(tmp) / 'tool'
            source.mkdir()
            with Daemon([str(source)] if case == 'bind' else None) as d:
                d.start()
                d.spawn()
                d.settled()
                d.stop()
                original = d.work if case.startswith('workspace') else source
                if case.startswith('workspace'):
                    d.edit_spec(lambda p: (p.pop('identity'), p.update(dev=1)))
                else:
                    file = d.state / 'sandbox-binds.json'
                    pins = json.loads(file.read_text())
                    pins[str(source)].pop('identity')
                    pins[str(source)]['dev'] = 1
                    file.write_text(json.dumps(pins))
                moved = original.with_suffix('.moved')
                original.rename(moved)
                wire_before = (moved / 'wire.jsonl').read_bytes() if case.startswith('workspace') else None
                target = moved if case == 'workspace-same-inode' else d.root / 'secret'
                target.mkdir(exist_ok=True)
                original.symlink_to(target, target_is_directory=True)
                d.start()
                failed = d.settled(phase='failed')
                d.stop()
                assert 'identity changed' in failed['error'], failed
                if target == moved:
                    assert (target / 'wire.jsonl').read_bytes() == wire_before, 'victim relaunched in relocated path'
                else:
                    assert not (target / 'wire.jsonl').exists(), 'victim executed in swapped target'
                if case == 'bind':
                    assert '--repin-binds' in failed['error'], failed
                    retained = json.loads(file.read_text())[str(source)]
                    assert retained['path'] == pins[str(source)]['path']
                    assert retained['ino'] == pins[str(source)]['ino']
                    assert retained.get('identity') is None
                else:
                    assert 'human spawn' in failed['error'], failed
                    with closing(sqlite3.connect(d.state / 'mail.db')) as db:
                        pin = json.loads(db.execute('SELECT spec FROM agent_specs').fetchone()[0])['workspace']
                        assert pin.get('identity') is None
                print('PASS legacy', case, 'rejects swapped path without upgrading pin', flush=True)

def dangling_parent():
    for case in ['absolute', 'relative-multihop']:
        with Daemon() as d:
            parent = d.home / '.codex'
            parent.rmdir()
            dotfiles = d.home / 'dotfiles'
            dotfiles.mkdir()
            target = dotfiles / 'missing' / 'codex'
            if case == 'absolute':
                parent.symlink_to(target, target_is_directory=True)
            else:
                alias = d.root / 'alias'
                alias.symlink_to(dotfiles, target_is_directory=True)
                hop = d.home / 'credential-hop'
                hop.symlink_to('../alias/missing/../missing/codex', target_is_directory=True)
                parent.symlink_to('credential-hop', target_is_directory=True)
            d.start()
            rejected = d.spawn('exposed', cwd=dotfiles)
            assert 'protected paths' in str(rejected), rejected
            assert not target.exists(), 'target must still be dangling'
            d.spawn()
            d.settled()
            print('PASS dangling credential parent', case, 'protects future target', flush=True)

def corrupt_identity(pin):
    if 'identity' in pin and 'Fsid' in pin['identity']:
        pin['identity']['Fsid'][0] ^= 1
    else:
        pin['ino'] += 1

def workspace_recovery():
    with Daemon() as d:
        d.start()
        original = d.spawn()['result']['id']
        d.settled()
        d.stop()
        d.edit_spec(corrupt_identity)
        d.start()
        failed = d.settled(phase='failed')
        assert 'human spawn' in failed['error'], failed
        d.call({'command': 'send', 'to': '@a', 'body': 'queued before recovery', 'key': 'mail'})
        assert 'error' in d.spawn(cwd=d.state), 'unsafe cwd must not authorize a new pin'
        assert d.row()['phase'] == 'failed'
        assert d.spawn()['result']['id'] == original
        d.settled()
        assert len(d.call({'command': 'ls'})['result']) == 1
        assert 'error' in d.spawn(), 'healthy actor must not be replaced'
        d.wait(lambda: any('queued before recovery' in line for line in (d.work / 'wire.jsonl').read_text().splitlines()), 'queued mail delivered')
        d.stop()
        with closing(sqlite3.connect(d.state / 'mail.db')) as db:
            assert db.execute('SELECT count(*) FROM agent_specs').fetchone()[0] == 1

def bind_recovery():
    with Daemon() as d:
        d.start()
        d.spawn()
        d.settled()
        d.stop()
        path = d.state / 'sandbox-binds.json'
        pins = json.loads(path.read_text())
        entry = next(iter(pins))
        corrupt_identity(pins[entry])
        path.write_text(json.dumps(pins))
        d.start()
        failed = d.settled(phase='failed')
        assert 'sandbox-binds.json entry ' + entry in failed['error'] and '--repin-binds' in failed['error'], failed
        d.stop()
        d.start('--repin-binds')
        d.settled()
        d.stop()
        assert json.loads(path.read_text())[entry]['ino'] == os.stat(entry).st_ino

def credentials(selected=None):
    for case in selected or ['missing', '70k', '256k', 'oversize', 'symlink', 'directory', 'fifo', 'parent-file']:
        with Daemon() as d:
            claude = d.home / '.claude'
            claude.mkdir(exist_ok=True)
            leaf = d.home / '.codex/auth.json'
            if case == 'missing':
                leaf.parent.rmdir()
            else:
                if case in ['70k', '256k', 'oversize']:
                    size = {'70k': 70000, '256k': 256 * 1024, 'oversize': 256 * 1024 + 1}[case]
                    leaf.write_bytes(b'{"token":"' + b'x' * (size - 12) + b'"}')
                elif case == 'symlink':
                    leaf.symlink_to(claude / 'absent')
                elif case == 'directory':
                    leaf.mkdir()
                elif case == 'fifo':
                    os.mkfifo(leaf)
                elif case == 'parent-file':
                    leaf.parent.rmdir()
                    leaf.parent.write_text('notdir')
            d.start()
            d.spawn()
            d.settled()
            workspace = d.root / 'codex-work'
            workspace.mkdir()
            d.spawn('b', 'codex', workspace)
            valid = case in ['missing', '70k', '256k']
            row = d.settled('b', 'idle' if valid else 'failed')
            if not valid:
                assert 'credential' in row['error'] and str(leaf) in row['error'], row
                assert row['error'] in (d.root / 'daemon.log').read_text()
                if case == 'oversize':
                    assert '256 KiB' in row['error']
                elif case == 'symlink':
                    assert 'symlink' in row['error']
                if case == 'directory':
                    leaf.rmdir()
                elif case == 'parent-file':
                    leaf.parent.unlink()
                    leaf.parent.mkdir()
                else:
                    leaf.unlink()
                leaf.write_text('{"token":1}')
                assert 'error' not in d.spawn('b', 'codex', workspace)
                d.settled('b')
            elif case == 'missing':
                target = d.root / 'login'
                target.mkdir()
                leaf.parent.symlink_to(target, target_is_directory=True)
                assert 'error' in d.spawn('unsafe', 'claude', target)
                leaf.write_text('{"token":1}')
                d.call({'command': 'send', 'to': '@b', 'body': 'die-before', 'key': 'reopen-login'})
                row = d.row('b')
                agent = row['id'].removeprefix('agent:')
                copied = d.state / 'agents' / agent / 'home/.codex/auth.json'
                d.wait(lambda: copied.exists() and json.loads(copied.read_text()) == {'token': 1}, 'credential resolved on later launch')
            print(f'PASS credential {case}', flush=True)

def binds(selected=None):
    for case in selected or ['missing', 'fifo', 'file-ro', 'file-rw', 'none']:
        with tempfile.TemporaryDirectory(prefix='agentd-bind-') as tmp:
            source = Path(tmp) / 'source'
            if case.startswith('file'):
                source.write_text('operator grant')
            elif case == 'fifo':
                os.mkfifo(source)
            with Daemon([str(source)], 'none' if case == 'none' else 'bwrap') as d:
                if case == 'file-rw':
                    config = d.config.read_text().replace(f', {json.dumps(str(source))}', '')
                    d.config.write_text(config.replace('\n[kind.claude]', f'\nrw_binds = [{json.dumps(str(source))}]\n[kind.claude]', 1))
                if case in ['missing', 'fifo']:
                    d.config.write_text(d.config.read_text().replace('[kind.claude]\n', '[kind.claude]\nsandbox = "none"\n'))
                d.start()
                d.spawn()
                d.settled()
                if case != 'none':
                    d.spawn('b', 'codex')
                    row = d.settled('b', 'failed' if case in ['missing', 'fifo'] else 'idle')
                    if case in ['missing', 'fifo']:
                        assert str(source) in row['error'] and 'sandbox-binds.json' in row['error'], row
                    else:
                        assert 'error' in d.spawn('file-workspace', 'codex', source)
                        d.stop()
                        source.rename(source.with_suffix('.old'))
                        source.write_text('replacement')
                        d.start()
                        d.settled('b', 'failed')
                        d.stop()
                        d.start('--repin-binds')
                        d.settled('b')
                        d.stop()
                        saved = json.loads((d.state / 'sandbox-binds.json').read_text())[str(source)]
                        source.unlink()
                        source.symlink_to(d.state, target_is_directory=True)
                        d.start('--repin-binds')
                        d.settled('b', 'failed')
                        d.stop()
                        assert json.loads((d.state / 'sandbox-binds.json').read_text())[str(source)] == saved
                print(f'PASS bind {case}', flush=True)

cases = {'legacy-swap': legacy_swap, 'dangling-parent': dangling_parent, 'stable-pin': stable_pin, 'workspace-recovery': workspace_recovery, 'bind-recovery': bind_recovery, 'credentials': credentials, 'binds': binds}
for name in sys.argv[3:] or cases:
    key, _, subcase = name.partition('/')
    cases[key]([subcase]) if subcase else cases[key]()
    print('PASS', name, flush=True)
