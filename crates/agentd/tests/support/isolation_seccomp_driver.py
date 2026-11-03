"""Exercise denied syscalls after the broker's real sandbox launch."""
import ctypes
import errno
import json
import os
from pathlib import Path
import platform
import select
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

libc = ctypes.CDLL(None, use_errno=True)
libc.syscall.restype = ctypes.c_long
numbers = {
    'x86_64': dict(keyctl=250, add_key=248, request_key=249),
    'aarch64': dict(keyctl=219, add_key=217, request_key=218),
}[platform.machine()]
numbers.update(io_uring_setup=425, io_uring_enter=426, io_uring_register=427)


def syscall(name, *args):
    ctypes.set_errno(0)
    result = libc.syscall(ctypes.c_long(numbers[name]), *args)
    return {'ret': result, 'errno': ctypes.get_errno() if result < 0 else 0}


def read_key(key):
    data = ctypes.create_string_buffer(256)
    result = syscall('keyctl', 11, ctypes.c_long(key), data, len(data), 0)
    if result['ret'] >= 0:
        result['data'] = data.raw[:result['ret']].hex()
    return result


def af_alg_socket():
    try:
        with socket.socket(socket.AF_ALG, socket.SOCK_SEQPACKET, 0):
            return {'errno': 0}
    except OSError as error:
        return {'errno': error.errno}


def probe(root):
    config = json.loads((root / 'probe-input.json').read_text())
    mode = config['mode']
    if mode == 'vsock':
        result = {}
        try:
            with socket.socket(socket.AF_VSOCK, socket.SOCK_STREAM) as sock:
                result['socket'] = {'errno': 0}
                sock.settimeout(2)
                try:
                    sock.connect((1, config['port']))
                    sock.sendall(b'escape')
                    result['connect'] = True
                except OSError as error:
                    result['connect'] = str(error)
        except OSError as error:
            result['socket'] = {'errno': error.errno}
        try:
            pair = socket.socketpair(socket.AF_VSOCK, socket.SOCK_STREAM)
        except OSError as error:
            result['socketpair'] = {'errno': error.errno}
        else:
            for sock in pair:
                sock.close()
            result['socketpair'] = {'errno': 0}
    elif mode == 'keyring':
        result = {'ring': read_key(-3), 'key': read_key(config['key'])}
        result['add_key'] = syscall('add_key', b'user', b'sandbox-key', b'x', 1, ctypes.c_long(-3))
        result['request_key'] = syscall('request_key', b'user', b'agentd-seccomp-dummy', None, 0)
        result['af_alg'] = af_alg_socket()
    else:
        params = ctypes.create_string_buffer(120)
        result = {'io_uring_setup': syscall('io_uring_setup', 1, params)}
        if result['io_uring_setup']['ret'] >= 0:
            os.close(result['io_uring_setup']['ret'])
        result['io_uring_enter'] = syscall('io_uring_enter', -1, 0, 0, 0, None, 0)
        result['io_uring_register'] = syscall('io_uring_register', -1, 0, None, 0)
    (root / 'probe-result.json').write_text(json.dumps(result))


def driver(binary, fake, mode):
    listener = None
    config = {'mode': mode}
    if mode == 'vsock':
        try:
            listener = socket.socket(socket.AF_VSOCK, socket.SOCK_STREAM)
            listener.bind((socket.VMADDR_CID_ANY, socket.VMADDR_PORT_ANY))
            listener.listen(1)
        except OSError as error:
            if listener is not None:
                listener.close()
            print(f'SKIP vsock: host cannot create a vsock listener: {error}', flush=True)
            return
        config['port'] = listener.getsockname()[1]
    if mode == 'keyring':
        joined = syscall('keyctl', 1, None)
        assert joined['ret'] > 0, joined
        payload = b'dummy-session-secret'
        key = syscall('add_key', b'user', b'agentd-seccomp-dummy', payload, len(payload), ctypes.c_long(-3))
        assert key['ret'] > 0, key
        config['key'] = key['ret']
        assert read_key(key['ret'])['data'] == payload.hex()
        assert read_key(-3)['ret'] > 0
        # AF_ALG can read keyring keys by serial; it must work on the host for the check to mean anything.
        host_af_alg = af_alg_socket()
        assert host_af_alg['errno'] == 0, f'host cannot create an AF_ALG socket: {host_af_alg}'
    if mode == 'io_uring':
        params = ctypes.create_string_buffer(120)
        baseline = syscall('io_uring_setup', 1, params)
        if baseline['ret'] < 0:
            print(f'SKIP io_uring: unfiltered host io_uring_setup failed: {baseline}', flush=True)
            return
        os.close(baseline['ret'])
    with tempfile.TemporaryDirectory(prefix='agentd-seccomp-') as temporary:
        root = Path(temporary)
        work = root / 'work'
        work.mkdir()
        home = root / 'home'
        home.mkdir()
        state = root / 'state'
        shutil.copy(__file__, work / 'probe.py')
        (work / 'probe-input.json').write_text(json.dumps(config))
        settings = '[bwrap]\nro_binds = [' + json.dumps(str(Path(fake).parent)) + ']\n'
        for kind in ['claude', 'codex']:
            settings += f'[kind.{kind}]\ncommand = [{json.dumps(fake)}]\ncredentials = []\n'
            settings += f'[kind.{kind}.env]\nFAKE_ROOT_FROM_CWD = "1"\nFAKE_MODE = "default"\n'
        (root / 'config.toml').write_text(settings)
        with (root / 'daemon.log').open('wb') as log:
            daemon = subprocess.Popen([binary, '--config', str(root / 'config.toml'), '--state-dir', str(state)],
                                      env=dict(os.environ, HOME=str(home)), stdout=subprocess.DEVNULL, stderr=log)
            def wait(predicate):
                until = time.monotonic() + 20
                while time.monotonic() < until:
                    assert daemon.poll() is None, (root / 'daemon.log').read_text()
                    if predicate():
                        return
                    time.sleep(0.02)
                raise AssertionError(f'timeout: {(root / "daemon.log").read_text()}')

            def ctl(request):
                with socket.socket(socket.AF_UNIX) as sock:
                    sock.settimeout(10)
                    sock.connect(str(state / 'agentd.sock'))
                    sock.sendall(json.dumps(request).encode() + b'\n')
                    response = json.loads(sock.makefile('rb').readline())
                    assert 'error' not in response, response
                    return response['result']

            try:
                wait(lambda: (state / 'agentd.sock').exists())
                ctl({'command': 'spawn', 'spec': {'name': 'probe', 'kind': 'claude', 'cwd': str(work),
                     'permissions': {'sandbox': 'ReadOnly', 'rules': [], 'timeout': None},
                     'model': None, 'idle_after': None}, 'first_message': None, 'key': 'spawn'})
                wait(lambda: any(row['phase'] == 'idle' for row in ctl({'command': 'ls'})))
                ctl({'command': 'send', 'to': '@probe', 'body': 'isolation-probe', 'key': 'probe'})
                wait(lambda: (work / 'probe-result.json').exists())
                result = json.loads((work / 'probe-result.json').read_text())
                print(json.dumps({'mode': mode, 'result': result}), flush=True)
                if mode == 'vsock':
                    reached = bool(select.select([listener], [], [], 0.2)[0])
                    print(f'host vsock listener reached: {reached}', flush=True)
                    assert not reached, 'sandbox connected to host vsock listener'
                    for call in ['socket', 'socketpair']:
                        assert result[call]['errno'] == errno.EAFNOSUPPORT, result
                else:
                    if mode == 'keyring':
                        assert result.pop('af_alg')['errno'] == errno.EAFNOSUPPORT, result
                    for call, outcome in result.items():
                        assert outcome['ret'] == -1 and outcome['errno'] == errno.ENOSYS, (call, result)
                print(f'PASS {mode} through real agentd launch', flush=True)
            finally:
                daemon.send_signal(signal.SIGTERM)
                try:
                    daemon.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    daemon.kill()
                    daemon.wait(timeout=5)
                if listener is not None:
                    listener.close()


if __name__ == '__main__':
    if len(sys.argv) == 2:
        probe(Path(sys.argv[1]))
    else:
        driver(*sys.argv[1:])
