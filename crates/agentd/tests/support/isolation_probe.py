import json
import os
from pathlib import Path
import socket
import subprocess
import sys

root = Path(sys.argv[1])
cfg = json.loads((root / 'probe-input.json').read_text())
result = {}

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
    credential = Path(cfg['credential'])
    temporary = credential.with_name('refresh.tmp')
    temporary.write_text('refreshed dummy credential')
    temporary.replace(credential)
    result['rename'] = credential.read_text() == 'refreshed dummy credential'
    def proxy(host):
        with socket.create_connection(('127.0.0.1', 3128), timeout=10) as s:
            s.sendall(f'CONNECT {host}:443 HTTP/1.1\r\nHost: {host}:443\r\n\r\n'.encode())
            return s.recv(4096).decode()
    denied = proxy('attacker.invalid')
    result['proxy_deny'] = '403' in denied and 'blocked by agentd egress policy for @a' in denied
    result['proxy_allow'] = '200 Connection Established' in proxy('example.com')
    result['proxy_private_deny'] = '403' in proxy('localhost')
    try:
        socket.create_connection(('1.1.1.1', 443), timeout=0.2)
        result['direct_network'] = True
    except OSError:
        result['direct_network'] = False
    # This detached descendant must die with the PID namespace, even after SIGKILL.
    subprocess.Popen(['/usr/bin/python3', '-c', 'import time; time.sleep(600)', cfg['marker']], start_new_session=True, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
(root / 'probe-result.json').write_text(json.dumps(result))
