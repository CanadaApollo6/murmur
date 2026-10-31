import http.server
import json
import threading
import urllib.request

pending = None

def post(inv, payload):
    request = urllib.request.Request(inv['callback_url'], json.dumps(payload).encode(), {'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=3) as response:
        response.read()

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass
    def reply(self, payload):
        data = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)
    def do_GET(self):
        self.reply({'name':'choices','endpoint':f'http://127.0.0.1:{self.server.server_port}/invoke','tools':[{'name':'choose','description':'Choose','inputSchema':{'type':'object'}}]})
    def do_POST(self):
        global pending
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        self.reply({})
        if self.path == '/invoke':
            pending = body
            payload = {'type':'user_choice','group_id':body['group_id'],'id':body['id'],'call_id':body.get('call_id'),'prompt':'choose','choices':['yes'],'default':0,'response_url':f'http://127.0.0.1:{self.server.server_port}/answer'}
            threading.Thread(target=post, args=(body,payload), daemon=True).start()
        elif self.path == '/answer':
            inv = pending
            payload = {'type':'tool_result','group_id':inv['group_id'],'id':inv['id'],'call_id':inv.get('call_id'),'text':'First choice completed'}
            threading.Thread(target=post, args=(inv,payload), daemon=True).start()

server = http.server.ThreadingHTTPServer(('127.0.0.1',0), Handler)
print(json.dumps({'port':server.server_port}), flush=True)
server.serve_forever()
