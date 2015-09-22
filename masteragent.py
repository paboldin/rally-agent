#!/usr/bin/python

import cgi
import collections
import datetime
import json
import six
import threading
import time
import uuid
import zmq

try:
    import Queue as queue
except:
    import queue

publish_context = zmq.Context()
publish_socket = publish_context.socket(zmq.PUB)
publish_socket.bind("tcp://*:1234")

pull_context = zmq.Context()
pull_socket = pull_context.socket(zmq.PULL)
pull_socket.bind("tcp://*:1235")

req_queue = queue.Queue()
missed_queue = collections.defaultdict(list)


def recv_thread(req_queue):
    while True:
        req, conf, queue = req_queue.get()
        if req is None:
            return
        timeout = conf["timeout"]
        tstart = datetime.datetime.now()
        if queue is not None:
            queue.extend(missed_queue.pop(req, []))
        while pull_socket.poll(timeout) and \
                len(queue) <= conf["agents"]:
            resp = pull_socket.recv_json()
            if resp["req"] != req:
                missed_queue[resp["req"]].append(resp)
            else:
                queue.append(resp)
            timeout = max(
                timeout -
                    (datetime.datetime.now() - tstart).total_seconds() * 1000,
                0)
        req_queue.task_done()


def make_flat(form):
    d = {}
    for k in form.keys():
        if isinstance(form[k], list):
            d[k] = [x.value for x in form[k]]
        else:
            d[k] = form[k].value
    return d


class PostHandler(six.moves.BaseHTTPServer.BaseHTTPRequestHandler):
    config = dict(timeout=1000, agents=float("+inf"))

    def configure(self, query):
        config = dict(six.moves.urllib.parse.parse_qsl(query))
        self.config["timeout"] = float(
            config.get("timeout", self.config["timeout"])
        )
        self.config["agents"] = float(
            config.get("agents", self.config["agents"])
        )
        self.send_response(200)

    def missed(self):
        req_queue.put(("", {"timeout": 10000, "agents": float('+Inf')}, []))
        req_queue.join()

        self.send_response(200)
        self.end_headers()
        self.wfile.write("Missed: %s\n" % json.dumps(missed_queue))
        if self.command == "DELETE":
            missed_queue.clear()

    def do_PUT(self):

        url = six.moves.urllib.parse.urlparse(self.path)
        if url.path == "/configure":
            return self.configure(url.query)

        self.send_response(404)

    def do_GET(self):
        url = six.moves.urllib.parse.urlparse(self.path)
        if url.path == "/missed":
            return self.missed()

        self.send_response(404)

    def do_DELETE(self):
        url = six.moves.urllib.parse.urlparse(self.path)
        if url.path == "/missed":
            return self.missed()
        
        self.send_response(404)

    def do_POST(self):
        form = cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={
                'REQUEST_METHOD':'POST',
                'CONTENT_TYPE':self.headers['Content-Type'],
            }
        )

        req = str(uuid.uuid4())

        d = make_flat(form)
        d["req"] = req
        publish_socket.send_json(d)

        r = []
        req_queue.put((req, self.config, r))
        req_queue.join()

        self.send_response(200)
        self.end_headers()
        self.wfile.write("Response: %s\n" % json.dumps(r))

if __name__ == "__main__":
    t = threading.Thread(target=recv_thread, args=(req_queue,))
    t.deamon = True
    t.start()
    server = six.moves.BaseHTTPServer.HTTPServer(
        ('localhost', 8080), PostHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        req_queue.put((None, None, None))
