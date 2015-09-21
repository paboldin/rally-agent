#!/usr/bin/python

import cgi
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

def recv_thread(req_queue):
    while True:
        queue = req_queue.get()
        if queue is None:
            return
        while pull_socket.poll(1000):
            queue.append(pull_socket.recv_json())
        req_queue.task_done()

class PostHandler(six.moves.BaseHTTPServer.BaseHTTPRequestHandler):
    def do_POST(self):
        form = cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={
                'REQUEST_METHOD':'POST',
                'CONTENT_TYPE':self.headers['Content-Type'],
            }
        )

        print self.path

        publish_socket.send_json(
            {k: form[k].value for k in form.keys()})

        r = []
        req_queue.put(r)
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
        req_queue.put(None)
        pass
