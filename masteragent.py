#!/usr/bin/python

import cgi
import collections
import datetime
import functools
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


INF = float("+inf")

class AgentsRequest(object):
    missed_queue = collections.defaultdict(list)
    last_req_id = None

    def __init__(self, req, config, req_id=None):
        self.req_id = req_id or str(uuid.uuid4())
        AgentsRequest.last_req_id = self.req_id
        self.req = req
        self.config = config

    def __call__(self):
        req = {
            "req": self.req_id
        }
        req.update(self.req)

        publish_socket.send_json(req)

        return self.recv_responses(self.req_id, **self.config)

    @classmethod
    def recv_responses(cls, req_id, timeout=1000, agents=INF):
        tstart = datetime.datetime.now()
        timeout = float(timeout)
        agents = float(agents)
        queue = cls.missed_queue.pop(req_id, [])
        while timeout and pull_socket.poll(timeout) and len(queue) < agents:
            resp = pull_socket.recv_json()
            if resp["req"] != req_id:
                cls.missed_queue[resp["req"]].append(resp)
            else:
                queue.append(resp)
            timeout = max(
                timeout -
                (datetime.datetime.now() - tstart).total_seconds() * 1000,
                0
            )
        return queue


def make_flat(form):
    d = {}
    for k in form.keys():
        if isinstance(form[k], list):
            d[k] = [x.value for x in form[k]]
        else:
            d[k] = form[k].value
    return d

class RegisterHandlerMeta(type):
    def __new__(cls, clsname, base, namespace):
        methods = namespace["methods"] = collections.defaultdict(dict)
        for func in namespace.values():
            if not callable(func):
                continue

            for item_method in getattr(func, "_methods", []):
                methods[item_method][getattr(func, "_path")] = func

        return type.__new__(cls, clsname, base, namespace)

def register(path, methods=('GET',)):
    def decorator(f):
        f._path = path
        f._methods = methods
        return f
    return decorator

@six.add_metaclass(RegisterHandlerMeta)
class RequestHandler(six.moves.BaseHTTPServer.BaseHTTPRequestHandler, object):
    config = dict(timeout=1000, agents=INF)

    def send_json_response(self, data, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "text/json")
        self.end_headers()

        self.wfile.write(json.dumps(data) + "\n")

    @register("/configure", ('GET', 'PUT'))
    def configure(self, query=None):
        if query:
            config = dict(six.moves.urllib.parse.parse_qsl(query))
            self.config["timeout"] = float(
                config.get("timeout", self.config["timeout"])
            )
            self.config["agents"] = float(
                config.get("agents", self.config["agents"])
            )

        self.send_json_response(self.config)

    def _get_config_from_url(self, **config):
        config.update(self._get_request_from_url()[1])
        return config

    @register("/missed", ('GET', 'DELETE'))
    def missed(self):
        AgentsRequest.recv_responses(None, timeout=10000, agents=INF)

        self.send_json_response({"missed": AgentsRequest.missed_queue})
        if self.command == "DELETE":
            AgentsRequest.missed_queue.clear()
    missed._methods = "ABC"

    @register("/ping")
    def ping(self):
        config = self._get_config_from_url(
            timeout=10000,
            agents=INF
        )
        return self.send_request_to_agents(config)

    @register("/poll")
    def poll(self):
        config = self._get_config_from_url(
            timeout=10000,
            agents=INF
        )

        responses = AgentsRequest.recv_responses(
            config.pop("req", AgentsRequest.last_req_id), **config)
        self.send_json_response(responses)

    def route(self):
        self.url = six.moves.urllib.parse.urlparse(self.path)
        path = self.url.path
        try:
            handler = self.methods[self.command][path]
        except KeyError:
            return self.send_response(404)

        return handler(self)

    do_PUT = do_GET = do_DELETE = route

    def do_POST(self):
        self.send_request_to_agents(self.config)

    def _parse_request(self):
        req = self._get_request_from_post()
        path, url_req = self._get_request_from_url()
        req["action"] = path[1:]
        if set(req) & set(url_req):
            self.send_json_response(
                {"error": "Duplicate argumets"},
                status=400
            )
            return
        return req

    def send_request_to_agents(self, config):

        req = self._parse_request()
        if req is None:
            return

        request = AgentsRequest(req, config)
        response = request()
        self.send_json_response(response)

    def _get_request_from_post(self):
        if (not self.headers.get("Content-Length") or
            not self.headers.get("Content-Type")):
            return {}
        form = cgi.FieldStorage(
            fp=self.rfile,
            headers=self.headers,
            environ={
                "REQUEST_METHOD": "POST",
                "CONTENT_TYPE": self.headers["Content-Type"],
            }
        )
        return make_flat(form)

    def _get_request_from_url(self):
        return (self.url.path,
                dict(six.moves.urllib.parse.parse_qsl(self.url.query)))


def main():
    server = six.moves.BaseHTTPServer.HTTPServer(
        ('localhost', 8080), RequestHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass

    server.server_close()

if __name__ == "__main__":
    main()
