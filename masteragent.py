#!/usr/bin/python

import argparse
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

datetime_now = datetime.datetime.now


INF = float("+inf")

class AgentsRequest(object):
    def __init__(self, req, config, req_id=None):
        self.req_id = req_id or str(uuid.uuid4())

        self.req = req
        self.config = config

    def __call__(self, publish_socket, pull_socket):
        req = {
            "req": self.req_id
        }
        req.update(self.req)

        publish_socket.send_json(req)

        return self.recv_responses(
            self.req_id, pull_socket, **self.config)

    @classmethod
    def recv_responses(cls, req_id, pull_socket, missed_queue=None,
                       timeout=1000, agents=INF):
        tstart = datetime_now()
        timeout = float(timeout)
        agents = float(agents)
        queue = []
        if missed_queue is not None:
            queue = missed_queue.pop(req_id, [])

        left = timeout
        while (left > 0 and len(queue) < agents
               and pull_socket.poll(left)):
            resp = pull_socket.recv_json()
            if resp["req"] != req_id and missed_queue is not None:
                missed_queue.setdefault(resp["req"], []).append(resp)
            else:
                queue.append(resp)
            left = timeout - (datetime_now() - tstart).total_seconds()*1000

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
    DEFAULT_CONFIG = dict(timeout=1000, agents=INF)

    def __init__(self, request, client_address, server, path=None):
        super(RequestHandler, self).__init__(request, client_address, server)

        self.pull_socket = server.pull_socket
        self.publish_socket = server.publish_socket
        self.missed_queue = {}
        self.last_req_id = None
        self.config = dict(**self.DEFAULT_CONFIG)
        if not hasattr(self, "path"):
            self.path = path
        self.url = six.moves.urllib.parse.urlparse(self.path)

    def send_json_response(self, data, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "text/json")
        self.end_headers()

        self.wfile.write((json.dumps(data) + "\n").encode("utf-8"))

    @register("/configure", ('GET', 'PUT'))
    def configure(self):
        if self.url.query:
            config = dict(six.moves.urllib.parse.parse_qsl(self.url.query))
            self.config["timeout"] = float(
                config.get("timeout", self.config["timeout"])
            )
            self.config["agents"] = float(
                config.get("agents", self.config["agents"])
            )

        self.send_json_response(self.config)

    @register("/missed", ('GET', 'DELETE'))
    def missed(self):
        AgentsRequest.recv_responses(
            None, self.pull_socket, self.missed_queue,
            timeout=10000, agents=INF)

        self.send_json_response({"missed": self.missed_queue})
        if self.command == "DELETE":
            self.missed_queue.clear()

    @register("/ping")
    def ping(self):
        config = self._get_request_from_url(
            timeout=10000,
            agents=INF
        )
        return self.send_request_to_agents(config)

    @register("/poll")
    def poll(self):
        config = self._get_request_from_url(
            timeout=10000,
            agents=INF
        )

        responses = AgentsRequest.recv_responses(
            config.pop("req", self.last_req_id),
            self.pull_socket, self.missed_queue,
            **config)
        self.send_json_response(responses)

    def route(self):
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
        url_req = self._get_request_from_url()
        req["action"] = self.url.path[1:]
        if set(req) & set(url_req):
            raise ValueError("Duplicate argumets.")
        req.update(url_req)

        return req

    def send_request_to_agents(self, config):
        try:
            req = self._parse_request()
        except ValueError as e:
            self.send_json_response(
                {"error": str(e)},
                status=400
            )
            return

        request = AgentsRequest(req, config)
        self.last_req_id = request.req_id
        response = request(self.publish_socket, self.pull_socket)
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

    def _get_request_from_url(self, **config):
        config.update(dict(six.moves.urllib.parse.parse_qsl(self.url.query)))
        return config


class MasterAgentHTTPServer(six.moves.BaseHTTPServer.HTTPServer):
    def __init__(self, address, request, publish_socket, pull_socket):
        six.moves.BaseHTTPServer.HTTPServer.__init__(self, address, request)
        self.publish_socket = publish_socket
        self.pull_socket = pull_socket


def init_zmq(publish_url, pull_url):
    publish_context = zmq.Context()
    publish_socket = publish_context.socket(zmq.PUB)
    publish_socket.bind(publish_url)

    pull_context = zmq.Context()
    pull_socket = pull_context.socket(zmq.PULL)
    pull_socket.bind(pull_url)

    return publish_socket, pull_socket

def parse_args():
    parser = argparse.ArgumentParser(
        description="Run a HTTP<->ZMQ proxy called 'masteragent'")
    parser.add_argument(
        "--http-host", help="Host to bind HTTP face to",
        default="0.0.0.0")
    parser.add_argument(
        "--http-port", help="Port to bind HTTP face to",
        type=int, default=8080)

    parser.add_argument(
        "--publish-url", help="ZMQ Publish bind URL",
        default="tcp://*:1234")
    parser.add_argument(
        "--pull-url", help="ZMQ Pull bind URL",
        default="tcp://*:1235")

    return parser.parse_args()

def main():
    args = parse_args()
    publish_socket, pull_socket = init_zmq(
        args.publish_url, args.pull_url)

    server = MasterAgentHTTPServer(
        (args.http_host, args.http_port), RequestHandler,
        publish_socket, pull_socket)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass

    server.server_close()

if __name__ == "__main__":
    main()
