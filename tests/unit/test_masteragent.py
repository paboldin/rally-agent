
#!/usr/bin/python

import ddt
import itertools
import mock
import datetime
import unittest
import zmq

orig_datetime = datetime

import masteragent

class MyDict(dict):
    pass

def annotated(d, name=None):
    d = MyDict(**d)
    if name:
        setattr(d, "__name__", name)
    return d


@ddt.ddt
class AgentsRequestTestCase(unittest.TestCase):
    def test___call__(self):
        request = masteragent.AgentsRequest(
            req={
                "foo": "bar",
            },
            config={
                "config": "foobar",
            },
            req_id="42")
        request.recv_responses = mock.Mock()
        publish_socket = mock.Mock()
        pull_socket = mock.Mock()

        request(publish_socket, pull_socket)

        publish_socket.send_json.assert_called_once_with(
            {"foo": "bar", "req": "42"})
        request.recv_responses.assert_called_once_with(
            "42", pull_socket, config="foobar")


    @mock.patch("masteragent.datetime_now")
    @ddt.data(
        annotated({
            # Poll timedout (returned False)
            "req_id": "foobar",
            "recv_json": [
                {}, {"req": "bar"}, {}
            ],
            "poll": [
                True, True, True, False
            ],
            "now": [10, 10.5, 10.8, 10.9],
            "expected_missed_queue": [1],
            "expected_queue": [0, 2],
        }, name="poll timed out"),
        annotated({
            # Recv timedout (no time left after recv_json)
            "req_id": "foobar",
            "recv_json": [
                {}, {"req": "bar"}, {}
            ],
            "poll": [
                True, True, True
            ],
            "now": [10, 10.5, 10.8, 11.1],
            "expected_missed_queue": [1],
            "expected_queue": [0, 2],
        }, name="recv timed out"),
        annotated({
            # All is missed
            "req_id": "",
            "recv_json": [
                {"req": "foo"}, {"req": "bar"}
            ],
            "poll": [
                True, True
            ],
            "now": [10, 10.8, 11.1],
            "expected_missed_queue": [0, 1],
            "expected_queue": [],
        }, name="all is missed"),
        annotated({
            # Picked from missing
            "req_id": "foobar",
            "recv_json": [
                {"req": "foo"}, {"req": "bar"}
            ],
            "poll": [
                True, True
            ],
            "now": [10, 10.8, 11.1],
            "expected_missed_queue": [0, 1],
            "missed_queue": {"foobar": [{"req": "foobar"}]},
            "expected_queue": [{"req": "foobar"}],
        }, name="picked from missing"),
        annotated({
            # stopped on recv
            "req_id": "foobar",
            "recv_json": [
                {}, {}, ValueError()
            ],
            "poll": [
                True, True, True
            ],
            "now": [10, 10.8, 11.1],
            "timeout": 10*1000,
            "should_raise": ValueError,
        }, name="stopped on recv"),
        annotated({
            # stopped on poll
            "req_id": "foobar",
            "recv_json": [
                {}, {}, {},
            ],
            "poll": [
                True, True, True, ValueError(),
            ],
            "now": [10, 11, 12, 13],
            "timeout": 10*1000,
            "should_raise": ValueError,
        }, name="stopped on poll"),
        annotated({
            # stopped on now
            "req_id": "foobar",
            "recv_json": [
                {}, {}, {},
            ],
            "poll": [
                True, True, True
            ],
            "now": [10, 11, 12, ValueError()],
            "timeout": 10*1000,
            "should_raise": ValueError,
        }, name="stopped on poll"),
        annotated({
            # recieved enough response
            "req_id": "foobar",
            "recv_json": [
                {}, {"req": "foo"}, {}
            ],
            "poll": [
                True, True, True
            ],
            "now": [10, 11, 12, 13],
            "timeout": 10*1000,
            "agents": 2,
            "expected_missed_queue": [1],
            "expected_queue": [0, 2]
        }, name="recv enough responses"),
    )
    def test_recv_responses(self, param, mock_masteragent_datetime_now):
        recv_json = param.pop("recv_json")
        for r in recv_json:
            try:
                r.setdefault("req", param.get("req_id"))
            except AttributeError:
                pass

        mock_pull_socket = mock.Mock(**{
            "recv_json.side_effect": recv_json,
            "poll.side_effect": param.pop("poll")
        })
        mock_masteragent_datetime_now.side_effect = [
            ts if isinstance(ts, Exception) else
            datetime.datetime.utcfromtimestamp(ts)
                    for ts in param.pop("now")
        ]

        expected_missed_queue = {}
        param.setdefault("missed_queue", {})

        emq = param.pop("expected_missed_queue", None)
        if emq is not None:
            emq = [recv_json[i] for i in emq]
            get_req = lambda j: j["req"]
            emq = sorted(emq, key=get_req)
            emq = dict([
                (key, list(subiter))
                for key, subiter in itertools.groupby(emq, key=get_req)])
            expected_missed_queue = emq
            del emq

        expected_queue = param.pop("expected_queue", None)
        if expected_queue:
            expected_queue = [recv_json[i] if isinstance(i, int) else i
                              for i in expected_queue]

        param["pull_socket"] = mock_pull_socket
        should_raise = param.pop("should_raise", False)
        if not should_raise:
            retval = masteragent.AgentsRequest.recv_responses(**param)
            self.assertEqual(expected_queue, retval)
        else:
            if should_raise is True:
                should_raise = StopIteration
            self.assertRaises(
                should_raise,
                masteragent.AgentsRequest.recv_responses,
                **param)

        self.assertEqual(expected_missed_queue, param["missed_queue"])


@ddt.ddt
class RequestHandlerTestCase(unittest.TestCase):
    def setUp(self):
        super(RequestHandlerTestCase, self).setUp()

        self.mocks = [
            mock.patch("six.moves.BaseHTTPServer."
                       "BaseHTTPRequestHandler.__init__")
        ]

        for mock_ in self.mocks:
            mock_.start()

    def tearDown(self):
        super(RequestHandlerTestCase, self).tearDown()

        for mock_ in self.mocks:
            mock_.stop()

    def get_req_handler(self, path="/"):
        server = mock.Mock(pull_socket="foo", publish_socket="bar")
        return masteragent.RequestHandler(
            request=None, client_address=None,
            server=server, path=path)

    def test_send_json_response(self):
        req_handler = self.get_req_handler()
        req_handler.send_response = mock.Mock()
        req_handler.send_header = mock.Mock()
        req_handler.end_headers = mock.Mock()
        req_handler.wfile = mock.Mock()

        req_handler.send_json_response(
            data={"hello": "there"}, status="foobar")

        req_handler.send_response.assert_called_once_with("foobar")
        req_handler.send_header.assert_called_once_with(
            "Content-Type", "text/json")
        req_handler.end_headers.assert_called_once_with()

        req_handler.wfile.write.assert_called_once_with(
            b"""{"hello": "there"}\n"""
        )

    @ddt.unpack
    @ddt.data(
        ("timeout=10&agents=1", {"timeout": 10., "agents": 1.}),
        ("timeout=10", {"timeout": 10., "agents": float("+Inf")}),
        ("agents=10", {"timeout": 1000., "agents": 10.}),
        ("", {"timeout": 1000., "agents": float("+Inf")})
    )
    def test_configure(self, query, config):
        req_handler = self.get_req_handler()
        req_handler.send_json_response = mock.Mock()

        req_handler.url = mock.Mock(query=query)

        req_handler.configure()

        req_handler.send_json_response.assert_called_once_with(config)

    def test__get_request_from_url(self):
        req_handler = self.get_req_handler()
        req_handler.url = mock.Mock(query="a=b&c=d&e=f")

        config = req_handler._get_request_from_url(a="z", g="h")

        self.assertEqual(
            {"a": "b", "c": "d", "e": "f", "g": "h"},
            config)

    @mock.patch("masteragent.AgentsRequest.recv_responses")
    def test_missed(self, mock_agents_request_recv_responses):
        req_handler = self.get_req_handler()
        req_handler.send_json_response = mock.Mock()
        req_handler.command = "DELETE"
        req_handler.missed_queue = mock.Mock()

        req_handler.missed()

        mock_agents_request_recv_responses.assert_called_once_with(
            None, "foo", req_handler.missed_queue, agents=float("Inf"),
            timeout=10000)
        req_handler.send_json_response.assert_called_once_with(
            {"missed": req_handler.missed_queue})
        req_handler.missed_queue.clear.assert_called_once_with()

    def test_ping(self):
        req_handler = self.get_req_handler()
        req_handler.send_request_to_agents = mock.Mock()
        req_handler._get_request_from_url = mock.Mock()

        req_handler.ping()

        req_handler._get_request_from_url.assert_called_once_with(
            timeout=10000, agents=float("Inf")
        )
        req_handler.send_request_to_agents.assert_called_once_with(
            req_handler._get_request_from_url.return_value)

    @ddt.data(
        {"req": "abc", "foo": "bar"},
        {"foo": "bar"}
    )
    @mock.patch("masteragent.AgentsRequest.recv_responses")
    def test_poll(self, config, mock_agents_request_recv_responses):
        req_handler = self.get_req_handler()
        req_handler.send_json_response = mock.Mock()
        req_handler._get_request_from_url = mock.Mock(
            return_value=dict(**config))
        req_handler.last_req_id = "last_req_id"
        req_handler.missed_queue = "missed_queue"

        req_handler.poll()

        mock_agents_request_recv_responses.assert_called_once_with(
            config.get("req", "last_req_id"),
            req_handler.pull_socket, req_handler.missed_queue, foo="bar"
        )
        req_handler.send_json_response.assert_called_once_with(
            mock_agents_request_recv_responses.return_value)

    def test_route_ok(self):
        self.assertEqual(
            masteragent.RequestHandler.route,
            masteragent.RequestHandler.do_PUT
        )
        self.assertEqual(
            masteragent.RequestHandler.route,
            masteragent.RequestHandler.do_GET
        )
        self.assertEqual(
            masteragent.RequestHandler.route,
            masteragent.RequestHandler.do_DELETE
        )

    @ddt.unpack
    @ddt.data(
        ("/here", "GET", False),
        ("/here", "POST", True),
        ("/there", "POST", False)
    )
    def test_route(self, path, command, should_404):
        req_handler = self.get_req_handler()
        req_handler.url = mock.Mock(path=path)
        req_handler.command = command
        req_handler.send_response = mock.Mock()
        req_handler.methods = {
            "GET": {"/here": lambda x: "foobar"},
            "POST": {"/there": lambda x: "foobar"},
        }

        retval = req_handler.route()

        if should_404:
            req_handler.send_response.assert_called_once_with(404)
        else:
            self.assertEqual("foobar", retval)

    def test_do_POST(self):
        req_handler = self.get_req_handler()
        req_handler.send_request_to_agents = mock.Mock()

        req_handler.do_POST()

        req_handler.send_request_to_agents.assert_called_once_with(
            req_handler.config
        )

    @ddt.unpack
    @ddt.data(
        ({"a": "b"}, {"a": "c"}, "", True),
        ({"b": "b"}, {"a": "c"}, "/here", False),
    )
    def test__parse_request(self, url, post, path, should_raise):
        req_handler = self.get_req_handler(path)
        req_handler._get_request_from_url = mock.Mock(
            return_value=dict(**url))
        req_handler._get_request_from_post = mock.Mock(
            return_value=dict(**post))

        if should_raise:
            self.assertRaises(ValueError, req_handler._parse_request)
            return

        post.update(url)
        post["action"] = path[1:]

        req = req_handler._parse_request()


        self.assertEqual(post, req)

    @mock.patch("masteragent.AgentsRequest")
    def test_send_request_to_agents(self, mock_masteragent_agents_request):
        req_handler = self.get_req_handler()
        req_handler._parse_request = mock.Mock()
        req_handler.send_json_response = mock.Mock()

        req_handler.send_request_to_agents({"foo": "bar"})

        mock_masteragent_agents_request.assert_called_once_with(
            req_handler._parse_request.return_value, {"foo": "bar"})
        self.assertEqual(
            mock_masteragent_agents_request.return_value.req_id,
            req_handler.last_req_id)
        mock_masteragent_agents_request.return_value.assert_called_once_with(
            "bar", "foo")
        req_handler.send_json_response.assert_called_once_with(
            mock_masteragent_agents_request.return_value.return_value)

    def test__get_request_from_post_empty(self):
        req_handler = self.get_req_handler()
        req_handler.headers = {}

        self.assertEqual({}, req_handler._get_request_from_post())

    @mock.patch("masteragent.make_flat")
    @mock.patch("cgi.FieldStorage")
    def test__get_request_from_post(self, mock_cgi_field_storage,
                                    mock_masteragent_make_flat):
        req_handler = self.get_req_handler()
        req_handler.headers = {
            "Content-Type": "form/multi-part",
            "Content-Length": 42
        }
        req_handler.rfile = mock.Mock()

        retval = req_handler._get_request_from_post()

        mock_cgi_field_storage.assert_called_once_with(
            fp=req_handler.rfile,
            headers=req_handler.headers,
            environ={
                "REQUEST_METHOD": "POST",
                "CONTENT_TYPE": req_handler.headers["Content-Type"],
            }
        )

        mock_masteragent_make_flat.assert_called_once_with(
            mock_cgi_field_storage.return_value)
        self.assertEqual(
            mock_masteragent_make_flat.return_value,
            retval)
