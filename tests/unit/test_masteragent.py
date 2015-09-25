
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
