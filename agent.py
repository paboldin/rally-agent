#!/usr/bin/python

import sys
import zmq

subscribe_context = zmq.Context()
subscribe_socket = subscribe_context.socket(zmq.SUB)
subscribe_socket.connect("tcp://127.0.0.1:1234")
subscribe_socket.setsockopt(zmq.SUBSCRIBE, "")

push_context = zmq.Context()
push_socket = push_context.socket(zmq.PUSH)
push_socket.connect("tcp://127.0.0.1:1235")

while True:
        print "SUB:"
        d = subscribe_socket.recv_json()
        print d
        d[sys.argv[1]] = sys.argv[2]
        push_socket.send_json(d)
        print "PUSHED"
