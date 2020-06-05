import random
from unittest.mock import Mock, call

import pytest

from osocket import ClientObjectSocket, COSocketDecodeError, COSocketEncodeError


class Hello:
    def __init__(self, msg):
        self.msg = msg

    def __eq__(self, other):
        return self.msg == other.msg


def test_to_and_from_bytes():
    x = 9
    assert x == ClientObjectSocket.object_from_bytes(
        ClientObjectSocket.object_to_bytes(x)
    )

    h = Hello("howdy")
    h_new = ClientObjectSocket.object_from_bytes(ClientObjectSocket.object_to_bytes(h))
    assert h == h_new

    with pytest.raises(COSocketDecodeError):
        ClientObjectSocket.object_from_bytes(ClientObjectSocket.object_to_bytes(h)[:30])


def test_send_object_across():
    port = random.randint(20000, 30000)
    server = ClientObjectSocket.get_cosserver(port)
    client = ClientObjectSocket.get_cosclient(port)

    client.send_object("hello")
    client.send_object("hello2")
    (sclient, _) = server.accept()
    assert sclient.recv_object() == "hello"
    assert sclient.recv_object() == "hello2"


def test_recv_object():
    mocksock = Mock()
    bs = ClientObjectSocket.object_to_bytes(Hello("howdy"))

    # receive object once in one chunk
    mocksock.recv.side_effect = [bs]
    cos = ClientObjectSocket(mocksock)
    o = cos.recv_object()
    assert Hello("howdy") == o

    # receive object twice in bits
    mocksock.recv.side_effect = [bs[:10], bs[10:] + bs[:25], bs[25:]]
    o = cos.recv_object()
    assert Hello("howdy") == o
    o = cos.recv_object()
    assert Hello("howdy") == o


def test_send_object():
    mocksock = Mock()
    bs = ClientObjectSocket.object_to_bytes(Hello("howdy"))

    mocksock.send.side_effect = [len(bs)]
    cos = ClientObjectSocket(mocksock)
    cos.send_object(Hello("howdy"))
    assert mocksock.send.call_count == 1
    assert mocksock.send.call_args == call(bs)

    mocksock.send.reset_mock()
    mocksock.send.side_effect = [25, len(bs) - 25]
    cos.send_object(Hello("howdy"))
    assert mocksock.send.call_count == 2
    assert mocksock.send.call_args_list == [call(bs), call(bs[25:])]
