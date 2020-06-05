"""Object Sockets

Wrapper around sockets that deal with objects; python objects in and
out.  Fix host to be localhost, since not dealing with non-local
networking yet.

Use for client:

    client = ClientObjectSocket.get_cosclient(port)
    client.send_object(o)
    o = client.recv_object()

Use for server:

    server = ClientObjectSocket.get_cosserver(port)

    while True:
        (sclient, _) = server.accept()
        o = sclient.recv_object()

"""

import logging
import pickle
import random
import string
import threading
import time
from logging.config import dictConfig
from socket import AF_INET, SOCK_STREAM, socket

import click
from ruamel.yaml import YAML

logger = logging.getLogger("osocket")


MAX_ENCODING_LEN_DIGITS = 10
## not a reasonable value, but just don't want to think about this yet
## later maybe use fixed much shorter value to indicate how long the
## length will be, and then just encode the length.

CO_SOCKET_TIMEOUT = 1


class COSocketException(Exception):
    pass


class COSocketEncodeError(COSocketException):
    pass


class COSocketDecodeError(COSocketException):
    pass


class COSConnectionClosed(COSocketException):
    pass


class COSServer:
    def __init__(self, serversocket):
        self.serversocket = serversocket

    def accept(self):
        (clientsocket, address) = self.serversocket.accept()
        clientsocket.settimeout(CO_SOCKET_TIMEOUT)
        return (ClientObjectSocket(clientsocket), address)

    def close(self):
        print(self.serversocket)
        self.serversocket.close()


class ClientObjectSocket:
    def __init__(self, socket):
        self.socket = socket
        self.recvd = bytes()

    @staticmethod
    def get_cosclient(port):
        logger.debug("Creating osocket client on %d", port)
        clientsocket = socket(AF_INET, SOCK_STREAM)
        clientsocket.settimeout(CO_SOCKET_TIMEOUT)
        clientsocket.connect(("localhost", port))
        return ClientObjectSocket(clientsocket)

    @staticmethod
    def get_cosserver(port):
        logger.debug("Creating osocket server on %d", port)
        serversocket = socket(AF_INET, SOCK_STREAM)
        serversocket.bind(("localhost", port))
        serversocket.settimeout(CO_SOCKET_TIMEOUT)
        serversocket.listen(5)
        return COSServer(serversocket)

    @staticmethod
    def object_to_bytes(object):
        bs = pickle.dumps(object)
        if len(str(len(bs))) > MAX_ENCODING_LEN_DIGITS:
            raise COSocketEncodeError(
                f"Object needs too many bytes to encode "
                "{len(bs)} > {MAX_ENCODING_LEN_DIGITS}"
            )
        lbs = bytes(
            ("{:" + str(MAX_ENCODING_LEN_DIGITS) + "}:").format(len(bs)), "ascii"
        )
        return lbs + bs

    @staticmethod
    def object_from_bytes(bs):
        lbs = int(bs[:MAX_ENCODING_LEN_DIGITS].decode("ascii"))
        bs = bs[MAX_ENCODING_LEN_DIGITS + 1 :]
        if not lbs == len(bs):
            logging.error("decoding bytes with incorrect encoded length")
            raise COSocketDecodeError(
                f"Trying to decode an object but have too "
                "{'few' if lbs < len(bs) else 'many'} bytes ({len(bs)}/{lbs}"
            )
        return pickle.loads(bs)

    def recv_object(self):
        logger.debug("Start receiving object %s", str(self.recvd))
        expected_len = None
        while True:
            if not expected_len and len(self.recvd) >= MAX_ENCODING_LEN_DIGITS + 1:
                expected_len = int(self.recvd[:MAX_ENCODING_LEN_DIGITS].decode("ascii"))
                logger.debug("Length received: %d", expected_len)
            if (
                expected_len
                and len(self.recvd) >= expected_len + MAX_ENCODING_LEN_DIGITS + 1
            ):
                logger.debug("Object received")
                rv = self.object_from_bytes(
                    self.recvd[: expected_len + MAX_ENCODING_LEN_DIGITS + 1]
                )
                self.recvd = self.recvd[expected_len + MAX_ENCODING_LEN_DIGITS + 1 :]
                return rv
            chunk = self.socket.recv(2048)
            if not chunk:
                logger.debug("empty chunk")
                raise COSConnectionClosed()
            self.recvd += chunk

    def send_object(self, o):
        logger.debug("Sending object: %s", repr(o))
        message = self.object_to_bytes(o)
        totalsend = 0
        while totalsend < len(message):
            send = self.socket.send(message[totalsend:])
            if send == 0:
                logger.debug("socket closed")
                return
            totalsend += send
        logger.debug("Object send")

    def close(self):
        self.socket.close()
