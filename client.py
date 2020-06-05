import logging
import random
import socket
import uuid

from expirytimer import ExpiryTimer
from machine import Command
from messages import ClientRequest, ClientResponse, Message
from osocket import ClientObjectSocket


class Client:
    logger = logging.getLogger("client")

    def __init__(self, cluster_config, id=None):
        self.cluster_config = cluster_config
        self.id = id if id else uuid.uuid4()
        self.current_server_id = None
        self.current_server_id = self.choose_server()
        self.expiry_timer = ExpiryTimer(
            1000, 1000
        )  # fixed to wait for response for a second

    def choose_server(self):
        return random.choice(
            [k for k in self.cluster_config.keys() if k != self.current_server_id]
        )

    def _get_socket(self):
        return ClientObjectSocket.get_cosclient(
            self.cluster_config[self.current_server_id]["port"]
        )

    def _send_command(self, s: ClientObjectSocket):
        s.send_object(
            Message(
                self.id,
                self.current_server_id,
                -1,
                ClientRequest(self.id, self.current_cmd),
            )
        )

    def request(self, key, val=None):
        self.current_cmd = Command(uuid.uuid4(), key, val)
        return self.re_request()

    def re_request(self):
        """Retry current command"""
        self.logger.info(f"Sending command {self.current_cmd}.")
        s = self._get_socket()
        self._send_command(s)

        self.expiry_timer.reset()
        response = None
        while not self.expiry_timer.expired():
            try:
                response = s.recv_object()
                break
            except socket.timeout:
                self.logger.info("retry")
                continue
            except Exception as e:
                self.logger.error(f"exception {str(e)}")
                return -666
        else:
            self.logger.info("expired")
            return -66

        s.close()
        self.logger.info(f"Response {response}.")

        if response and response.payload.cmd_id == self.current_cmd.id:
            if (
                isinstance(response.payload.leader_id, int)
                and response.payload.leader_id != self.current_server_id
            ):
                self.current_server_id = response.payload.leader_id
            return response.payload.val
        return None  # didn't succeed; we are set up for retry, so retry

    def __repr__(self):
        return f"Client({self.id})"
