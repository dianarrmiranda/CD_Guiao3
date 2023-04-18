"""Message Broker"""
import enum
from typing import Dict, List, Any, Tuple
import selectors
import socket
import signal
import sys
import logging
from .protocol import CDProto


class Serializer(enum.Enum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2


class Broker:
    """Implementation of a PubSub Message Broker."""

    def __init__(self):
        """Initialize broker."""
        self.canceled = False
        self._host = "localhost"
        self._port = 5000

        # para não bloquear o socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self._host, self._port))
        self.sock.listen(100)

        self.sel = selectors.DefaultSelector()
        self.sel.register(self.sock, selectors.EVENT_READ, self.accept)

        self.topics = {}
        self.subscriptions = {}

    def signal_handler(self, sig, frame):
        print('\nDone!')
        self.canceled = True

    signal.signal(signal.SIGINT, signal_handler)
    print('Press Ctrl+C to exit...')

    def accept(self, sock):
        conn, addr = sock.accept()
        logging.debug('Accepted connection from %s ', addr)
        self.sel.register(conn, selectors.EVENT_READ, self.read)

    def read(self, conn):
        header = conn.recv(8)

        if header:
            header = int.from_bytes(header, byteorder='big')
            typeMsg = conn.recv(2)
            typeMsg = int.from_bytes(typeMsg, byteorder='big')
            data

        else:
            logging.debug('Closing connection to %s ', conn)
            for channel in self.channels:
                if conn in self.channels[channel]:
                    self.channels[channel].remove(conn)

            self.sel.unregister(conn)
            conn.close()

    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics containing values."""
        return list(self.topics.keys())

    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        return self.topics.get(topic)

    def put_topic(self, topic, value):
        """Store in topic the value."""
        self.topics[topic] = value

    def list_subscriptions(self, topic: str) -> List[Tuple[socket.socket, Serializer]]:
        """Provide list of subscribers to a given topic."""
        return self.subscriptions.get(topic, [])

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        if _format is None:
            _format = Serializer.JSON
        self.subscriptions.setdefault(topic, []).append((address, _format))

    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        self.subscriptions[topic].remove(address)

    def run(self):
        """Run until canceled."""

        while not self.canceled:
            events = self.sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
