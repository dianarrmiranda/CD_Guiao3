"""Message Broker"""
import enum
import json
from typing import List, Tuple
import selectors
import socket
import signal
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
        self.channels = {}

    def signal_handler(self, sig, frame):
        print('\nDone!')
        self.canceled = True

    signal.signal(signal.SIGINT, signal_handler)
    print('Press Ctrl+C to exit...')

    def accept(self, sock):

        # Aceita a conexão
        conn, addr = sock.accept()
        conn.setblocking(False)

        # Lê o header e a informação
        header = conn.recv(2)
        header = int.from_bytes(header, byteorder='big')
        data = conn.recv(header).decode('utf-8')

        # Verifica qual é o tipo da mensagem através da informação recbida
        serializer_type = json.loads(data)['Serializer']
        if serializer_type == 'JSONQueue':
            serializer = Serializer.JSON
        elif serializer_type == 'XMLQueue':
            serializer = Serializer.XML
        elif serializer_type == 'PickleQueue':
            serializer = Serializer.PICKLE
        else:
            serializer = None

        if serializer:
            self.channels[conn] = serializer
            self.selector.register(conn, selectors.EVENT_READ, self.read)
        else:
            conn.close()

    def read(self, conn):
        
        if conn in self.channels:
            serializer = self.channels[conn]
            if serializer == Serializer.JSON:
                msg = CDProto.recv_msg(conn, type="JSONQueue")
            elif serializer == Serializer.XML:
                msg = CDProto.recv_msg(conn, type="XMLQueue")
            elif serializer == Serializer.PICKLE:
                msg = CDProto.recv_msg(conn, type="PickleQueue")

            if msg is not None:
                command = msg["command"]
                topic = msg["topic"]
                message = msg["message"]

                if command == 'subscribe':
                    self.subscribe(topic, conn, serializer)
                elif command == 'publish':
                    self.put_topic(topic, message)
                    for sub_topic in self.subscriptions.keys():
                        if topic.startswith(sub_topic):
                            for subscriber in self.subscriptions[sub_topic]:
                                CDProto.send_msg(
                                    subscriber[0], command, serializer, topic, message)
                elif command == 'listTopics':
                    topics = self.list_topics()
                    CDProto.send_msg(conn, command, serializer, topic, topics)
                elif command == 'unsubscribe':
                    self.unsubscribe(topic, conn)
        else:
            self.unsubscribe("", conn)
            self.selector.unregister(conn)
            conn.close()

    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics containing values."""
        return [topic for topic in self.topics.keys() if self.topics[topic]]

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
        if address not in self.channels:
            self.channels[address] = _format

        if topic not in self.subscriptions:
            self.subscriptions[topic] = []
        
        self.subscriptions[topic].append((address, _format))

        last_msg = self.get_topic(topic)
        if last_msg:
            CDProto.send_msg(address, "publish", self.channels[address], topic, last_msg)

    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        if topic in self.subscriptions:
            Serializer = self.channels.get(address)
            self.subscriptions[topic].remove((address, Serializer))

    def run(self):
        """Run until canceled."""

        while not self.canceled:
            events = self.sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
