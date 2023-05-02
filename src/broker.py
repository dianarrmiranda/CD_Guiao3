"""Message Broker"""
import enum
import json
from typing import List, Tuple
import selectors
import socket
import sys
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
        self.subscriptions = {}  # topic -> connections (sockets)
        self.channels = {}  # conn -> serializer

    def signal_handler(sig, frame):
        print('\nDone!')
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    print('Press Ctrl+C to exit...')

    def accept(self, sock, mask):

        # Aceita a conexão
        conn, addr = sock.accept()
        conn.setblocking(False)

        self.sel.register(conn, selectors.EVENT_READ, self.read)
        self.channels[conn] = None

    def read(self, conn, mask):

        if conn in self.channels:
            msg = None
            # Lê o header e a informação
            header = conn.recv(1)
            if header == b'':
                #print("closed")
                self.unsubscribe("", conn)
                self.sel.unregister(conn)
                conn.close()
                return
            header = int.from_bytes(header, byteorder='big')

            # Verifica qual é o tipo da mensagem através da informação recbida
            if header == Serializer.JSON.value:
                serializer = Serializer.JSON
            elif header == Serializer.XML.value:
                serializer = Serializer.XML
            elif header == Serializer.PICKLE.value:
                serializer = Serializer.PICKLE
            else:
                serializer = None

            if serializer:
                self.channels[conn] = serializer
            else:
                conn.close()

            if serializer == Serializer.JSON:
                msg = CDProto.recv_msg(conn, serializer.value)
            elif serializer == Serializer.XML:
                msg = CDProto.recv_msg(conn, serializer.value)
            elif serializer == Serializer.PICKLE:
                msg = CDProto.recv_msg(conn, serializer.value)

            if msg is not None:
                command = msg["command"]
                topic = msg["topic"]

                if command == 'subscribe':
                    self.subscribe(topic, conn, serializer)

                elif command == 'publish':

                    message = msg["message"]
                    #print ( "Publish message is gonna be sent to topic" , topic , "\n ")

                    self.put_topic(topic, message)
                    # print("Broker published " + message + "to topic " + topic)startswith(sub_topic) or topic == sub_topic
                    for sub_topic in self.subscriptions.keys():
                        if sub_topic == msg["topic"] or sub_topic in msg["topic"]:
                            for subscriber in self.subscriptions[sub_topic]:
                                #print ("Sending publish message to" , self.channels[subscriber[0]] , "\n" )
                                CDProto.send_msg(
                                    subscriber[0], command, serializer.value, topic, message)

                elif command == 'listTopics':

                    topics = self.list_topics()
                    CDProto.send_msg(
                        conn, command, serializer.value, topic, topics)

                elif command == 'unsubscribe':
                    self.unsubscribe(topic, conn)
            else:
                self.unsubscribe("", conn)
                self.sel.unregister(conn)
                conn.close()
        else:
            self.unsubscribe("", conn)
            self.sel.unregister(conn)
            conn.close()

    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics containing values."""
        return list(self.topics.keys())

    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        if topic in self.topics.keys():
            return self.topics[topic]
        return 

    def put_topic(self, topic, value):
        """Store in topic the value."""
        self.topics[topic] = value

    def list_subscriptions(self, topic: str) -> List[Tuple[socket.socket, Serializer]]:
        """Provide list of subscribers to a given topic."""
        if topic in self.subscriptions.keys():
            return self.subscriptions[topic]
        return 

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        self.channels[address] = _format

        if topic not in self.subscriptions:
            self.subscriptions[topic] = [(address, _format)]
        else:
            self.subscriptions[topic].append((address, _format))

        if topic in self.topics:
            last_msg = self.get_topic(topic)
            if last_msg:
                serializer = self.channels[address]
                CDProto.send_msg(address, "publish",
                                 serializer.value, topic, last_msg)

    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        if topic in self.subscriptions:
            serializer = self.channels.get(address)
            self.subscriptions[topic].remove((address, serializer))
        elif topic == "":
            for topic, address in self.subscriptions.items():
                for conn, ser in address:
                    self.subscriptions[topic].remove((conn, ser))
        else:
            print("Erro")

    def run(self):
        """Run until canceled."""

        while not self.canceled:
            events = self.sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
