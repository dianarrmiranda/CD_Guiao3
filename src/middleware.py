"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
from typing import Any, Tuple
import selectors, sys, socket
from .protocol import CDProto


class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type = MiddlewareType.CONSUMER):
        """Create Queue."""
        self.topic = topic
        self.type = _type
        self.sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)

        self.sel = selectors.DefaultSelector()
        self.sel.register(self.sock, selectors.EVENT_READ, self.pull)
        self.sel.register(sys.stdin, selectors.EVENT_READ, self.push)

        if self.type == MiddlewareType.CONSUMER:
            submsg = CDProto.subscribe(self.topic)
            CDProto.send_msg(self.sock, submsg)


    def push(self, value):
        """Sends data to broker."""
        pubmsg = CDProto.publish(value, self.topic)
        CDProto.send_msg(self.sock, pubmsg)
        

    def pull(self) -> Tuple[str, Any]:
        """Receives (topic, data) from broker. Should BLOCK the consumer!"""
        msg = CDProto.recv_msg(self.sock)
        if msg: # pub
            return msg
        #elif: # list topcs
         #   pass
            # invocar callback
        #else:

    
    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        CDProto.send_msg(self.sock, CDProto.list_topics())
        callback 
        

    def cancel(self):
        """Cancel subscription."""
        CDProto.send_msg(self.sock, CDProto.cancel(self.topic))


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""


class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""


class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
