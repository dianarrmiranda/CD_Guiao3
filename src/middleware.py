"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
import json
import pickle
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

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self.topic = topic
        self.type = _type
        self.sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.sock.connect(('localhost',5000))


    def push(self, value):
        """Sends data to broker."""
        

    def pull(self) -> Tuple[str, Any]:
        """Receives (topic, data) from broker. Should BLOCK the consumer!"""

    
    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        

    def cancel(self):
        """Cancel subscription."""


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.ser_type = 0

        if self.type == MiddlewareType.CONSUMER:
            CDProto.send_msg(self.sock, "subscribe", self.ser_type, self.topic)

    def push(self, value):
        """Sends data to broker."""

        CDProto.send_msg(self.sock, "publish", self.ser_type, self.topic, value)
        #print ("Producer (JSON)" + str(self.ser_type) + " sent " + str(value) + " to topic" + self.topic)
        
    def pull(self) -> Tuple[str, Any]:
        """Receives (topic, data) from broker. Should BLOCK the consumer!"""
        header = self.sock.recv(1)
        #print('header', header)
        msg = CDProto.recv_msg(self.sock, self.ser_type)
        #print(msg)
        if msg is not None:
            print ("Consumer (JSON)" + str(self.ser_type) + " received " + str(msg["message"]) + " from topic" + msg["topic"])
            if msg["command"] == "publish":
                return msg["topic"], msg["message"]
            elif msg["command"] == "listTopics":
                #invocar callback
                pass
            #else:

    
    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        CDProto.send_msg(self.sock, "listTopics", self.ser_type, self.topic)
         
        

    def cancel(self):
        """Cancel subscription."""
        CDProto.send_msg(self.sock, "unsubscribe", self.ser_type, self.topic)


class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.ser_type = 1

        if self.type == MiddlewareType.CONSUMER:
            CDProto.send_msg(self.sock, "subscribe", self.ser_type, self.topic)

    def push(self, value):
        """Sends data to broker."""
        CDProto.send_msg(self.sock, "publish", self.ser_type, self.topic, value)
        print ("Producer (XML) " + str(self.ser_type) + " sent " + str(value) + " to topic" + self.topic)
        
    def pull(self) -> Tuple[str, Any]:
        """Receives (topic, data) from broker. Should BLOCK the consumer!"""
        header = self.sock.recv(1)
        msg = CDProto.recv_msg(self.sock, self.ser_type)
        if msg:
            print ("Consumer (XML) " + str(self.ser_type) + " received " + str(msg["message"]) + " from topic" + msg["topic"])
            if msg["command"] == "publish": 
                return msg["topic"], msg["message"]
            elif msg["command"] == "listTopics":
            #invocar callback
                pass
        else:
            return
        #else:

    
    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        CDProto.send_msg(self.sock, "listTopics", self.ser_type, self.topic)
        callback 
        

    def cancel(self):
        """Cancel subscription."""
        CDProto.send_msg(self.sock, "unsubscribe", self.ser_type, self.topic)


class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.ser_type = 2

        if self.type == MiddlewareType.CONSUMER:
            CDProto.send_msg(self.sock, "subscribe", self.ser_type, self.topic)

    def push(self, value):
        """Sends data to broker."""
        CDProto.send_msg(self.sock, "publish", self.ser_type, self.topic, value)
        print ("Producer (Pickle) " + str(self.ser_type) + " sent " + str(value) + " to topic" + self.topic)
        
    def pull(self) -> Tuple[str, Any]:
        """Receives (topic, data) from broker. Should BLOCK the consumer!"""
        #print ("Pickle consumer tried to pull a message \n")
        header = self.sock.recv(1)
        #msg = CDProto.recv_msg(self.sock, self.ser_type)
        size = self.sock.recv(2)
        size = int.from_bytes(size, byteorder="big")
        msg = self.sock.recv(size)
        print("PICKLE CONSUMER PULLED MESSAGE \n" , msg , "\n")
        try:
            msg = pickle.loads(msg)
        except pickle.UnpicklingError as e:
            print(f"Error unpickling object: {str(e)}")
        
        

        print("Pickle consumer sucessfully loaded message")
        #print("sock: ", str(self.sock), " type: ", str(self.ser_type), "\n")
        #print("MESSSSSAGE   ", msg, "\n")

        if msg:
            print ("Consumer  (Pickle)" + str(self.ser_type) + " received " + str(msg["message"]) + " from topic" + msg["topic"])
            if msg["command"] == "publish":
                return msg["topic"], msg["message"]
            elif msg["command"] == "listTopics":
                #invocar callback
                pass
            #else:

    
    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        CDProto.send_msg(self.sock, "listTopics", self.ser_type)
         
        

    def cancel(self):
        """Cancel subscription."""
        CDProto.send_msg(self.sock, "unsubscribe", self.ser_type, self.topic)
