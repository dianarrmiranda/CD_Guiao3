import json, pickle
from socket import socket
import xml.etree.ElementTree as ET
class Message:
    """Message Type."""

    def __init__(self, command):
        self.command = command

class Subscribe(Message):
    """Message to join a chat topic."""
    def __init__(self, command, type, topic):
        super().__init__(command)
        self.topic = topic
        self.type = type

    def dict(self):
        return {"command": self.command, "type": self.type.__str__(), "topic": self.topic}
    
    def __str__(self):
        return f'{{"command": "{self.command}", "type": "{self.type.__str__()}", "topic": "{self.topic}"}}'
        
class ListTopics(Message):
    def __init__(self, command, type):
        super().__init__(command)
        self.type = type

    def dict(self):
        return {"command": self.command, "type": self.type.__str__()}
    
    def __str__(self):
        return f'{{"command": "{self.command}", "type": "{self.type.__str__()}"}}'
    

class ListTopicsOK(Message):
    def __init__(self, command, topics):
        super().__init__(command)
        self.topics = topics

    def dict(self):
        return {"command": self.command, "type": self.type.__str__(), "topics": self.topics}

    def __str__(self):
        return f'{{"command": "{self.command}", "type": "{self.type.__str__()}", "topics": "{self.topics}"}}'
    
class Publish(Message):
    """Message to chat with other clients."""
    def __init__(self, command, type, topic, message):
        super().__init__(command)
        self.topic = topic
        self.message = message
        self.type = type

    def dict(self):
        return {"command": self.command, "type": self.type.__str__(), "topic": self.topic, "message": self.message}
    
    def __str__(self):
        return f'{{"command": "{self.command}", "type": "{self.type.__str__()}", "topic": "{self.topic}", "message": "{self.message}"}}'

class Unsubscribe(Message):
    def __init__(self, command, type, topic):
        super().__init__(command)
        self.topic = topic
        self.type = type

    def dict(self):
        return {"command": self.command, "type": self.type.__str__(), "topic": self.topic}
    
    def __str__(self):
        return f'{{"command": "{self.command}", "type": "{self.type.__str__()}", "topic": "{self.topic}"}}'
    
class CDProto:
    """Computação Distribuida Protocol."""

    @classmethod
    def subscribe(self, type, topic) -> Subscribe:
        """Creates a SubscribeMessage object."""
        return Subscribe("subscribe", type, topic)

    @classmethod
    def publish(self, type, topic, message) -> Publish:
        """Creates a PublishMessage object."""
        return Publish("publish", type, topic, message)
    
    @classmethod
    def listTopics(self, type, list = None) -> ListTopics:
        """Creates a ListTopicsMessage object."""
        if list:
            return ListTopicsOK("listTopics", list)
        return ListTopics("listTopics", type)

    @classmethod
    def unsubscribe(self, type, topic) -> Unsubscribe:
        """Creates a UnsubscribeMessage object."""
        return Unsubscribe("unsubscribe", type, topic)
    
    @classmethod
    def send_msg(self, connection: socket, command, _type="", topic="",  message = None):
        if command == "subscribe":
            msg = self.subscribe(_type, topic)
        elif command == "publish":
            msg = self.publish(_type, topic, message)
        elif command == "listTopics":
            msg = self.listTopics(_type, message)
        elif command == "unsubscribe":
            msg = self.unsubscribe(_type, topic)
        
        if _type == "JSONQueue" or _type.__str__() == "Serializer.JSON":
            msg = json.dumps(msg.dict()).encode('utf-8')

        elif _type == "XMLQueue" or _type.__str__() == "Serializer.XML":
            msg = msg.dict()
            for key in msg:
                msg[key] = str(msg[key])
            msg = ET.tostring(ET.Element("message", msg))

        try:
            header = (len(msg)).to_bytes(2, byteorder="big")
            connection.send(header + msg)
        except:
            raise CDProtoBadFormat(msg)
        
    @classmethod
    def recv_msg(self, connection: socket) -> Message:
        """Receives through a connection a Message object."""
        try:
            size = connection.recv(2)
            size = int.from_bytes(size,byteorder="big")

            if size == 0:
                return None
            elif size >= 2**16:
                raise CDProtoBadFormat(size)
            
            data = connection.recv(size)

            if data:
                try:
                    msg = data.decode("utf-8")
                    msg = json.loads(msg)
                except:
                    try:
                        msg = pickle.loads(data)
                    except:
                        msg = ET.fromstring(data.decode("utf-8"))
                        msg = msg.attrib


                if msg["command"] == "subscribe":
                    return self.subscribe(msg["type"], msg["topic"]).dict()
                elif msg["command"] == "publish":
                    return self.publish(msg["type"], msg["topic"], msg["message"]).dict()
                elif msg["command"] == "listTopics":
                    if msg["topics"]:
                        return self.listTopics(msg["type"], msg["topics"]).dict()
                    return self.listTopics(msg["type"]).dict()
                elif msg["command"] == "unsubscribe":
                    return self.unsubscribe(msg["type"], msg["topic"]).dict()

        except:
            raise CDProtoBadFormat(data)

class CDProtoBadFormat(Exception):
    """Exception when source message is not CDProto."""

    def __init__(self, original_msg: bytes=None) :
        """Store original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode("utf-8")