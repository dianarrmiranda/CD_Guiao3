import json, pickle
from socket import socket
import xml.etree.ElementTree as ET


class Message:
    """Message Type."""

    def __init__(self, command):
        self.command = command


class Subscribe(Message):
    """Message to join a chat topic."""
    def __init__(self, command, topic):
        super().__init__(command)
        self.topic = topic

    def dict(self):
        return {"command": self.command, "topic": self.topic}
    
    def __str__(self):
        return f'{{"command": "{self.command}", "topic": "{self.topic}"}}'


class ListTopics(Message):
    def __init__(self, command):
        super().__init__(command)

    def dict(self):
        return {"command": self.command}
    
    def __str__(self):
        return f'{{"command": "{self.command}"}}'
    

class ListTopicsOK(Message):
    def __init__(self, command, topics):
        super().__init__(command)
        self.topics = topics

    def dict(self):
        return {"command": self.command, "topics": self.topics}

    def __str__(self):
        return f'{{"command": "{self.command}", "topics": "{self.topics}"}}'


class Publish(Message):
    """Message to chat with other clients."""
    def __init__(self, command,  topic, message):
        super().__init__(command)
        self.topic = topic
        self.message = message

    def dict(self):
        return {"command": self.command,  "topic": self.topic, "message": self.message}
    
    def __str__(self):
        return f'{{"command": "{self.command}",  "topic": "{self.topic}", "message": "{self.message}"}}'


class Unsubscribe(Message):
    def __init__(self, command,  topic):
        super().__init__(command)
        self.topic = topic
        self.type = type

    def dict(self):
        return {"command": self.command, "topic": self.topic}
    
    def __str__(self):
        return f'{{"command": "{self.command}", "topic": "{self.topic}"}}'
    

class CDProto:
    """Computação Distribuida Protocol."""

    @classmethod
    def subscribe(self, topic) -> Subscribe:
        """Creates a SubscribeMessage object."""
        return Subscribe("subscribe",topic)

    @classmethod
    def publish(self, topic, message) -> Publish:
        """Creates a PublishMessage object."""
        return Publish("publish",  topic, message)
    
    @classmethod
    def listTopics(self,  list = None) -> ListTopics:
        """Creates a ListTopicsMessage object."""
        if list:
            return ListTopicsOK("listTopics", list)
        return ListTopics("listTopics", type)

    @classmethod
    def unsubscribe(self,  topic) -> Unsubscribe:
        """Creates a UnsubscribeMessage object."""
        return Unsubscribe("unsubscribe",  topic)
    
    @classmethod
    def send_msg(self, connection: socket, command, serializer: int, topic="",  message = None):
        msg = ""
        if command == "subscribe":
            msg = self.subscribe(topic)
        elif command == "publish":
            print ("Producer " + str(serializer) + " published a message to topic " + topic + "\n")
            msg = self.publish(topic, message)
        elif command == "listTopics":
            msg = self.listTopics(message)
        elif command == "unsubscribe":
            msg = self.unsubscribe(topic)
        
        if serializer == 0:
            msg = json.dumps(msg.dict()).encode('utf-8')
        elif serializer == 1:
            msg = msg.dict()
            for key in msg:
                msg[key] = str(msg[key])
            msg = ET.tostring(ET.Element("message", msg))
        elif serializer == 2:
            #print ("Pickle message before being dumped", msg)
            msg = pickle.dumps(msg.dict())
            #print ("Pickle message dumped in send", msg)
        else:
            raise CDProtoBadFormat(msg)

        try:
            size = (len(msg)).to_bytes(2, byteorder="big")
            header = serializer.to_bytes(1, byteorder="big")
            connection.send(header + size + msg)
        except:
            raise CDProtoBadFormat(msg)
        
        
    @classmethod
    def recv_msg(self, connection: socket, serializer: int) -> Message:
        """Receives through a connection a Message object."""
        

        size = connection.recv(2)
        #print('sizeb', size.decode("utf-8"))

        size = int.from_bytes(size, byteorder="big")
            
        if size == 0:
            return
        elif size >= 2**16:
            raise CDProtoBadFormat(size)
        
        data = connection.recv(size)
        #print ( "Received message with size", size, " actual message size : ", len(data))

        if data:
            #print('data', data)
            #print("serelizer: ", serializer)
            if serializer == 0:
                msg = data.decode("utf-8")
                msg = json.loads(msg)
            elif serializer == 1:
                msg = ET.fromstring(data)
                msg = msg.attrib
            elif serializer == 2:
                #print ("Deloaded pickle message: ", data," \n")
                #print ("pickle DATA: " , data , "\n" )
                #data = pickle.dumps(data)
                msg = pickle.loads(data)
            else:
                return
                
            if msg["command"] == "subscribe":
                #if serializer == 2:
                #    print("Received a pickle subscriber")
                return self.subscribe(msg["topic"]).dict()
            elif msg["command"] == "publish":
                return self.publish(msg["topic"], msg["message"]).dict()
            elif msg["command"] == "listTopics":
                if msg["topics"]:
                    return self.listTopics(msg["topics"]).dict()
                return self.listTopics().dict()
            elif msg["command"] == "unsubscribe":
                return self.unsubscribe(msg["topic"]).dict()
            

class CDProtoBadFormat(Exception):
    """Exception when source message is not CDProto."""

    def __init__(self, original_msg: bytes=None) :
        """Store original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode("utf-8")