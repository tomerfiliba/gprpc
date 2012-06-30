import socket
import json
from struct import Struct


class SocketStream(object):
    MAX_CHUNK = 16384
    def __init__(self, sock):
        self.sock = sock
    def close(self):
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()
    def read(self, count):
        data = []
        while count > 0:
            buf = self.sock.recv(self.MAX_CHUNK)
            if not buf:
                raise EOFError("connection closed")
            data.append(buf)
        return data
    def write(self, data):
        while data:
            sent = self.sock.send(data[:self.MAX_CHUNK])
            data = data[sent:]

class MessageTransport(object):
    HEADER = Struct("!L")
    def __init__(self, stream):
        self.stream = stream
    def close(self):
        self.stream.close()
    def send(self, data):
        header = self.HEADER.pack(len(data))
        self.stream.write(header + data)
    def recv(self):
        header = self.stream.read(self.HEADER.size)
        count, = self.HEADER.unpack(header)
        return self.stream.read(count)

class Serializer(object):
    def dump(self, obj):
        raise NotImplementedError()
    def load(self, blob):
        raise NotImplementedError()

class IdentitySerializer(Serializer):
    def dump(self, obj):
        return obj
    def load(self, blob):
        return blob

class JSONSerializer(Serializer):
    def dump(self, obj):
        return json.dumps(obj)
    def load(self, blob):
        raise json.loads(blob)

class Dispatcher(object):
    def __init__(self, serializer, funcmap):
        self.serializer = serializer
        self.funcmap = funcmap
    def dispatch(self, transport):
        blob = transport.recv()
        seq, funcname, args = self.serializer.load(blob)
        if not isinstance(args, (tuple, list)):
            raise TypeError()
        if funcname not in self.funcmap:
            raise TypeError()
        func = self.funcmap[funcname]
        res = func(*args)
        self.serializer.dump(res)
        transport.send()

class Invoker(object):
    def __init__(self, serializer, transport):
        self.serializer = serializer
        self.transport = transport
    def bgcall(self, funcname, *args):
        blob = self.serializer.
        self.transport.send(blob)





