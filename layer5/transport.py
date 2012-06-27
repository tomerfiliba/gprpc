import socket
from struct import Struct
import itertools
import json


class Listener(object):
    def __init__(self, port, host = "", backlog = 10, port = 0, family = 0, stype = 0, proto = 0):
        self.sock = socket.socket(family, stype, proto)
        self.sock.bind((host, port))
        self.sock.listen(backlog)
    def accept(self):
        sock2, _ = self.sock.accept()
        return StreamSocket(sock2)

class ClosedSocket(object):
    def __getattr__(self, name):
        raise EOFError("Socket closed")
    def fileno(self):
        raise EOFError("Socket closed")
    def close(self):
        pass
    def __bool__(self):
        return False
    __nonzero__ = __bool__
ClosedSocket = ClosedSocket()

class StreamSocket(object):
    MAX_CHUNK = 16384
    def __init__(self, sock):
        self.sock = sock
    @classmethod
    def connect(cls, host, port, family = 0, stype = 0, proto = 0):
        sock = socket.socket(family, stype, proto)
        sock.connect((host, port))
        return cls(sock)
    
    def fileno(self):
        return self.sock.fileno()
    
    def close(self):
        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except EnvironmentError:
                pass
            self.sock.close()
            self.sock = ClosedSocket
    
    def recv(self, count):
        data = []
        while count > 0:
            buf = self.sock.recv(min(count, self.MAX_CHUNK))
            if not buf:
                raise EOFError("Connection closed")
            count -= len(buf)
            data.append(buf)
        return "".join(data)
    
    def send(self, blob):
        while blob:
            sent = self.sock.send(blob[:self.MAX_CHUNK])
            blob = blob[sent:]


class MessageTransport(object):
    def close(self):
        raise NotImplementedError()
    def send(self, blob):
        raise NotImplementedError()
    def recv(self):
        raise NotImplementedError()

class StreamMessageTransport(MessageTransport):
    HEADER = Struct("!LL")
    def __init__(self, stream):
        self.stream = stream
        self.seq = itertools.count()
    def close(self):
        self.stream.close()
    def send(self, blob, seq = None):
        if seq is None:
            seq = self.seq.next()
        header = self.HEADER.pack(seq, len(blob))
        self.stream.write(header + blob)
        return seq
    def recv(self):
        header = self.stream.recv(self.HEADER.size)
        seq, length = self.HEADER.unpack(header)
        blob = self.stream.recv(length)
        return seq, blob


class ProtocolError(Exception):
    pass
class RemoteException(Exception):
    pass

RESP_OK = 0
RESP_ERROR = 1
RESP_EXCEPTION = 2

class Dispatcher(object):
    def __init__(self, transport, serializer, funcmap):
        self.transport = transport
        self.serializer = serializer
        self.funcmap = {}
    def dispatch(self):
        seq, blob = self.transport.recv()
        try:
            func, args = self.serializer.decode(blob)
        except Exception as ex:
            resp = (RESP_ERROR, "Could not decode blob")
        else:
            if not isinstance(args, (tuple, list)):
                resp = (RESP_ERROR, "Arguments must be a list or a tuple")
            try:
                res = func(*args)
            except Exception as ex:
                resp = (RESP_EXCEPTION, repr(ex))
            else:
                resp = (RESP_OK, res)
        try:
            blob2 = self.serializer.encode(resp)
        except Exception:
            resp = (RESP_ERROR, "Could not encode response")
            blob2 = self.serializer.encode(resp)
        self.transport.send(blob2, seq)

class Serializer(object):
    def encode(self, obj):
        raise NotImplementedError()
    def decode(self, blob):
        raise NotImplementedError()

class JSONSerializer(Serializer):
    encode = staticmethod(json.dumps)
    decode = staticmethod(json.loads)

class Invocation(object):
    __slots__ = ["invoker", "seq"]
    def __init__(self, invoker, seq):
        self.invoker = invoker
        self.seq = seq


class Invoker(object):
    def __init__(self, transport, serializer):
        self.transport = transport
        self.serializer = serializer
        self._responses = {}
    def bgcall(self, func, *args):
        blob = self.serializer.encode((func, args))
        seq = self.transport.send(blob)
        return Invocation(self, seq)
    def call(self, func, *args):
        resp = self.bgcall(func, *args)
        return resp.wait()
    def _recv(self):
        seq, blob = self.transport.recv()
        try:
            resp, obj = self.serializer.decode(blob)
        except Exception:
            pass
        if resp == RESP_ERROR:
            raise ProtocolError(obj)
        self._responses[seq] = (resp, obj)
        return seq
    def _wait(self, seq):
        while True:
            self._recv()
            if seq not in self._responses:
                continue
            resp, obj = self._responses.pop(seq)
            if resp == RESP_OK:
                return obj
            elif resp == RESP_EXCEPTION:
                raise RemoteException(obj)
            else:
                raise ProtocolError("Unknown response code %r" % (resp,))



"""
* Mixin Layers
    * compression
    * multiplexing
    * ssl
    * ssh
* Options:
    * reconnect
    * timeouts

"""









