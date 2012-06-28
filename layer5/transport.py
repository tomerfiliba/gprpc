import socket
from struct import Struct
import itertools
import json
import zlib
import ssl


class ListenerSocket(object):
    def __init__(self, port, host = "", backlog = 10, family = socket.AF_INET, 
            stype = socket.SOCK_STREAM, proto = 0):
        self.sock = socket.socket(family, stype, proto)
        self.sock.bind((host, port))
        self.sock.listen(backlog)
        self.host, self.port = self.sock.getsockname()
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
    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
    
    @classmethod
    def connect(cls, host, port, family = socket.AF_INET, stype = socket.SOCK_STREAM, proto = 0):
        sock = socket.socket(family, stype, proto)
        sock.connect((host, port))
        return cls(sock)
    
    @classmethod
    def connect_ssl(cls, host, port, family = socket.AF_INET, stype = socket.SOCK_STREAM, proto = 0,
            keyfile = None, certfile = None, **ssl_options):
        sock = socket.socket(family, stype, proto)
        sock.connect((host, port))
        sock2 = ssl.wrap_socket(sock, keyfile, certfile, server_side = False, **ssl_options)
        return cls(sock2)
    
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
    
    def read(self, count):
        data = []
        while count > 0:
            buf = self.sock.recv(min(count, self.MAX_CHUNK))
            if not buf:
                raise EOFError("Connection closed")
            count -= len(buf)
            data.append(buf)
        return "".join(data)
    
    def write(self, blob):
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
    @classmethod
    def connect(cls, *args, **kwargs):
        return cls(StreamSocket.connect(*args, **kwargs))
    def close(self):
        self.stream.close()
    def send(self, blob, seq = None):
        if seq is None:
            seq = self.seq.next()
        header = self.HEADER.pack(seq, len(blob))
        self.stream.write(header + blob)
        return seq
    def recv(self):
        header = self.stream.read(self.HEADER.size)
        seq, length = self.HEADER.unpack(header)
        blob = self.stream.read(length)
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
        self.funcmap = funcmap
    def dispatch(self):
        seq, blob = self.transport.recv()
        try:
            funcname, args = self.serializer.decode(blob)
        except Exception as ex:
            resp = (RESP_ERROR, "Could not decode blob: %r" % (ex,))
        else:
            if funcname not in self.funcmap:
                resp = (RESP_ERROR, "Unknown function %r" % (funcname,))
            elif not isinstance(args, (tuple, list)):
                resp = (RESP_ERROR, "Arguments must be a list or a tuple, not %r" % (type(args),))
            else:
                try:
                    func = self.funcmap[funcname]
                    res = func(*args)
                except Exception as ex:
                    resp = (RESP_EXCEPTION, repr(ex))
                else:
                    resp = (RESP_OK, res)
        try:
            blob2 = self.serializer.encode(resp)
        except Exception as ex:
            resp = (RESP_ERROR, "Could not encode response: %r" % (ex,))
            blob2 = self.serializer.encode(resp)
        self.transport.send(blob2, seq)

class Serializer(object):
    def encode(self, obj):
        raise NotImplementedError()
    def decode(self, blob):
        raise NotImplementedError()

class IdentitySerializer(Serializer):
    encode = decode = staticmethod(lambda x: x)

class JSONSerializer(Serializer):
    encode = staticmethod(json.dumps)
    decode = staticmethod(json.loads)

class CompressedJSONSerializer(Serializer):
    encode = staticmethod(lambda obj: zlib.compress(json.dumps(obj)))
    decode = staticmethod(lambda blob: json.loads(zlib.decompress(blob)))


class Invoker(object):
    def __init__(self, transport, serializer):
        self.transport = transport
        self.serializer = serializer
        self._responses = {}
    
    def bgcall(self, func, *args):
        blob = self.serializer.encode((func, args))
        seq = self.transport.send(blob)
        return seq
    def call(self, func, *args):
        seq = self.bgcall(func, *args)
        return self.wait_for(seq)
    
    def _recv(self):
        seq, blob = self.transport.recv()
        try:
            resp, obj = self.serializer.decode(blob)
        except Exception as ex:
            raise ProtocolError("Could not decode blob", ex)
        if resp == RESP_ERROR:
            raise ProtocolError(obj)
        if resp != RESP_OK and resp != RESP_EXCEPTION:
            raise ProtocolError("Unknown response code %r" % (resp,))
        self._responses[seq] = (resp, obj)
    
    def wait_for(self, seq):
        while True:
            self._recv()
            if seq not in self._responses:
                continue
            resp, obj = self._responses.pop(seq)
            if resp == RESP_OK:
                return obj
            elif resp == RESP_EXCEPTION:
                raise RemoteException(obj)

class Service(object):
    def __contains__(self, name):
        return hasattr(self.__class__, "exposed_" + name)
    def __getitem__(self, name):
        if not isinstance(name, basestring) or name not in self:
            raise KeyError(name)
        return getattr(self, "exposed_" + name)


# RPC over RPC



listener = ListenerSocket(0)
trns1 = StreamMessageTransport.connect(listener.host, listener.port)
trns2 = StreamMessageTransport(listener.accept())


inv = Invoker(trns1, JSONSerializer)
disp = Dispatcher(trns2, JSONSerializer, dict(
        add = lambda x, y: x + y,
        mul = lambda x, y: x * y,
    ))
seq = inv.bgcall("add", 8, 4)
disp.dispatch()
print inv.wait_for(seq)


"""
* Mixin Layers
    * compression
    * multiplexing
    * ssl
    * ssh
* Options:
    * reconnect
    * timeouts
    * session layer

"""









