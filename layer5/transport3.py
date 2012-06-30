import json
from functools import partial


class RemoteError(Exception):
    pass

class Seriliazer(object):
    def dump(self, obj):
        raise NotImplementedError()
    def load(self, blob):
        raise NotImplementedError()
    @classmethod
    def get_dispatcher(cls):
        return partial(dispatch, serializer = cls())
    @classmethod
    def get_invoker(cls):
        return partial(invoke, serializer = cls())

class JSONSerializer(Seriliazer):
    dump = staticmethod(json.dumps)
    load = staticmethod(json.loads)

class IdentitySerializer(Seriliazer):
    load = dump = staticmethod(lambda x: x)

def dispatch(transport, serializer, funcmap):
    funcname, args = serializer.load(transport.recv())
    try:
        res = funcmap[funcname](*args)
        blob2 = serializer.dump((True, res))
    except Exception as ex:
        blob2 = serializer.dump((False, repr(ex)))
    transport.send(blob2)

def invoke(transport, serializer, funcname, args):
    transport.send(serializer.dump((funcname, args)))
    succ, obj = serializer.load(transport.recv())
    if succ:
        return obj
    else:
        raise RemoteError(obj)
























