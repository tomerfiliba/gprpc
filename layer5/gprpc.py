class RPCClient(object):
    def __init__(self, transport):
        self.transport = transport
    def async_call(self, func, args):
        pass
    def call(self, func, args):
        pass

class RPCServer(object):
    def __init__(self, funcmap, transport):
        self.transport = transport
        self.funcmap = funcmap
    def serve(self):
        self.transport.recv()





