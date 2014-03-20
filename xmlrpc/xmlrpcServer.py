__author__ = 'james'

from SimpleXMLRPCServer import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler

#restrict to particular path
class RequestHandler(SimpleXMLRPCRequestHandler):

    rpc_paths = ('/RPC2',)

#create server
server = SimpleXMLRPCServer(("localhost", 8000), requestHandler=RequestHandler)
server.register_introspection_functions()

#register functions ..eg. add() func.
server.register_function(pow)

#register function under a diff name
def sum_func(x, y):
    return x+y
server.register_function(sum_func, 'add')

# Register an instance; all the methods of the instance are
# published as XML-RPC methods (in this case, just 'div').
class MyFuncs:
    def div(self, x, y):
        return x // y

server.register_instance(MyFuncs())

#run server main loop
server.serve_forever()
