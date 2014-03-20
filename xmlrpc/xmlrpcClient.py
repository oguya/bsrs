__author__ = 'james'

import xmlrpclib

server = xmlrpclib.ServerProxy('http://localhost:8000')
print server.add(2,3)
print server.div(6,2)
