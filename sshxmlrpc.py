__author__ = 'pmontgom'
import xmlrpclib
import httplib
import socket

class SSHHTTPConnection(httplib.HTTPConnection):
    def __init__(self, host, ssh_transport, **kwargs):
        httplib.HTTPConnection.__init__(self, host, **kwargs)
        self.ssh_transport = ssh_transport

    def connect(self):
        """Connect to the host and port specified in __init__."""
        source_address = (socket.gethostname(), 0)
        self.sock = self.ssh_transport.open_channel('direct-tcpip',
                                                   (self.host,self.port),
                                                   source_address)

        # hack to work around issue in using paraminko channels as a "socket".  See http://bugs.python.org/issue7806
        # for details.  The gist is socket.close() doesn't actually close a socket in python.  It only removes a reference
        # and allows the gc reference counting to perform the actual close.
        def monkey_patched_close():
            print "ignoring close"

        self.sock.close = monkey_patched_close

        if self._tunnel_host:
            self._tunnel()


class Transport(xmlrpclib.Transport):
    def __init__(self, ssh_transport, **kwargs):
        xmlrpclib.Transport.__init__(self, **kwargs)
        self.ssh_transport = ssh_transport

    # mostly copied from xmlrpclib.Transport.make_connection
    def make_connection(self, host):
        #return an existing connection if possible.  This allows
        #HTTP/1.1 keep-alive.
        if self._connection and host == self._connection[0]:
            return self._connection[1]

        # create a HTTP connection object from a host descriptor
        chost, self._extra_headers, x509 = self.get_host_info(host)
        #store the host argument along with the connection object
        self._connection = host, SSHHTTPConnection(host, self.ssh_transport)
        return self._connection[1]

