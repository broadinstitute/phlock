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
        self.sock = self.ssh_transport.ssh_transport.open_channel('direct-tcpip',
                                                   (self.host,self.port),
                                                   source_address)

        # hack to work around issue in using paraminko channels as a "socket".  See http://bugs.python.org/issue7806
        # for details.  The gist is socket.close() doesn't actually close a socket in python.  It only removes a reference
        # and allows the gc reference counting to perform the actual close.
        original_close = self.sock.close

        def monkey_patched_close():
            print "ignoring close"

        def real_close():
            print "real close"
            original_close()

        self.ssh_transport.clean_up_callbacks.append(real_close)
        self.sock.close = monkey_patched_close

        if self._tunnel_host:
            self._tunnel()


class SshTransport(xmlrpclib.Transport):
    def __init__(self, ssh_transport, **kwargs):
        xmlrpclib.Transport.__init__(self, **kwargs)
        self.ssh_transport = ssh_transport
        self.clean_up_callbacks = []

    # mostly copied from xmlrpclib.Transport.make_connection
    def make_connection(self, host):
        #return an existing connection if possible.  This allows
        #HTTP/1.1 keep-alive.
        if self._connection and host == self._connection[0]:
            return self._connection[1]

        # create a HTTP connection object from a host descriptor
        chost, self._extra_headers, x509 = self.get_host_info(host)
        #store the host argument along with the connection object
        self._connection = host, SSHHTTPConnection(host, self)
        return self._connection[1]

    def dispose(self):
        for c in self.clean_up_callbacks:
            c()
        self.ssh_transport = None

import threading
per_thread_cache = threading.local()
import traceback
import paramiko

class MethodProxy(object):
    def __init__(self, hostname, username, key_filename, method_name):
        self.hostname = hostname
        self.username = username
        self.key_filename = key_filename
        self.method_name = method_name

    def __call__(self, *args, **kwargs):
        client, transport = threadsafe_get_client(self.hostname, self.username, self.key_filename)

        try:
            # if we want retry logic, it should go here
            result = client.__getattr__(self.method_name)(*args, **kwargs)
        finally:
            transport.dispose()

        return result

class SshXmlServiceProxy(object):
    def __init__(self, hostname, username, key_filename, port, client_methods):
        self.port = port
        self.username = username
        self.key_filename = key_filename
        self.hostname = hostname
        self.client_methods = client_methods

    def __getattribute__(self, name):
        client_methods = object.__getattribute__(self, "client_methods")
        if name in client_methods:
            return MethodProxy(self.hostname, self.username, self.key_filename, name)
        else:
            return object.__getattribute__(self, name)


def threadsafe_get_client(hostname, username, key_filename):
    needs_connect = True
    if hasattr(per_thread_cache, "client"):
        client = per_thread_cache.client
        needs_connect = not client.get_transport().is_active()
    else:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        per_thread_cache.client = client

    if needs_connect:
        client.connect(hostname, username=username, key_filename=key_filename)

    transport = SshTransport(client.get_transport())
    service = xmlrpclib.ServerProxy("http://localhost:3010", transport=transport, allow_none=True)
    return service, transport

