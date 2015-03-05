__author__ = 'pmontgom'
import xmlrpclib
import httplib
import socket
import paramiko
import traceback

class SSHHTTPConnection(httplib.HTTPConnection):
    def __init__(self, host, ssh_transport, **kwargs):
        httplib.HTTPConnection.__init__(self, host, **kwargs)
        self.ssh_transport = ssh_transport

    def connect(self):
        """Connect to the host and port specified in __init__."""
        source_address = (socket.gethostname(), 0)
        dest_address = (self.host, self.port)

        self.sock = self.ssh_transport._open_ssh_channel(dest_address, source_address)

        # hack to work around issue in using paraminko channels as a "socket".  See http://bugs.python.org/issue7806
        # for details.  The gist is socket.close() doesn't actually close a socket in python.  It only removes a reference
        # and allows the gc reference counting to perform the actual close.
        original_close = self.sock.close

        def monkey_patched_close():
            pass

        def real_close():
            original_close()

        self.ssh_transport.clean_up_callbacks.append(real_close)
        self.sock.close = monkey_patched_close

        if self._tunnel_host:
            self._tunnel()


class SshTransport(xmlrpclib.Transport):
    def __init__(self, ssh_client, **kwargs):
        xmlrpclib.Transport.__init__(self, **kwargs)
        self.ssh_client = ssh_client
        self.clean_up_callbacks = []

    def _open_ssh_channel(self, dest_address, source_address):
        return self.ssh_client.get_transport().open_channel('direct-tcpip',
                                                   dest_address,
                                                   source_address)


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
        self.ssh_client = None


class MethodProxy(object):
    def __init__(self, owner, hostname, username, key_filename, method_name):
        self.owner = owner
        self.hostname = hostname
        self.username = username
        self.key_filename = key_filename
        self.method_name = method_name

    def _exec_call(self, args, kwargs):
        xmlrpcclient, transport = self.owner._get_connection()
        try:
            result = xmlrpcclient.__getattr__(self.method_name)(*args, **kwargs)
        finally:
            transport.dispose()
        return result

    def __call__(self, *args, **kwargs):
        try:
            result = self._exec_call(args, kwargs)
        except paramiko.SSHException:
            self.owner._reconnect()
            print "Got exception, reconnecting and retrying call"
            traceback.print_exc()
            result = self._exec_call(args, kwargs)

        return result

class SshXmlServiceProxy(object):
    def __init__(self, hostname, username, key_filename, port, client_methods):
        self.port = port
        self.username = username
        self.key_filename = key_filename
        self.hostname = hostname
        self.client_methods = client_methods
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    def _reconnect(self):
        #print "calling reconnect <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
        self.client.connect(self.hostname, username=self.username, key_filename=self.key_filename)

    def _get_connection(self):
        if self.client.get_transport() == None or not self.client.get_transport().is_active():
            self._reconnect()

        transport = SshTransport(self.client)
        service = xmlrpclib.ServerProxy("http://localhost:3010", transport=transport, allow_none=True)

        return service, transport

    def __getattribute__(self, name):
        client_methods = object.__getattribute__(self, "client_methods")
        if name in client_methods:
            return MethodProxy(self, self.hostname, self.username, self.key_filename, name)
        else:
            return object.__getattribute__(self, name)


