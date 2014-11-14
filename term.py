import pyte
import time
import threading
import os
import subprocess
import pty
import uuid

class Promise(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.callbacks = []
        self.is_done = False

    def then(self, callback):
        self.lock.acquire()
        if self.is_done:
            callback()
        else:
            self.callbacks.append(callback)
        self.lock.release()

    def done(self):
        self.lock.acquire()
        assert not self.is_done
        self.is_done = True
        callbacks = self.callbacks
        self.lock.release()

        for callback in callbacks:
            callback()

class ManagedProcess(object):
    def __init__(self, proc, stdout, terminal):
        self.proc = proc
        self.stdout = stdout
        self.terminal = terminal
        self.completion_promise = Promise()
        self.thread = None

        terminal.status = "running"
        terminal.is_running = True

    def io_loop(self):
        while True:
            buffer = os.read(self.stdout.fileno(), 65536)
            if buffer == '':
                break
            self.terminal.write(buffer)

        self.proc.wait()
        self.stdout.close()

        print "process has ended.  Notifying promise"
        self.completion_promise.done()

    def start_thread(self):
        self.thread = threading.Thread(target=self.io_loop)
        self.thread.start()
        return self.completion_promise


class Terminal(object):
    def __init__(self, id, title, command_line=None, log_file=None):
        self.id = id
        self.title = title
        self.command_line = command_line
        self.start_time = time.time()

        self.screen = pyte.Screen(145, 140)
        self.stream = pyte.ByteStream()
        self.stream.attach(self.screen)

        self.status = "running"
        self.is_running = True
        self.lock = threading.Lock()
        self.is_canceled = False
        self.log_file = log_file

    def write(self, buffer):
        xbuffer = buffer.replace("\n", "\r\n")
        self.lock.acquire()
        self.stream.feed(xbuffer)
        if self.log_file != None:
            self.log_file.write(buffer)
        self.lock.release()

    def notify_terminated(self):
        if self.log_file != None:
            self.log_file.flush()

        self.status = "terminated"
        self.is_running = False

    def display(self):
        self.lock.acquire()
        b = tuple(self.screen.display)
        self.lock.release()
        return b

    def is_active(self):
        return True

    def pid(self):
        return "?"

    def kill(self):
        self.is_canceled = True
        if self.proc != None:
            self.proc.terminate()
            for i in xrange(50):
                if self.proc.poll() != None:
                    break
                time.sleep(0.1)
            if self.proc.poll() == None:
                self.proc.kill()


def create_term_for_command(id, args, title=None):
    print "executing ", args
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
    command_line = " ".join(args)
    if title == None:
        title = command_line
    t = Terminal(id, title=title, command_line=command_line)
    mp = ManagedProcess(p, p.stdout, t)
    mp.start_thread().then(t.notify_terminated)

    return t

#def attach_new_command(terminal, args_generator, interval):
#    args = args_generator()
#    print "running %s" % args
#    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
#    cmd_completion = terminal.attach(p.stdout, lambda: terminal.run_until_terminate(p), p)
#
#    def sleep_and_reattach():
#        if not terminal.is_canceled:
#            print "sleeping for %d seconds" % interval
#            terminal.status = "sleeping"
#            time.sleep(interval)
#            attach_new_command(terminal, args_generator, interval)
#
#    cmd_completion.then(sleep_and_reattach)
#
#def create_periodic_execution(id, title, args_generator, interval):
#    t = Terminal(id, title)
#    attach_new_command(t, args_generator, interval)
#    return t

class TerminalManager:
    def __init__(self):
        # map of ID to terminal
        self.terminals = {}

    def get_active_terminals(self):
        return [t for t in self.terminals.values() if t.is_active()]

    def kill_terminal(self, id):
        self.terminals[id].kill()

    def get_terminal(self, id):
        return self.terminals[id]

    def start_named_terminal(self, title, log_file=None):
        id = uuid.uuid4().hex
        terminal = Terminal(id, title, log_file=log_file)
        self.terminals[id] = terminal
        return terminal

    def start_term(self, args, title):
        id = uuid.uuid4().hex
        terminal = create_term_for_command(id, args, title=title)
        self.terminals[id] = terminal
        return terminal
